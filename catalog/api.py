"""Read API. Run: python -m catalog.api create-user NAME; uvicorn catalog.api:app."""
import argparse
import getpass
import hashlib
import hmac
import json
import os
import re
import secrets
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Annotated, Any, Literal

import jwt
import psycopg
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, create_model, model_validator

from .inventory import INVENTORY_FIELDS, SAT

USER_SCHEMA = """
CREATE TABLE IF NOT EXISTS api_user (
    username text PRIMARY KEY, password_hash text NOT NULL,
    enabled boolean NOT NULL DEFAULT true, token_version int NOT NULL DEFAULT 1
)
"""
SCALAR_FIELDS = {name.strip() for name in INVENTORY_FIELDS.split(",")} | {"run_id"}
INTEGER_FIELDS = {"run_id", "car_id", "year", "mileage"}
NUMBERS = {"run_id", "car_id", "year", "mileage", "engine_cc", "power", "seller_price",
           "full_price", "estimated_price", "partial_price", "displayed_price"}
BOOLEANS = {"loaded", "has_data", "has_detail", "sanctioned", "damage_confirmed"}
JSON_FIELDS = {"summary", "detail"}
MAX_BIGINT = 2**63 - 1
CATALOG_COLUMNS = ", ".join("i." + name.strip() for name in INVENTORY_FIELDS.split(","))
DEFAULT_DSN = "postgresql://catalog:catalog-local@127.0.0.1:55432/catalog"
oauth = OAuth2PasswordBearer(tokenUrl="token")
app = FastAPI(title="Каталог автомобилей", version="1.0.0", description=(
    "Все сохранённые объявления, включая снятые с продажи. JWT через /token. "
    "Пагинация по ID без OFFSET; next_cursor передавайте с теми же фильтрами. "
    "Каталог обновляется во время чтения, это не транзакционный снимок. "
    "Поля и примеры фильтров: /fields. Исходные данные: summary и detail."))


def secret():
    value = os.getenv("CATALOG_JWT_SECRET", "")
    if len(value) < 32:
        raise HTTPException(503, "CATALOG_JWT_SECRET должен содержать минимум 32 символа")
    return value


@contextmanager
def connection():
    try:
        with psycopg.connect(os.getenv("DATABASE_URL", DEFAULT_DSN), autocommit=True,
                             row_factory=dict_row, connect_timeout=5) as db:
            db.execute("SET statement_timeout='10s'")
            yield db
    except psycopg.Error:
        raise HTTPException(503, "Каталог временно недоступен", headers={"Retry-After": "5"}) from None


def password_hash(password, salt=None):
    salt = salt or secrets.token_hex(16)
    digest = hashlib.scrypt(password.encode(), salt=salt.encode(), n=16384, r=8, p=1).hex()
    return salt + ":" + digest


def verify_password(password, stored):
    return hmac.compare_digest(password_hash(password, stored.split(":")[0]), stored)


DUMMY_HASH = password_hash("unused-password")


def encode_token(claims, audience, minutes):
    now = datetime.now(timezone.utc)
    return jwt.encode({**claims, "iss": "catalog", "aud": audience, "iat": now,
                       "exp": now + timedelta(minutes=minutes)}, secret(), algorithm="HS256")


def decode_token(token, audience, code=401):
    try:
        return jwt.decode(token, secret(), algorithms=["HS256"], issuer="catalog", audience=audience,
                          options={"require": ["exp", "iat", "iss", "aud"]})
    except jwt.InvalidTokenError:
        raise HTTPException(code, "Недействительный или истёкший токен", headers={"WWW-Authenticate": "Bearer"}) from None


def authorized(token: Annotated[str, Depends(oauth)]):
    claims = decode_token(token, "catalog-api")
    with connection() as db:
        user = db.execute("SELECT username, enabled, token_version FROM api_user WHERE username=%s",
                          (claims.get("sub"),)).fetchone()
    if not user or not user["enabled"] or user["token_version"] != claims.get("ver"):
        raise HTTPException(401, "Доступ отозван", headers={"WWW-Authenticate": "Bearer"})
    return user["username"]


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int = 1800


@app.post("/token", response_model=Token, tags=["Авторизация"])
def login(form: Annotated[OAuth2PasswordRequestForm, Depends()], response: Response):
    secret()
    if len(form.username) > 120 or len(form.password) > 1024:
        raise HTTPException(401, "Неверные учётные данные")
    with connection() as db:
        user = db.execute("SELECT * FROM api_user WHERE username=%s", (form.username,)).fetchone()
    valid = verify_password(form.password, user["password_hash"] if user else DUMMY_HASH)
    if not valid or not user or not user["enabled"]:
        raise HTTPException(401, "Неверные учётные данные", headers={"WWW-Authenticate": "Bearer"})
    response.headers["Cache-Control"] = "no-store"
    return Token(access_token=encode_token({"sub": user["username"], "ver": user["token_version"]}, "catalog-api", 30))


class Filter(BaseModel):
    model_config = ConfigDict(extra="forbid")
    field: str = Field(max_length=240, description="Поле /fields или JSON-путь, например summary.listing.city")
    op: Literal["eq", "ne", "gt", "gte", "lt", "lte", "in", "contains", "isnull"] = "eq"
    value: Any = None


class PageParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_id: int | None = Field(None, gt=0, le=MAX_BIGINT)
    limit: int = Field(100, ge=1, le=1000)
    cursor: str | None = Field(None, max_length=4096)
    include_raw: bool = False
    parsed_from: AwareDatetime | None = Field(None, description="Время последнего парсинга от включительно, RFC 3339 со смещением")
    parsed_to: AwareDatetime | None = Field(None, description="Время последнего парсинга до исключительно, RFC 3339 со смещением")

    @model_validator(mode="after")
    def check_dates(self):
        if self.parsed_from and self.parsed_to and self.parsed_from >= self.parsed_to:
            raise ValueError("parsed_from должен быть раньше parsed_to")
        return self


class Search(PageParameters):
    filters: list[Filter] = Field(default_factory=list, max_length=40)


class QueryParameters(PageParameters):
    model_config = ConfigDict(extra="allow")


def scalar_query_fields():
    for name in sorted(SCALAR_FIELDS - {"run_id"}):
        if name in INTEGER_FIELDS:
            value_type = Annotated[int, Field(ge=-MAX_BIGINT - 1, le=MAX_BIGINT)]
        elif name in NUMBERS:
            value_type = Annotated[Decimal, Field(allow_inf_nan=False)]
        elif name in BOOLEANS:
            value_type = bool
        else:
            value_type = Annotated[str, Field(max_length=2000)]
        yield name, (list[value_type] | None, Field(None, max_length=100, description="Точное совпадение. Можно повторять."))
        if name in NUMBERS:
            yield name + "_min", (value_type | None, Field(None, description="От включительно"))
            yield name + "_max", (value_type | None, Field(None, description="До включительно"))


CarsQuery = create_model("CarsQuery", __base__=QueryParameters, **dict(scalar_query_fields()))


CatalogVehicle = create_model("CatalogVehicle", **{
    field: ((int | float | None) if field in NUMBERS else (bool | None) if field in BOOLEANS else (str | None), None)
    for field in sorted(SCALAR_FIELDS - {"run_id"})
}, parsed_at=(AwareDatetime | None, None), summary=(dict[str, Any] | None, None),
    detail=(dict[str, Any] | None, None))


class Page(BaseModel):
    run_id: int | None
    items: list[CatalogVehicle]
    next_cursor: str | None
    has_more: bool


def raw_path(field):
    parts = field.split(".")
    if len(field) > 240 or len(parts) > 12 or any(not re.fullmatch(r"[A-Za-z0-9_-]+", p) for p in parts[1:]):
        raise HTTPException(422, "Некорректный JSON-путь")
    return parts[0], parts[1:]


def compile_filters(filters):
    clauses, args = [], []
    for f in filters:
        raw = f.field.partition(".")[0] in JSON_FIELDS
        if raw:
            column, path = raw_path(f.field)
            expr = f"v.{column} #> %s::text[]"
            args.append(path)
        elif f.field in SCALAR_FIELDS:
            expr = "i." + f.field
        else:
            raise HTTPException(422, f"Неизвестное поле: {f.field}")
        value = f.value
        if f.op == "isnull":
            if type(value) is not bool:
                raise HTTPException(422, "isnull требует true или false")
            clauses.append(f"({expr} IS {'NULL' if value else 'NOT NULL'})")
            continue
        if value is None:
            if f.op not in {"eq", "ne"}:
                raise HTTPException(422, "null допустим только с eq, ne или isnull")
            if raw:
                clauses.append(f"{expr} {'=' if f.op == 'eq' else '<>'} 'null'::jsonb")
            else:
                clauses.append(f"{expr} IS {'NULL' if f.op == 'eq' else 'NOT NULL'}")
            continue
        values = value if f.op == "in" else [value]
        if not isinstance(values, list) or not 1 <= len(values) <= 100:
            raise HTTPException(422, "in требует массив из 1–100 значений")
        if not raw:
            for item in values:
                if f.field in NUMBERS:
                    if type(item) not in {int, float, Decimal} or not Decimal(str(item)).is_finite():
                        raise HTTPException(422, f"{f.field} требует число")
                elif f.field in BOOLEANS:
                    if type(item) is not bool:
                        raise HTTPException(422, f"{f.field} требует boolean")
                elif not isinstance(item, str) or len(item) > 2000:
                    raise HTTPException(422, f"{f.field} требует строку до 2000 символов")
        if f.op == "in":
            if raw:
                clauses.append(f"{expr} = ANY(%s::jsonb[])")
                args.append([Jsonb(item) for item in values])
            else:
                clauses.append(f"{expr} = ANY(%s)")
                args.append(values)
        elif f.op == "contains":
            if not raw or not isinstance(value, (dict, list)):
                raise HTTPException(422, "contains требует JSON-объект или массив и поле summary/detail")
            clauses.append(f"{expr} @> %s")
            args.append(Jsonb(value))
        else:
            if f.op in {"gt", "gte", "lt", "lte"} and not raw and f.field not in NUMBERS:
                raise HTTPException(422, "Диапазоны поддерживаются для чисел и JSON-значений")
            operator = {"eq": "=", "ne": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[f.op]
            clauses.append(f"{expr} {operator} %s")
            args.append(Jsonb(value) if raw else value)
    return clauses, args


def read_catalog(search, raw_matches=None):
    clauses, args = compile_filters(search.filters)
    for field, values in (raw_matches or {}).items():
        column, path = raw_path(field)
        clauses.append(f"(v.{column} #>> %s::text[]) = ANY(%s::text[])")
        args.extend((path, values))
    date_range = {}
    for name, operator in (("parsed_from", ">="), ("parsed_to", "<")):
        bound = getattr(search, name)
        if bound is not None:
            date_range[name] = bound.astimezone(timezone.utc).isoformat()
            clauses.append(f"o.last_seen_at {operator} %s")
            args.append(bound)
    scope_data = [f.model_dump(mode="json") for f in search.filters]
    if date_range or raw_matches:
        scope_data = {"filters": scope_data, "dates": date_range, "raw": raw_matches}
    scope = hashlib.sha256(json.dumps(scope_data, sort_keys=True).encode()).hexdigest()
    position = decode_token(search.cursor, "catalog-pagination", 422) if search.cursor else None
    if position and (position.get("filters") != scope or search.run_id not in (None, position.get("run"))):
        raise HTTPException(422, "Курсор относится к другой выборке")
    with connection() as db:
        run_id = position["run"] if position else search.run_id
        if run_id is None:
            run = db.execute("SELECT id FROM crawl_run WHERE origin=%s ORDER BY id DESC LIMIT 1", (SAT,)).fetchone()
            if not run:
                return Page(run_id=None, items=[], next_cursor=None, has_more=False)
            run_id = run["id"]
        elif not db.execute("SELECT 1 FROM crawl_run WHERE id=%s", (run_id,)).fetchone():
            raise HTTPException(404, "Прогон не найден")
        after = position["after"] if position else 0
        where = " AND " + " AND ".join(clauses) if clauses else ""
        raw_filter = raw_matches or any(f.field.partition(".")[0] in JSON_FIELDS for f in search.filters)
        joins = "JOIN vehicle v USING (run_id, car_id)" if raw_filter else ""
        columns = CATALOG_COLUMNS
        result_columns = "p.*"
        result_joins = ""
        if date_range:
            joins += " JOIN vehicle_observation o USING (run_id, car_id)"
            columns += ", o.last_seen_at AS parsed_at"
        else:
            result_columns += ", o.last_seen_at AS parsed_at"
            result_joins = "LEFT JOIN vehicle_observation o USING (run_id, car_id)"
        if search.include_raw:
            if raw_filter:
                columns += ", v.summary, v.detail"
            else:
                result_columns += ", v.summary, v.detail"
                result_joins += " JOIN vehicle v USING (run_id, car_id)"
        rows = db.execute(f"""
            WITH page AS (
                SELECT i.run_id, {columns} FROM vehicle_inventory i {joins}
                WHERE i.run_id=%s AND i.car_id>%s {where}
                ORDER BY i.car_id LIMIT %s
            )
            SELECT {result_columns} FROM page p {result_joins} ORDER BY p.car_id
            """, [run_id, after, *args, search.limit + 1]).fetchall()
    more = len(rows) > search.limit
    rows = rows[:search.limit]
    # Preserve decimal prices as JSON numbers without silently converting into strings.
    for row in rows:
        for key, value in row.items():
            if isinstance(value, Decimal):
                row[key] = int(value) if value == value.to_integral_value() else float(value)
    cursor = encode_token({"run": run_id, "after": rows[-1]["car_id"], "filters": scope}, "catalog-pagination", 1440) if more else None
    return Page(run_id=run_id, items=rows, next_cursor=cursor, has_more=more)


@app.get("/cars", response_model=Page, dependencies=[Depends(authorized)], tags=["Каталог"])
def cars(request: Request, query: Annotated[CarsQuery, Query()]):
    """Параметры передаются отдельно. Для исходных полей используйте, например, summary.listing.city=Шанхай."""
    params = query.model_dump(include=set(PageParameters.model_fields))
    filters = []
    for name in sorted(SCALAR_FIELDS - {"run_id"}):
        if values := getattr(query, name):
            values = sorted(set(values))
            filters.append(Filter(field=name, op="eq" if len(values) == 1 else "in",
                                  value=values[0] if len(values) == 1 else values))
        if name in NUMBERS:
            minimum, maximum = getattr(query, name + "_min"), getattr(query, name + "_max")
            if minimum is not None and maximum is not None and minimum > maximum:
                raise HTTPException(422, f"{name}_min выше {name}_max")
            if minimum is not None:
                filters.append(Filter(field=name, op="gte", value=minimum))
            if maximum is not None:
                filters.append(Filter(field=name, op="lte", value=maximum))
    raw_matches = {}
    for field in query.model_extra:
        if "." not in field or field.partition(".")[0] not in JSON_FIELDS:
            raise HTTPException(422, f"Неизвестный параметр: {field}")
        values = request.query_params.getlist(field)
        if len(values) > 100 or any(len(value) > 2000 for value in values):
            raise HTTPException(422, "Допустимо до 100 значений длиной не более 2000 символов")
        raw_matches[field] = sorted(set(values))
    if len(filters) + len(raw_matches) > 40:
        raise HTTPException(422, "Допустимо не более 40 условий")
    return read_catalog(Search(**params, filters=filters), raw_matches=raw_matches or None)


@app.post("/cars/search", response_model=Page, dependencies=[Depends(authorized)], include_in_schema=False)
def search_cars(search: Search):
    """Совместимость с клиентами, использующими прежнее тело запроса."""
    return read_catalog(search)


@app.get("/cars/{car_id}", dependencies=[Depends(authorized)], tags=["Каталог"])
def car(car_id: int, run_id: Annotated[int | None, Query(gt=0, le=MAX_BIGINT)] = None):
    if not 0 < car_id <= MAX_BIGINT:
        raise HTTPException(404, "Объявление не найдено")
    page = read_catalog(Search(run_id=run_id, limit=1, include_raw=True, filters=[Filter(field="car_id", value=car_id)]))
    if not page.items:
        raise HTTPException(404, "Объявление не найдено")
    return {"run_id": page.run_id, **page.items[0].model_dump()}


@app.get("/runs", dependencies=[Depends(authorized)], tags=["Каталог"])
def runs():
    with connection() as db:
        return db.execute("SELECT id,origin,status,pages,started_at,finished_at FROM crawl_run ORDER BY id DESC LIMIT 100").fetchall()


@app.get("/fields", dependencies=[Depends(authorized)], tags=["Каталог"])
def fields():
    return {"fields": {field: "number" if field in NUMBERS else "boolean" if field in BOOLEANS else "string"
                       for field in sorted(SCALAR_FIELDS)},
            "raw": {"summary": "Все поля ответа списка", "detail": "Сохранённая карточка или null"},
            "parsed_at": "Время последнего сохранённого наблюдения или null. Фильтры parsed_from и parsed_to.",
            "query_example": "/cars?country=CN&country=KR&year_min=2020&summary.listing.city=Шанхай",
            "pagination": "Следующая страница: next_cursor и те же параметры фильтрации. Порядок car_id ASC."}


def main():
    parser = argparse.ArgumentParser(description="Управление пользователями API")
    parser.add_argument("action", choices=["create-user", "disable-user"])
    parser.add_argument("username")
    parser.add_argument("--password-stdin", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.@-]{1,120}", args.username):
        parser.error("Некорректное имя пользователя")
    with connection() as db:
        db.execute(USER_SCHEMA)
        if args.action == "disable-user":
            db.execute("UPDATE api_user SET enabled=false,token_version=token_version+1 WHERE username=%s", (args.username,))
        else:
            password = sys.stdin.readline().rstrip("\n") if args.password_stdin else getpass.getpass("Пароль API: ")
            if not 12 <= len(password) <= 1024:
                parser.error("Пароль должен содержать 12–1024 символа")
            db.execute("""INSERT INTO api_user(username,password_hash) VALUES(%s,%s)
                ON CONFLICT(username) DO UPDATE SET password_hash=excluded.password_hash,
                enabled=true,token_version=api_user.token_version+1""", (args.username, password_hash(password)))
    print("Пользователь обновлён")


if __name__ == "__main__":
    main()
