.DEFAULT_GOAL := help
.PHONY: help up down parser-run parser-resume parser-status sitemap carhub trim-build trim-check test

PYTHON ?= .venv/bin/python
DETAILS ?= 96
BRAND ?=
RUN ?=
TEST_DATABASE_URL ?= postgresql://catalog:catalog-local@127.0.0.1:55432/catalog

help:
	@printf '%s\n' \
	  'make up                         запустить веб-каталог и подготовить БД' \
	  'make parser-run                 загрузить порцию карточек SATAuto' \
	  'make parser-resume              продолжить очередь SATAuto' \
	  'make parser-status              посмотреть очередь и причины ошибок' \
	  'make sitemap                    обновить полный индекс адресов' \
	  'make carhub                     собрать публичную выборку CarHub' \
	  'make trim-build RUN=8           построить справочник из прогона' \
	  'make trim-check RUN=4           проверить совпадения в прогоне' \
	  'make test                       выполнить интеграционные тесты' \
	  'make down                       остановить каталог'

up:
	./parse init

down:
	docker compose stop

parser-run parser-resume:
	./parse --details "$(DETAILS)" $(if $(BRAND),--make "$(BRAND)")

parser-status:
	./parse status $(if $(RUN),--run "$(RUN)")

sitemap:
	./parse index

carhub:
	./parse carhub

trim-build trim-check:
	./parse $@ --run "$(RUN)"

test:
	docker compose up -d --wait postgres
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" "$(PYTHON)" -m unittest discover -s tests -v
