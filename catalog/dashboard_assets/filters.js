const catalogFilters = document.querySelector("[data-inventory-filters]");
async function inventorySection(section) {
  const url = new URL("/inventory/" + section, window.location.href);
  url.search = window.location.search;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error("HTTP " + response.status);
  }
  return new DOMParser().parseFromString(await response.text(), "text/html");
}

if (catalogFilters) {
  catalogFilters.addEventListener("change", (event) => {
    const name = event.target.name;
    if (name === "country") {
      catalogFilters.elements.make.value = "";
    }
    if (name === "country" || name === "make") {
      catalogFilters.elements.model.value = "";
    }
    if (name === "country" || name === "make" || name === "model") {
      catalogFilters.elements.trim.value = "";
    }
    if (event.target.tagName === "SELECT" || event.target.type === "radio") {
      catalogFilters.requestSubmit();
    }
  });

  inventorySection("options").then((documentFragment) => {
    const form = documentFragment.querySelector("[data-inventory-filters]");
    for (const name of ["make", "model", "trim", "fuel", "color", "body", "drive", "transmission"]) {
      catalogFilters.elements[name].replaceChildren(...form.elements[name].options);
    }
    catalogFilters.querySelector(".quick-brands").replaceWith(form.querySelector(".quick-brands"));
  }).catch(() => {
    const message = document.createElement("p");
    message.className = "hint";
    message.textContent = "Варианты фильтров недоступны. Обновите страницу, чтобы повторить загрузку.";
    catalogFilters.append(message);
  });
}

const catalogSummary = document.querySelector("[data-catalog-summary]");
if (catalogSummary) {
  inventorySection("summary").then((documentFragment) => {
    const summary = documentFragment.querySelector("[data-total-pages]");
    document.querySelector("[data-page-total]").textContent = " из " + summary.dataset.totalPages;
    catalogSummary.replaceChildren(...summary.childNodes);
  }).catch(() => {
    catalogSummary.querySelector(".hint").textContent = "Аналитика временно недоступна.";
  });
}
