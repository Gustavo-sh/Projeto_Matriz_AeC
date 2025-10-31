/* matriz_apoio_cadastro.js
 * Página: Apoio - Cadastro
 * Modo: Modular Parcial (usa matriz_core.js para blocos comuns)
 * Carregar DEPOIS de matriz_core.js
 *
 * Seções:
 * 1) Pós-HTMX: mapear URL -> tipo de pesquisa + atualizar hidden fields
 * 2) Cache Key: construir e atualizar
 * 3) Select-All (Botão)
 * 4) Select-All (Checkbox Mestre)
 * 5) Exportação customizada
 * 6) DMM Duplicar (regras extras)
 */


/* =============================
 * 1) Pós-HTMX: mapear tipo pesquisa + atualizar hidden fields
 * ============================= */
(function () {
  document.body.addEventListener("htmx:afterRequest", function (evt) {
    const xhr = evt.detail?.xhr;
    if (!xhr || !(xhr.status >= 200 && xhr.status < 300)) return;

    const url = evt.detail?.elt?.getAttribute("hx-post") || xhr.responseURL || "";
    if (!url) return;

    const atributoAtual = document.getElementById("atributo_select")?.value || "";
    const atributoInput = document.getElementById("duplicar_atributo");
    const tipoPesquisaHidden = document.getElementById("duplicar_tipo_pesquisa");
    const cacheKey = document.getElementById("cache_key_pesquisa");

    let tipoValor = null;

    if (url.includes("/pesquisarm0")) {
      tipoValor = "m0";
    } else if (url.includes("/pesquisarm1")) {
      tipoValor = "m1";
    } else if (url.includes("/pesquisarmmais1")) {
      tipoValor = "m+1";
    } else if (url.includes("/pesquisarnaoacordos")) {
      if (cacheKey) cacheKey.value = "nao_acordos_apoio";
    }

    if (tipoValor) {
      if (tipoPesquisaHidden) tipoPesquisaHidden.value = tipoValor;
      if (atributoInput) atributoInput.value = atributoAtual;

      document.body.dispatchEvent(
        new CustomEvent("buildCacheKey", {
          detail: { tipo: tipoValor, atributo: atributoAtual },
        })
      );
    }
  });
})();


/* =============================
 * 2) Cache Key: construir e atualizar
 * ============================= */
(function () {
  document.addEventListener("DOMContentLoaded", function () {
    document.body.addEventListener("buildCacheKey", function (evt) {
      const cacheKeyInput = document.getElementById("cache_key_pesquisa");
      const tipo = evt.detail?.tipo;
      const atributo = evt.detail?.atributo;
      let page = null;
      const url = window.location.pathname.toLowerCase();
      // let area = document.body.dataset.area;
      // area = area ? area : "None";
      if (url.includes("cadastro")) {
        page = "cadastro";
      } else {
        page = "demais";
      }
      if (cacheKeyInput && tipo && atributo) {
        const novaCacheKey = `pesquisa_${tipo}:${atributo}:${page}`;
        cacheKeyInput.value = novaCacheKey;
        console.log("[APOIO CADASTRO] cache_key atualizada:", novaCacheKey);
      }
    });
  });
})();


/* =============================
 * 3) Select-All via BOTÃO ("Selecionar Tudo" / "Desmarcar Tudo")
 * ============================= */
(function () {
  document.addEventListener("DOMContentLoaded", function () {
    const selectAllButton = document.getElementById("selecionar-tudo-btn");
    if (!selectAllButton) return;

    selectAllButton.addEventListener("click", function () {
      const checkboxes = document.querySelectorAll('input[name="registro_ids"]');
      const shouldSelect = Array.from(checkboxes).some((cb) => !cb.checked);
      checkboxes.forEach((cb) => (cb.checked = shouldSelect));
      selectAllButton.textContent = shouldSelect ? "Desmarcar Tudo" : "Selecionar Tudo";
    });
  });
})();






/* =============================
 * 5) Exportação customizada
 * ============================= */
(function () {
  document.addEventListener("DOMContentLoaded", function () {
    const exportBtn = document.getElementById("export-btn");
    if (!exportBtn) return;

    exportBtn.addEventListener("click", function () {
      const atributo = document.getElementById("atributo_select")?.value || "";
      const tipo = document.getElementById("duplicar_tipo_pesquisa")?.value || "";
      const cache_key = document.getElementById("cache_key_pesquisa")?.value || "";
      const params = new URLSearchParams();
      params.append("atributo", atributo);
      params.append("duplicar_tipo_pesquisa", tipo);
      params.append("cache_key", cache_key);
      const url = "/export_table?" + params.toString();
      window.open(url, "_blank");
    });
  });
})();


/* =============================
 * 6) DMM Duplicar: regras extras
 * ============================= */
(function () {
  window.addEventListener("DOMContentLoaded", function () {
    const dmmDuplicar = document.querySelector("#dmm_duplicar");
    if (!dmmDuplicar || typeof flatpickr === "undefined") return;

    flatpickr(dmmDuplicar, {
      mode: "multiple",
      dateFormat: "Y-m-d",
      locale: "pt",
      altInput: true,
      altFormat: "d/m/Y",
      clickOpens: false,
      onChange: function (selectedDates, dateStr, instance) {
        if (selectedDates.length > 5) {
          selectedDates.pop();
          instance.setDate(selectedDates);
          alert("Você só pode selecionar no máximo 5 datas.");
        }
      },
    });

    const checkDmmLimits = function () {
      const inicio = document.getElementById("data_inicio_duplicar")?.value;
      const fim = document.getElementById("data_fim_duplicar")?.value;
      const possui = document.querySelector("input[name='possuiDmmDuplicar']:checked")?.value;
      if (possui === "Sim" && inicio && fim) {
        dmmDuplicar._flatpickr.set("minDate", inicio);
        dmmDuplicar._flatpickr.set("maxDate", fim);
        dmmDuplicar._flatpickr.set("clickOpens", true);
      } else {
        dmmDuplicar._flatpickr.clear();
        dmmDuplicar._flatpickr.set("clickOpens", false);
      }
    };

    ["#data_inicio_duplicar", "#data_fim_duplicar"].forEach((sel) => {
      const el = document.querySelector(sel);
      if (el) el.addEventListener("change", checkDmmLimits);
    });

    document.querySelectorAll("input[name='possuiDmmDuplicar']").forEach((r) =>
      r.addEventListener("change", checkDmmLimits)
    );
  });
})();
