/* matriz_apoio.js
 * Página: Apoio
 * Modo: Modular Parcial (usa matriz_core.js para blocos comuns)
 * Carregar DEPOIS de matriz_core.js
 *
 * Responsabilidades específicas:
 * 1) Pós-HTMX: mapear URL -> tipo de pesquisa e preencher duplicar_*
 * 2) Construir cache_key_pesquisa ("pesquisa_<tipo>:<atributo>")
 * 3) Select-all via BOTÃO ("Selecionar Tudo" / "Desmarcar Tudo")
 * 4) Exportação customizada
 * 5) Ajustes no DMM duplicar (caso existam inputs específicos)
 */

/* =============================
 * 1) Pós-HTMX: mapear tipo pesquisa + atualizar campos ocultos
 * ============================= */
(function () {
  document.body.addEventListener("htmx:afterRequest", function (evt) {
    const xhr = evt.detail?.xhr;
    if (!xhr || !(xhr.status >= 200 && xhr.status < 300)) return;

    const url = evt.detail?.elt?.getAttribute("hx-post") || xhr.responseURL || "";
    if (!url) return;

    const atributoInput = document.getElementById("duplicar_atributo");
    const tipoPesquisaHidden = document.getElementById("duplicar_tipo_pesquisa");
    const cacheKey = document.getElementById("cache_key_pesquisa");
    const atributoAtual = document.getElementById("atributo_select")?.value || "";

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

      // Dispara construção da cache key
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
        console.log("[APOIO] cache_key atualizada:", novaCacheKey);
      }
    });
  });
})();





/* =============================
 * 4) Exportação customizada
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
 * 5) DMM duplicar: limite e bloqueio seguro
 * ============================= */
(function () {
  window.addEventListener("DOMContentLoaded", function () {
    const dmmDuplicar = document.querySelector("#dmm_apoio");
    if (!dmmDuplicar || typeof flatpickr === "undefined") return;

    function obterIntervaloDaTabela() {
      const linha = document.querySelector(".tabela-pesquisa tbody tr");
      if (!linha) return { inicio: null, fim: null };

      const celulas = linha.querySelectorAll("td");

      return {
        inicio: celulas[9]?.innerText.trim() || null,
        fim: celulas[10]?.innerText.trim() || null,
      };
    }

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
      const possui = document.querySelector("input[name='possuiDmm_apoio']:checked")?.value;

      const { inicio, fim } = obterIntervaloDaTabela();

      if (possui !== "Sim" || !inicio || !fim) {
        dmmDuplicar._flatpickr.set("clickOpens", false);
        return;
      }

      dmmDuplicar._flatpickr.set("minDate", inicio);
      dmmDuplicar._flatpickr.set("maxDate", fim);
      dmmDuplicar._flatpickr.set("clickOpens", true);
    };

    // 🔥 Quando o usuário clica no input → checar intervalo ANTES de abrir o calendário
    dmmDuplicar.addEventListener("mousedown", checkDmmLimits);

    // Quando escolher Sim/Não
    document.querySelectorAll("input[name='possuiDmm_apoio']").forEach((r) =>
      r.addEventListener("change", checkDmmLimits)
    );

    // Quando a tabela for atualizada pelo HTMX
    document.body.addEventListener("htmx:afterSwap", function (evt) {
      if (evt.target.id === "tabela-pesquisa") {
        checkDmmLimits();
      }
    });
  });
})();
