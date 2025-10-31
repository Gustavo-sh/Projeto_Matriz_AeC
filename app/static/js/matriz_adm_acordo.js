/* matriz_adm_acordo.js
 * Página: ADM - Acordo
 * Modo: Modular Parcial (usa matriz_core.js para blocos comuns)
 * Carregar DEPOIS de matriz_core.js
 *
 * Responsabilidades aqui:
 * 1) Pós-HTMX: mapear URL -> tipo de pesquisa (ADM/APOIO) e setar atributos ocultos
 * 2) Construir/atualizar cache_key_pesquisa ("pesquisa_<tipo>:<atributo>")
 * 3) Select-all por checkbox mestre (id=select-all-pesquisa) para #tabela-pesquisa
 * 4) Mensagens específicas desta página (quando necessário)
 *
 * Observação: não replicamos nada que já vive em matriz_core.js
 */

/* =============================
 * 1) Pós-HTMX: mapear tipo pesquisa + preencher campos ocultos
 * ============================= */
(function () {
  document.body.addEventListener("htmx:afterRequest", function (evt) {
    const xhr = evt.detail?.xhr;
    if (!xhr || !(xhr.status >= 200 && xhr.status < 300)) return;

    const url = evt.detail?.elt?.getAttribute("hx-post") || xhr.responseURL || "";
    if (!url) return;

    const atributoInput = document.getElementById("duplicar_atributo");
    const tipoPesquisaAdmApoio = document.getElementById("tipo_pesquisa_admapoio");
    const tipoPesquisaHidden = document.getElementById("duplicar_tipo_pesquisa");
    const cacheKey = document.getElementById("cache_key_pesquisa");
    const atributoAtual = document.getElementById("atributo_select")?.value || "";

    let tipoValor = null;

    // Rotas ADM/APOIO
    if (url.includes("/pesquisarm0admapoio")) {
      tipoValor = "m0";
    } else if (url.includes("/pesquisarm1admapoio")) {
      tipoValor = "m1";
    }
    // Rotas padrão (mantidas para compatibilidade)
    else if (url.includes("/pesquisarm0")) {
      tipoValor = "m0";
    } else if (url.includes("/pesquisarm1")) {
      tipoValor = "m1";
    } else if (url.includes("/pesquisarmmais1")) {
      tipoValor = "m+1";
    }
    // Pesquisas especiais desta página
    else if (url.includes("/pesquisarnaoacordos")) {
      if (cacheKey) cacheKey.value = "nao_acordos_apoio";
    }

    if (tipoValor) {
      if (tipoPesquisaAdmApoio) tipoPesquisaAdmApoio.value = tipoValor;
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
 * 2) Cache Key: construir e manter atualizada
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
        console.log("[ADM Acordo] cache_key atualizada:", novaCacheKey);
      }
    });
  });
})();


/* =============================
 * 3) Select-all por checkbox mestre (id=select-all-pesquisa)
 * ============================= */
(function () {
  document.addEventListener("DOMContentLoaded", function () {
    document.body.addEventListener("change", function (evt) {
      if (evt.target && evt.target.id === "select-all-pesquisa") {
        const isChecked = evt.target.checked;
        const checkboxes = document.querySelectorAll(
          '#tabela-pesquisa input[type="checkbox"][name="registro_ids"]'
        );
        checkboxes.forEach((checkbox) => (checkbox.checked = isChecked));
      }
    });
  });
})();


/* =============================
 * 4) Mensagens específicas (opcional e seguro)
 * ============================= */
(function () {
  // Se a página quiser direcionar mensagens de sucesso específicas para um container
  document.body.addEventListener("mostrarSucesso", function (evt) {
    const msg = evt.detail?.value;
    if (!msg) return;
    const alvo = document.getElementById("mensagens-filtro");
    if (alvo && msg.toLowerCase().includes("filtro")) {
      alvo.innerHTML = `<div>${msg}</div>`;
      setTimeout(() => (alvo.innerHTML = ""), 8000);
    }
  });
})();
