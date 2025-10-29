/* matriz_adm.js
 * Lógica específica da página ADM, usando o núcleo em matriz_core.js
 * Script tradicional. Carregar DEPOIS de matriz_core.js
 */

/* =============================
 * Import / Upload Excel (mensagem pós-HTMX)
 * ============================= */
(function () {
  document.body.addEventListener("htmx:afterRequest", function (evt) {
    const importDiv = document.getElementById("upload_result");
    const responseText = evt.detail?.xhr?.response;
    if (!importDiv || !responseText) return;

    if (responseText.includes("xImportx")) {
      const tempDiv = document.createElement("div");
      tempDiv.innerHTML = responseText;
      const cleanMessage = tempDiv.textContent.trim();
      importDiv.innerText = cleanMessage;
      setTimeout(() => (importDiv.innerText = ""), 8000);
    }
  });
})();


/* =============================
 * mostrarSucesso: ADM também pode direcionar para upload_result
 * ============================= */
(function () {
  document.body.addEventListener("mostrarSucesso", function (evt) {
    const mensagem = evt.detail?.value;
    if (!mensagem) return;
    const importDiv = document.getElementById("upload_result");
    if (mensagem.toLocaleLowerCase().includes("import") && importDiv) {
      importDiv.innerHTML = `<div>${mensagem}</div>`;
      setTimeout(() => (importDiv.innerHTML = ""), 8000);
    }
  });
})();


/* =============================
 * Pós-HTMX: mapear tipo_pesquisa_admapoio, cache, duplicar_* (ADM)
 * ============================= */
(function () {
  document.body.addEventListener("htmx:afterRequest", function (evt) {
    const url = evt.detail?.xhr?.responseURL;
    const xhr = evt.detail?.xhr;
    if (!xhr || !(xhr.status >= 200 && xhr.status < 300)) return;

    const atributoInput = document.getElementById("duplicar_atributo");
    const tipoPesquisaInputAdmApoio = document.getElementById("tipo_pesquisa_admapoio");
    const tipoPesquisaInputAntigo = document.getElementById("duplicar_tipo_pesquisa");
    const cacheKey = document.getElementById("cache_key_pesquisa");

    if (!url) return;

    let tipoValor = null;

    if (url.includes("/pesquisarm0admapoio")) {
      tipoValor = "m0";
    } else if (url.includes("/pesquisarm1admapoio")) {
      tipoValor = "m1";
    } else if (url.includes("/pesquisarm0")) {
      tipoValor = "m0";
    } else if (url.includes("/pesquisarm1")) {
      tipoValor = "m1";
    } else if (url.includes("/pesquisarmmais1")) {
      tipoValor = "m+1";
    } else if (url.includes("/pesquisarnaoacordos")) {
      if (cacheKey) cacheKey.value = "nao_acordos_apoio";
    }

    if (tipoValor) {
      if (tipoPesquisaInputAdmApoio) tipoPesquisaInputAdmApoio.value = tipoValor;
      if (atributoInput) atributoInput.value = document.getElementById("atributo_select")?.value || "";
      if (tipoPesquisaInputAntigo) tipoPesquisaInputAntigo.value = tipoValor;

      document.body.dispatchEvent(
        new CustomEvent("buildCacheKey", {
          detail: {
            tipo: tipoValor,
            atributo: atributoInput ? atributoInput.value : "",
          },
        })
      );
    }
  });
})();


/* =============================
 * Cache Key: construir e guardar
 * ============================= */
(function () {
  document.addEventListener("DOMContentLoaded", function () {
    document.body.addEventListener("buildCacheKey", function (evt) {
      const cacheKeyInput = document.getElementById("cache_key_pesquisa");
      const tipo = evt.detail?.tipo;
      const atributo = evt.detail?.atributo;
      if (cacheKeyInput && tipo && atributo) {
        const novaCacheKey = `pesquisa_${tipo}:${atributo}`;
        cacheKeyInput.value = novaCacheKey;
        console.log("CHAVE DE CACHE CONSTRUÍDA COM SUCESSO:", novaCacheKey);
      } else {
        console.error("ERRO: Componentes para cache key estão faltando.");
      }
    });
  });
})();


/* =============================
 * SELECT-ALL por checkbox mestre (select-all-pesquisa) – ADM
 * ============================= */
(function () {
  document.addEventListener("DOMContentLoaded", function () {
    document.body.addEventListener("change", function (evt) {
      if (evt.target && evt.target.id === "select-all-pesquisa") {
        const isChecked = evt.target.checked;
        const checkboxes = document.querySelectorAll(
          '#tabela-pesquisa input[type="checkbox"][name="registro_ids"]'
        );
        checkboxes.forEach((checkbox) => {
          checkbox.checked = isChecked;
        });
      }
    });
  });
})();
