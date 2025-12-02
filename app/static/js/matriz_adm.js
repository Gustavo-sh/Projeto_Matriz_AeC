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