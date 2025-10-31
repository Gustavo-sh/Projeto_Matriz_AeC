/* matriz_operacao.js
 * Unificação do JavaScript do indexOperacao.html
 * Estilo: Script tradicional (sem type="module")
 * Organização: Agrupado por tema
 * Optimiz.: debounce, cache de seletores, remoção de duplicações, correções de bugs
 *
 * Observação: se estiver servindo via FastAPI, mova este arquivo para /static/js/matriz_operacao.js
 * e referencie com: <script src="{{ url_for('static', path='/js/matriz_operacao.js') }}"></script>
 */

/* =============================
 * HELPERS GERAIS
 * ============================= */
(function () {
  // Pequeno utilitário de debounce
  function debounce(fn, wait) {
    let t;
    return function (...args) {
      clearTimeout(t);
      t = setTimeout(() => fn.apply(this, args), wait);
    };
  }

  // Cache simples de querySelector
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);

  // Expor helpers que podem ser úteis em mais blocos
  window.__helpers__ = { debounce, $, $$ };
})();


/* =============================
 * BLOCO: SEGURANÇA / LOGIN (HTMX 401)
 * ============================= */
(function () {
  document.body.addEventListener("htmx:afterRequest", function (evt) {
    if (evt?.detail?.xhr?.status === 401) {
      window.location.href = "/login";
    }
  });
})();


/* =============================
 * BLOCO: TRATAMENTO DE ERROS GERAIS (HTMX 422)
 * (xFiltrox / xPesquisax / xIndicadorx)
 * ============================= */
(function () {
  document.body.addEventListener("htmx:responseError", function (evt) {
    const xhr = evt?.detail?.xhr;
    if (!xhr) return;

    const indicadorDiv = document.getElementById("mensagens-indicador");
    const filtroDiv = document.getElementById("mensagens-filtro");
    const pesquisaDiv = document.getElementById("mensagens-pesquisa");

    if (xhr.status === 422) {
      const resp = xhr.response || "";
      const write = (el, def) => {
        try {
          const data = JSON.parse(resp);
          el.innerText = data.detail || def;
        } catch {
          el.innerText = resp;
        }
      };

      if (resp.includes("xFiltrox")) {
        write(filtroDiv, "Erro inesperado x1x.");
      } else if (resp.includes("xPesquisax")) {
        write(pesquisaDiv, "Erro inesperado x2x.");
      } else if (resp.includes("xIndicadorx")) {
        write(indicadorDiv, "Erro inesperado x3x.");
      }
    }

    // Limpa mensagens após 8s
    setTimeout(() => {
      if (indicadorDiv) indicadorDiv.innerText = "";
      if (filtroDiv) filtroDiv.innerText = "";
      if (pesquisaDiv) pesquisaDiv.innerText = "";
    }, 8000);
  });
})();


/* =============================
 * BLOCO: FLATPICKR (Datas de Indicador + DMM)
 * ============================= */
(function () {
  // flatpickr principais
  let dataFimPicker;
  window.addEventListener("DOMContentLoaded", function () {
    dataFimPicker = flatpickr("#data_fim", {
      dateFormat: "Y-m-d",
      locale: "pt",
      altInput: true,
      altFormat: "d/m/Y",
    });

    flatpickr("#data_inicio", {
      dateFormat: "Y-m-d",
      locale: "pt",
      altInput: true,
      altFormat: "d/m/Y",
      onChange: function (selectedDates) {
        if (selectedDates.length > 0) {
          const inicio = selectedDates[0];
          const ultimoDia = new Date(inicio.getFullYear(), inicio.getMonth() + 1, 0);
          dataFimPicker.set("minDate", inicio);
          dataFimPicker.set("maxDate", ultimoDia);
        }
      },
    });
  });

  // DMM pickers (cadastro + duplicar)
  let dmmPicker, dmmPickerDuplicar;

  function createDmmPicker(selector) {
    return flatpickr(selector, {
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
  }

  function updateDmmLimits() {
    const inicio = document.getElementById("data_inicio")?.value;
    const fim = document.getElementById("data_fim")?.value;
    const possuiDmm = document.querySelector("input[name='possuiDmm']:checked")?.value;

    if (inicio && fim && possuiDmm === "Sim") {
      dmmPicker.set("minDate", inicio);
      dmmPicker.set("maxDate", fim);
      dmmPicker.set("clickOpens", true);
    } else {
      dmmPicker.clear();
      dmmPicker.set("clickOpens", false);
    }
  }

  function updateDmmDuplicarLimits() {
    const inicio = document.getElementById("data_inicio_duplicar")?.value;
    const fim = document.getElementById("data_fim_duplicar")?.value;
    const possuiDmm = document.querySelector("input[name='possuiDmmDuplicar']:checked")?.value;

    if (inicio && fim && possuiDmm === "Sim") {
      dmmPickerDuplicar.set("minDate", inicio);
      dmmPickerDuplicar.set("maxDate", fim);
      dmmPickerDuplicar.set("clickOpens", true);
    } else {
      dmmPickerDuplicar.clear();
      dmmPickerDuplicar.set("clickOpens", false);
    }
  }

  window.addEventListener("DOMContentLoaded", function () {
    dmmPicker = createDmmPicker("#dmm");
    dmmPickerDuplicar = createDmmPicker("#dmm_duplicar");

    // listeners de limite
    const set1 = ["#data_inicio", "#data_fim"];
    set1.forEach((sel) => {
      const el = document.querySelector(sel);
      if (el) el.addEventListener("change", updateDmmLimits);
    });
    document.querySelectorAll("input[name='possuiDmm']").forEach((r) => {
      r.addEventListener("change", updateDmmLimits);
    });

    const set2 = ["#data_inicio_duplicar", "#data_fim_duplicar"];
    set2.forEach((sel) => {
      const el = document.querySelector(sel);
      if (el) el.addEventListener("change", updateDmmDuplicarLimits);
    });
    document.querySelectorAll("input[name='possuiDmmDuplicar']").forEach((r) => {
      r.addEventListener("change", updateDmmDuplicarLimits);
    });
  });

  // Duplicar: pickers e sincronização de período
  function initDuplicarDataPickers() {
    const inicioInput = document.getElementById("data_inicio_duplicar");
    const fimInput = document.getElementById("data_fim_duplicar");

    if (inicioInput && fimInput) {
      const fim = flatpickr(fimInput, {
        dateFormat: "Y-m-d",
        locale: "pt",
        onChange: function () {
          const inicioInputValue = document.getElementById("data_inicio_duplicar").value;
          syncDuplicarPeriodo(inicioInputValue);
        },
      });

      flatpickr(inicioInput, {
        dateFormat: "Y-m-d",
        locale: "pt",
        onChange: function (selectedDates, dateStr) {
          if (selectedDates.length > 0) {
            const inicio = selectedDates[0];
            const ultimoDia = new Date(inicio.getFullYear(), inicio.getMonth() + 1, 0);
            fim.set("minDate", inicio);
            fim.set("maxDate", ultimoDia);
          }
          syncDuplicarPeriodo(dateStr);
        },
      });
    }
  }

  function syncDuplicarPeriodo(dataInicioStr) {
    const periodoInput = document.getElementById("periodo_duplicar");
    if (periodoInput && dataInicioStr) {
      const dataObj = new Date(dataInicioStr + " 12:00:00");
      if (isNaN(dataObj)) {
        periodoInput.value = "";
        return;
      }
      const ano = dataObj.getFullYear();
      const mes = dataObj.getMonth() + 1;
      const mesFormatado = String(mes).padStart(2, "0");
      periodoInput.value = `${ano}-${mesFormatado}-01`;
    } else if (periodoInput) {
      periodoInput.value = "";
    }
  }

  window.initDuplicarDataPickers = initDuplicarDataPickers; // exposto pois é chamado em DOMContentLoaded noutro bloco
  window.syncDuplicarPeriodo = syncDuplicarPeriodo;
})();


/* =============================
 * BLOCO: CHOICES.JS (combos)
 * ============================= */
(function () {
  let choicesAtributo;
  const selects = [
    "#indicadores",
    "#criterio_final",
    "#area",
    "#tipo_faturamento",
    "#atributo_select",
    "#escala_select",
    "#atributos_replicar",
  ];

  function initChoices(selector) {
    const el = document.querySelector(selector);
    if (el && !el.dataset.choicesInitialized) {
      new Choices(el, {
        searchPlaceholderValue: "Selecione...",
        itemSelectText: "",
        shouldSort: false,
        position: "bottom",
        searchResultLimit: 15,
      });
      el.dataset.choicesInitialized = "true";
    }
  }

  function bulkInit() {
    let lastInstance;
    selects.forEach((sel) => {
      const el = document.querySelector(sel);
      if (el) {
        lastInstance = new Choices(el, {
          searchPlaceholderValue: "Selecione...",
          itemSelectText: "",
          shouldSort: false,
          position: "bottom",
          searchResultLimit: 15,
        });
        el.dataset.choicesInitialized = "true";
      }
      if (sel === "#atributo_select") {
        choicesAtributo = lastInstance;
      }
    });
  }

  function syncAtributoHidden() {
    const atributoSelect = document.getElementById("atributo_select");
    const atributoHidden = document.getElementById("atributo_hidden");
    if (atributoSelect && atributoHidden) {
      atributoHidden.value = atributoSelect.value || "";
      atributoSelect.addEventListener("change", function () {
        atributoHidden.value = this.value || "";
      });
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    bulkInit();
    initChoices("#operacao_select");
    initChoices("#atributo_select");
    syncAtributoHidden();
  });

  document.body.addEventListener("htmx:afterSwap", function (evt) {
    if (evt.detail?.target?.id === "atributos-container") {
      initChoices("#atributo_select");
      syncAtributoHidden();
    }
  });

  // expor o choicesAtributo para outro bloco (handleAtributoChange)
  window.__choicesAtributo__ = () => choicesAtributo;
  window.initChoices = initChoices;
  window.syncAtributoHidden = syncAtributoHidden;
})();


/* =============================
 * BLOCO: ATRIBUTO -> CONFIRMA LIMPAR REGISTROS (handleAtributoChange)
 * ============================= */
(function () {
  let lastSelectedAtributo = document.getElementById("atributo_select")?.value || "";

  if (typeof htmx === "undefined") {
    console.error("HTMX não carregado. A função handleAtributoChange não funcionará.");
  }

  function handleAtributoChange(selectElement) {
    const novoAtributo = selectElement.value;
    const registrosContainer = document.getElementById("registros");
    const temRegistros = registrosContainer && registrosContainer.children.length > 0;

    if (temRegistros) {
      const confirma = confirm(
        "Atenção! Ao mudar o atributo, você perderá todos os indicadores registrados na tabela. Deseja continuar?"
      );

      if (confirma) {
        htmx
          .ajax("POST", "/clear_registros", {
            target: registrosContainer,
            swap: "innerHTML",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
          })
          .then(() => {
            lastSelectedAtributo = novoAtributo;
            console.log("Registros zerados e atributo alterado para:", novoAtributo);
          })
          .catch((error) => {
            console.error("Erro ao limpar o cache via HTMX:", error);
            alert("Erro ao limpar os registros no servidor. Por favor, tente novamente ou recarregue a página.");
            selectElement.value = lastSelectedAtributo;
          });
      } else {
        selectElement.value = lastSelectedAtributo;
        const getChoices = window.__choicesAtributo__ && window.__choicesAtributo__();
        if (getChoices && getChoices.setChoiceByValue) {
          getChoices.setChoiceByValue(lastSelectedAtributo);
        } else {
          console.error("Choices.js para atributo não está inicializado corretamente.");
        }
      }
    } else {
      lastSelectedAtributo = novoAtributo;
    }
    return false;
  }

  // Necessário porque o HTML chama onchange="handleAtributoChange(this)"
  window.handleAtributoChange = handleAtributoChange;
})();


/* =============================
 * BLOCO: MENSAGENS (afterSwap filtra justificativa / mostrarSucesso / mostrarErro)
 * ============================= */
(function () {
  // Limpar mensagens-registros se não for o parcial de justificativa
  document.body.addEventListener("htmx:afterSwap", function (evt) {
    if (evt.detail?.target?.id === "mensagens-registros") {
      const contemJustificativa = evt.detail.target.querySelector(".form-justificativa");
      if (!contemJustificativa) {
        setTimeout(() => {
          evt.detail.target.innerHTML = "";
        }, 8000);
      }
    }
  });

  // Eventos customizados: sucesso/erro
  document.body.addEventListener("mostrarSucesso", function (evt) {
    const mensagem = evt.detail?.value;
    const indicadorDiv = document.getElementById("mensagens-indicador");
    const filtroDiv = document.getElementById("mensagens-filtro");
    if (!mensagem) return;

    const html = `<div>${mensagem}</div>`;
    if (mensagem.toLocaleLowerCase().includes("pesquisa")) {
      if (filtroDiv) filtroDiv.innerHTML = html;
    } else {
      if (indicadorDiv) indicadorDiv.innerHTML = html;
    }
    setTimeout(() => {
      if (indicadorDiv) indicadorDiv.innerHTML = "";
      if (filtroDiv) filtroDiv.innerHTML = "";
    }, 8000);
  });

  document.body.addEventListener("mostrarErro", function (evt) {
    const mensagem = evt.detail?.value;
    const indicadorDiv = document.getElementById("mensagens-registros");
    if (!mensagem || !indicadorDiv) return;
    indicadorDiv.innerHTML = `<div style="color: red; font-weight: bold;">Erro: ${mensagem}</div>`;
    setTimeout(() => {
      indicadorDiv.innerHTML = "";
    }, 8000);
  });
})();


/* =============================
 * BLOCO: INDICADORES -> preenche campos e tipo de input META (unificado)
 * ============================= */
(function () {
  function onIndicadorChange() {
    const sel = document.getElementById("indicadores");
    if (!sel) return;

    const selected = sel.options[sel.selectedIndex];
    if (!selected) return;

    let acumulado = selected.getAttribute("data-acumulado");
    let esquema = selected.getAttribute("data-esquema");
    let formato = selected.getAttribute("data-formato");

    if (esquema === "Diario") esquema = "Diário";
    const metaInput = document.getElementById("meta_input");

    // Preenche campos fixos
    const ac = document.getElementById("acumulado_input");
    const es = document.getElementById("esquema_input");
    const fm = document.getElementById("formato_input");
    if (ac) ac.value = acumulado || "";
    if (es) es.value = esquema || "";
    if (fm) fm.value = formato || "";

    // Ajusta tipo/placeholder do campo meta
    if (metaInput) {
      metaInput.value = ""; // sempre limpar ao trocar
      if (formato === "Decimal") {
        metaInput.type = "number";
        metaInput.step = "0.01";
        metaInput.placeholder = "Digite um número decimal";
      } else if (formato === "Percentual") {
        metaInput.type = "number";
        metaInput.step = "0.01";
        metaInput.placeholder = "Digite a % (ex: 75.5)";
      } else if (formato === "Inteiro") {
        metaInput.type = "number";
        metaInput.step = "1";
        metaInput.placeholder = "Digite um número inteiro";
      } else if (formato === "Hora") {
        metaInput.type = "time";
        metaInput.step = "1";
        metaInput.placeholder = "Digite a hora";
      } else {
        metaInput.type = "text";
        metaInput.placeholder = "Selecione um indicador";
      }
    }
  }

  window.addEventListener("DOMContentLoaded", function () {
    const sel = document.getElementById("indicadores");
    if (sel) sel.addEventListener("change", onIndicadorChange);
  });
})();


/* =============================
 * BLOCO: PERÍODO a partir de DATA_INICIO
 * ============================= */
(function () {
  window.addEventListener("DOMContentLoaded", function () {
    const dataInicio = document.getElementById("data_inicio");
    const periodo = document.getElementById("periodo_input");
    if (!dataInicio || !periodo) return;

    dataInicio.addEventListener("change", function () {
      if (dataInicio.value) {
        const [yyyy, mm] = dataInicio.value.split("-").map(Number);
        const periodoStr = `${yyyy}-${String(mm).padStart(2, "0")}-01`;
        periodo.value = periodoStr;
      }
    });
  });
})();


/* =============================
 * BLOCO: ATRIBUTO -> preenche gerente e tipo_matriz
 * ============================= */
(function () {
  function bindAtributoToGerente() {
    const atributoSelect = document.getElementById("atributo_select");
    const gerenteInput = document.getElementById("gerente_input");
    const tipoInput = document.getElementById("tipomatriz_select");

    if (atributoSelect && gerenteInput) {
      atributoSelect.addEventListener("change", function () {
        const opt = this.options[this.selectedIndex];
        gerenteInput.value = opt ? opt.dataset.gerente || "" : "";
      });
    }
    if (atributoSelect && tipoInput) {
      atributoSelect.addEventListener("change", function () {
        const opt = this.options[this.selectedIndex];
        tipoInput.value = opt ? opt.dataset.tipo || "" : "";
      });
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    if (window.initChoices) window.initChoices("#atributo_select");
    if (window.syncAtributoHidden) window.syncAtributoHidden();
    bindAtributoToGerente();
    if (window.initDuplicarDataPickers) window.initDuplicarDataPickers();
  });

  document.body.addEventListener("htmx:afterSwap", function (evt) {
    if (evt.detail?.target?.id === "atributos-container") {
      if (window.initChoices) window.initChoices("#atributo_select");
      if (window.syncAtributoHidden) window.syncAtributoHidden();
      bindAtributoToGerente();
    }
  });
})();


/* =============================
 * BLOCO: HTMX afterRequest -> definir campos ocultos de duplicação
 * ============================= */
(function () {
  document.body.addEventListener("htmx:afterRequest", function (evt) {
    const url = evt.detail?.elt?.getAttribute("hx-post");
    const xhr = evt.detail?.xhr;
    if (!xhr || !(xhr.status >= 200 && xhr.status < 300)) return;

    const atributoInput = document.getElementById("duplicar_atributo");
    const tipoPesquisaInput = document.getElementById("duplicar_tipo_pesquisa");

    if (!url || !tipoPesquisaInput) return;
    const getAtributo = () => document.getElementById("atributo_select")?.value;

    if (url.includes("/pesquisarm0")) {
      tipoPesquisaInput.value = "m0";
      if (atributoInput) atributoInput.value = getAtributo();
    } else if (url.includes("/pesquisarm1")) {
      tipoPesquisaInput.value = "m1";
      if (atributoInput) atributoInput.value = getAtributo();
    } else if (url.includes("/pesquisarmmais1")) {
      tipoPesquisaInput.value = "m+1";
      if (atributoInput) atributoInput.value = getAtributo();
    } else if (url.includes("/allatributesoperacao") || url.includes("/allatributesapoio")) {
      if (atributoInput) atributoInput.value = getAtributo();
    }
  });
})();


/* =============================
 * BLOCO: SELECT-ALL na TABELA DE PESQUISA
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
 * BLOCO: EXPORTAÇÃO (Excel)
 * ============================= */
(function () {
  document.addEventListener("DOMContentLoaded", function () {
    const btn = document.getElementById("export-btn");
    if (!btn) return;

    btn.addEventListener("click", function () {
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
 * BLOCO: HTMX configRequest -> persistir tipo_pesquisa ALL
 * ============================= */
(function () {
  document.addEventListener("htmx:configRequest", function (event) {
    const element = event.detail?.elt;
    const id = element?.id;
    if (id === "all_m0" || id === "all_m1" || id === "all_m+1") {
      const tipoPesquisa = event.detail.parameters?.["tipo_pesquisa"];
      const hidden = document.getElementById("duplicar_tipo_pesquisa");
      if (hidden) hidden.value = tipoPesquisa;
    }
  });
})();


/* =============================
 * BLOCO: DROPDOWN (Links Importantes)
 * ============================= */
(function () {
  document.addEventListener("DOMContentLoaded", function () {
    const btn = document.querySelector(".dropdown-button");
    const content = document.querySelector(".dropdown-content");
    if (!btn || !content) return;
    btn.addEventListener("click", function () {
      content.classList.toggle("show");
    });
  });
})();

/* =============================
 * LOADER ROBUSTO PARA HTMX
 * Suporta: swap, no-swap, outerHTML, abort, error, dblclick, etc.
 * ============================= */
(function () {
  const overlay = document.getElementById("overlay");
  const loader  = document.getElementById("loader");
  if (!overlay || !loader) return;

  let inflight = 0;
  let failSafeTimer = null;

  function showLoader() {
    inflight++;
    overlay.style.display = "block";
    loader.style.display  = "block";

    // failsafe: se algo travar, desmonta após 12s
    clearTimeout(failSafeTimer);
    failSafeTimer = setTimeout(() => {
      inflight = 0;
      hideLoader();
    }, 12000);
  }

  function hideLoader() {
    if (inflight > 0) inflight--;
    if (inflight === 0) {
      overlay.style.display = "none";
      loader.style.display  = "none";
      clearTimeout(failSafeTimer);
    }
  }

  // Início de qualquer requisição HTMX
  document.body.addEventListener("htmx:beforeRequest", showLoader);

  // Fim quando conteúdo chegou e foi aplicado
  document.body.addEventListener("htmx:afterSwap", hideLoader);

  // Quando resposta chega mas não gera swap
  document.body.addEventListener("htmx:afterRequest", hideLoader);

  // Em erro de response
  document.body.addEventListener("htmx:responseError", hideLoader);

  // Em erro de request antes de chegar ao servidor
  document.body.addEventListener("htmx:requestError", hideLoader);

  // Quando requisição é abortada
  document.body.addEventListener("htmx:abort", hideLoader);
})();