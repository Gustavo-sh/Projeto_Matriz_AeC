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
        inicio: celulas[12]?.innerText.trim() || null,
        fim: celulas[13]?.innerText.trim() || null,
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
