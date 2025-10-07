def validation_submit_table(registros):
    moedas = 0
    validation_conditions = []
    for dic in registros:
        moeda_val = dic.get("moeda", "")
        meta_val = dic.get("meta", "")
        nome_val = dic.get("nome", "")
        if moeda_val == "":
            moeda_val = 0
            dic["moeda"] = 0
        else:
            try:
                if int(moeda_val) <= 0: 
                    moeda_val = 0
                    dic["moeda"] = 0
                moedas += int(moeda_val)
            except ValueError:
                return "<p>Erro: Moeda deve ser um valor inteiro.</p>"
        try:
            if dic["tipo_indicador"] != "Hora":
                if int(meta_val) <= 0: 
                    meta_val = 0
                    dic["meta"] = 0
        except ValueError:
            return "<p>Erro: Meta deve ser um número válido.</p>"
        if nome_val == "6 - % Absenteísmo" and (moeda_val != 0 or meta_val == "" or meta_val == 0):
            return "<p>Absenteísmo não pode ter moedas e deve ter uma meta diferente de zero.</p>"
        if nome_val == "901 - % Disponibilidade" and (int(moeda_val) < 8 or meta_val != "94"):
            return "<p>Disponibilidade não pode ter menos que 8 moedas e deve ter 94 de meta.</p>"
        if (nome_val == "25 - Pausa NR17" or nome_val == "15 - Tempo Logado") and (moeda_val != 0 or meta_val != "00:00:00"):
            return "<p>O valor de moeda deve ser 0 e o valor de meta para Pausa NR17 e Tempo Logado deve ser 00:00:00.</p>"
        if dic["tipo_indicador"] == "Hora":
            try:
                if len(dic["meta"].split(':')) < 3:
                    return "<p>O valor digitado em meta não foi um valor de hora no formato HH:MM:SS.</p>"
            except Exception as e:
                return "<p>Erro ao converter o tempo: " + str(e) + "</p>"
        else:
            try:
                dic["meta"] = float(dic["meta"])
            except ValueError:
                return "<p>Erro: Meta deve ser um valor numérico.</p>"
        validation_conditions.append({
            "atributo": dic["atributo"],
            "periodo": dic["periodo"],
            "id_nome_indicador": dic["nome"],
            "data_inicio_sbmit": dic["data_inicio"],
            "data_fim_submit": dic["data_fim"]
        })
    return moedas, validation_conditions