import uuid


def validation_submit_table(registros):
    print(registros)
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
    if moedas != 30 and moedas != 35:
        return "<p>A soma de moedas deve ser igual a 30 ou 35.</p>"
    elif moedas == 30:
        try:
            registros.append({'atributo': f'{registros[0]["atributo"]}', 'nome': '48 - Presença', 'meta': '2', 'moeda': 5, 'tipo_indicador': 'Decimal', 'acumulado': 'Não', 'esquema_acumulado': 'Diário',
                            'tipo_matriz': 'OPERAÇÃO', 'data_inicio': f'{registros[0]["data_inicio"]}', 'data_fim': f'{registros[0]["data_fim"]}', 'periodo': f'{registros[0]["periodo"]}', 'escala': f'{registros[0]["escala"]}',
                            'tipo_faturamento': 'Controle', 'descricao': f'{registros[0]["descricao"]}', 'ativo': 0, 'chamado': f'{registros[0]["chamado"]}', 'criterio_final': 'Meta AeC', 'area': 'Plajamento', 'responsavel': '', 'gerente': f'{registros[0]["gerente"]}', 
                            'possuiDmm': f'{registros[0]["possuiDmm"]}', 'dmm': f'{registros[0]["dmm"]}', 'submetido_por': f'{registros[0]["submetido_por"]}', 'data_submetido_por': f'{registros[0]["data_submetido_por"]}', 'qualidade': f'{registros[0]["qualidade"]}', 'da_qualidade': f'{registros[0]["da_qualidade"]}', 'data_da_qualidade': f'{registros[0]["data_da_qualidade"]}', 
                            'planejamento': f'{registros[0]["planejamento"]}', 'da_planejamento': f'{registros[0]["da_planejamento"]}', 'data_da_planejamento': f'{registros[0]["data_da_planejamento"]}', 'exop': f'{registros[0]["exop"]}', 'da_exop': f'{registros[0]["da_exop"]}', 'data_da_exop': f'{registros[0]["data_da_exop"]}', 'id': uuid.uuid4()})
        except KeyError:
            registros.append({'atributo': f'{registros[0]["atributo"]}', 'nome': '48 - Presença', 'meta': '2', 'moeda': 5, 'tipo_indicador': 'Decimal', 'acumulado': 'Não', 'esquema_acumulado': 'Diário',
                        'tipo_matriz': 'OPERAÇÃO', 'data_inicio': f'{registros[0]["data_inicio"]}', 'data_fim': f'{registros[0]["data_fim"]}', 'periodo': f'{registros[0]["periodo"]}', 'escala': f'{registros[0]["escala"]}',
                        'tipo_faturamento': 'Controle', 'descricao': f'{registros[0]["descricao"]}', 'ativo': 0, 'chamado': f'{registros[0]["chamado"]}', 'criterio_final': 'Meta AeC', 'area': 'Plajamento', 'responsavel': '', 'gerente': f'{registros[0]["gerente"]}', 
                        'possuiDmm': f'{registros[0]["possuiDmm"]}', 'dmm': f'{registros[0]["dmm"]}', 'submetido_por': '', 'data_submetido_por': '', 'qualidade': '', 'da_qualidade': '', 'data_da_qualidade': '', 
                        'planejamento': '', 'da_planejamento': '', 'data_da_planejamento': '', 'exop': '', 'da_exop': '', 'data_da_exop': '', 'id': uuid.uuid4()})

    return validation_conditions, registros