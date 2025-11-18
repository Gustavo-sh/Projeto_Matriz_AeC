import uuid
from fastapi.templating import Jinja2Templates
from fastapi import APIRouter, Request, Form, Query, HTTPException, Response, status
from app.connections_db import get_resultados_indicadores_m3, get_all_atributos

templates = Jinja2Templates(directory="app/templates")

async def validation_submit_table(registros):
    moedas = 0
    validation_conditions = []
    disp_in = False
    erro_dmm = validate_dmm_consistency(registros)
    if erro_dmm:
        return erro_dmm
    for dic in registros:
        moeda_val = dic.get("moedas", "")
        meta_val = dic.get("meta", "")
        nome_val = dic.get("id_nome_indicador", "").lower()
        resultados_indicadores_m3 = await get_resultados_indicadores_m3()
        id_indicador = dic["id_nome_indicador"].split(" - ")[0]
        if int(dic["moedas"]) > 0 and int(id_indicador) not in resultados_indicadores_m3:
            return f"<p>Não é possível cadastrar o indicador {dic['id_nome_indicador']}, pois ele não tem resultados para os ultimos dois meses+mes atual.</p>"
        if moeda_val == "":
            moeda_val = 0
            dic["moedas"] = 0
        else:
            try:
                if int(moeda_val) < 0: 
                    moeda_val = 0
                    dic["moedas"] = 0
                elif int(moeda_val) > 0 and int(moeda_val) < 3:
                    return "<p>A monetização mínima é de 3 moedas. O indicador " + dic["id_nome_indicador"] + " possui menos de 3 moedas.</p>"
                moedas += int(moeda_val)
                
            except ValueError:
                return "<p>Erro: Moeda deve ser um valor inteiro, valor informado: " + moeda_val + ", para o indicador: " + dic["id_nome_indicador"] + ".</p>"
        try:
            if dic["tipo_indicador"] != "Hora":
                if dic["tipo_indicador"] == "Inteiro":
                    if int(meta_val) <= 0:
                        meta_val = 0
                        dic["meta"] = meta_val
                    meta_val = int(meta_val)
                    dic["meta"] = meta_val
                else: 
                    if float(meta_val) <= 0:
                        meta_val = 0
                        dic["meta"] = meta_val
                    meta_val = float(meta_val)
                    dic["meta"] = meta_val
                    
        except ValueError:
            return "<p>Erro: Meta deve ser um número válido, valor informado: " + meta_val + ", para o indicador: " + dic["id_nome_indicador"] + ".</p>"
        if nome_val == r"6 - % absenteísmo" and (moeda_val != 0 or meta_val == "" or meta_val == 0):
            return "<p>Absenteísmo não pode ter moedas e deve ter uma meta diferente de zero.</p>"
        if nome_val == r"901 - % disponibilidade":
            disp_in = True
            if (int(moeda_val) < 8 or int(meta_val) != 94):
                return "<p>Disponibilidade não pode ter menos que 8 moedas e deve ter 94 de meta.</p>"
        if (nome_val == "25 - pausa nr17" or nome_val == "15 - tempo logado") and (moeda_val != 0 or meta_val != "00:00:00"):
            return "<p>O valor de moeda deve ser 0 e o valor de meta para Pausa NR17 e Tempo Logado deve ser 00:00:00.</p>"
        if dic["tipo_indicador"] == "Hora":
            try:
                if len(dic["meta"].split(':')) < 3:
                    return "<p>O valor digitado em meta não foi um valor de hora no formato HH:MM:SS.</p>"
            except Exception as e:
                return "<p>Erro ao converter o tempo: " + str(e) + "</p>"
        else:
            try:
                float(dic["meta"])
            except ValueError:
                return "<p>Erro: Meta deve ser um valor numérico.</p>"
        validation_conditions.append({
            "atributo": dic["atributo"],
            "periodo": dic["periodo"],
            "id_nome_indicador": dic["id_nome_indicador"],
            "data_inicio_sbmit": dic["data_inicio"],
            "data_fim_submit": dic["data_fim"]
        })
    # if not disp_in:
    #     return "<p>Disponibilidade é um indicador obrigatório, por favor adicione-o com 8 ou mais moedas e 94 de meta.</p>"
    if moedas != 30 and moedas != 35:
        return "<p>A soma de moedas deve ser igual a 30 ou 35.</p>"
    elif moedas == 30 and registros[0]["tipo_matriz"] != "ADMINISTRAÇÃO":
        try:
            registros.append({'atributo': f'{registros[0]["atributo"]}', 'id_nome_indicador': '48 - Presença', 'meta': '2', 'moedas': 5, 'tipo_indicador': 'Decimal', 'acumulado': 'Não', 'esquema_acumulado': 'Diário',
                            'tipo_matriz': 'OPERAÇÃO', 'data_inicio': f'{registros[0]["data_inicio"]}', 'data_fim': f'{registros[0]["data_fim"]}', 'periodo': f'{registros[0]["periodo"]}', 'escala': f'{registros[0]["escala"]}',
                            'tipo_de_faturamento': 'Controle', 'descricao': f'{registros[0]["descricao"]}', 'ativo': 0, 'chamado': '', 'criterio': 'Meta AeC', 'area': 'Planejamento', 'responsavel': '', 'gerente': f'{registros[0]["gerente"]}', 
                            'possui_dmm': f'{registros[0]["possui_dmm"]}', 'dmm': f'{registros[0]["dmm"]}', 'submetido_por': f'{registros[0]["submetido_por"]}', 'data_submetido_por': f'{registros[0]["data_submetido_por"]}', 'qualidade': f'{registros[0]["qualidade"]}', 'da_qualidade': f'{registros[0]["da_qualidade"]}', 'data_da_qualidade': f'{registros[0]["data_da_qualidade"]}', 
                            'planejamento': f'{registros[0]["planejamento"]}', 'da_planejamento': f'{registros[0]["da_planejamento"]}', 'data_da_planejamento': f'{registros[0]["data_da_planejamento"]}', 'exop': f'{registros[0]["exop"]}', 'da_exop': f'{registros[0]["da_exop"]}', 'data_da_exop': f'{registros[0]["data_da_exop"]}', 'justificativa': '', 'da_superintendente': '', 'id': uuid.uuid4()})
        except KeyError:
            registros.append({'atributo': f'{registros[0]["atributo"]}', 'id_nome_indicador': '48 - Presença', 'meta': '2', 'moedas': 5, 'tipo_indicador': 'Decimal', 'acumulado': 'Não', 'esquema_acumulado': 'Diário',
                        'tipo_matriz': 'OPERAÇÃO', 'data_inicio': f'{registros[0]["data_inicio"]}', 'data_fim': f'{registros[0]["data_fim"]}', 'periodo': f'{registros[0]["periodo"]}', 'escala': f'{registros[0]["escala"]}',
                        'tipo_de_faturamento': 'Controle', 'descricao': f'{registros[0]["descricao"]}', 'ativo': 0, 'chamado': '', 'criterio': 'Meta AeC', 'area': 'Planejamento', 'responsavel': '', 'gerente': f'{registros[0]["gerente"]}', 
                        'possui_dmm': f'{registros[0]["possui_dmm"]}', 'dmm': f'{registros[0]["dmm"]}', 'submetido_por': '', 'data_submetido_por': '', 'qualidade': '', 'da_qualidade': '', 'data_da_qualidade': '', 
                        'planejamento': '', 'da_planejamento': '', 'data_da_planejamento': '', 'exop': '', 'da_exop': '', 'data_da_exop': '', 'justificativa': '', 'da_superintendente': '', 'id': uuid.uuid4()})

    return validation_conditions, registros

def validate_dmm_consistency(registros: list) -> str | None:
    if not registros:
        return None 
    referencia = registros[0]
    ref_possui_dmm = referencia.get("possuiDmm", "")
    ref_dmm = referencia.get("dmm", "")
    if not ref_dmm and ref_possui_dmm.lower() == 'sim':
         ref_possui_dmm = 'Não'
    for i, registro in enumerate(registros):
        if i == 0:
            continue
        current_possui_dmm = registro.get("possuiDmm", "")
        current_dmm = registro.get("dmm", "")
        if current_possui_dmm != ref_possui_dmm:
             return f"<p>Erro de Consistência DMM: O campo **Possui DMM** deve ser **idêntico** em todos os indicadores registrados. O indicador {registro.get('nome', 'desconhecido')} é inconsistente com os demais.</p>"
        if ref_possui_dmm.lower() == 'sim':
            if current_dmm != ref_dmm:
                 return f"<p>Erro de Consistência DMM: Todos os indicadores com **Possui DMM = Sim** devem ter a **mesma data ou informação DMM**. O indicador {registro.get('nome', 'desconhecido')} é inconsistente com os demais.</p>"
    return None

async def validation_import_from_excel(registros, request):
    registros_copia = registros
    retorno = []
    atributos = await get_all_atributos()
    for i in registros_copia: 
        if i["atributo"] not in atributos:
            i["descricao"] = "Erro de atributo,"
            retorno.append(i)
            continue
        if len(i["id_nome_indicador"].split(" - ")) != 2:
            i["descricao"] = "Erro de indicador,"
            retorno.append(i)
            continue
        if i["tipo_indicador"] == "Hora":
            try:
                if len(i["meta"].split(":")) != 3:
                    i["descricao"] = "Erro de meta para indicador tipo hora,"
                    retorno.append(i)
                    continue
            except ValueError:
                i["descricao"] = "Erro de valor meta,"
                retorno.append(i)
                continue
        elif i["tipo_indicador"] == "Inteiro":
            try:
                int(i["meta"])
            except ValueError:
                i["descricao"] = "Erro de meta para indicador tipo hora,"
                retorno.append(i)
                continue
        elif i["tipo_indicador"] == "Decimal":
            try:
                float(i["meta"])
            except ValueError:
                i["descricao"] = "Erro de valor meta,"
                retorno.append(i)
                continue
        elif i["tipo_indicador"] == "Percentual":
            try:
                float(i["meta"])
            except ValueError:
                i["descricao"] = "Erro de valor meta,"
                retorno.append(i)
                continue
        try:
            int(i["moedas"])
        except ValueError:
            i["descricao"] = "Erro de valor moedas,"
            retorno.append(i)
            continue

    html_content = templates.TemplateResponse(
    "_pesquisa.html", 
    {"request": request, "registros": retorno} 
    )
    response = Response(content=html_content.body, media_type="text/html")
    response.headers["HX-Trigger"] = '{"mostrarSucesso": "xImportx: A validação encontrou erros, veja-os na primeira tabela abaixo!"}'
    if len(retorno) > 0:
        return response
    return None

async def validation_meta_moedas(registros, meta, moedas):
    tipo = registros["tipo_indicador"]
    if registros["id_nome_indicador"] == r"901 - % disponibilidade" and int(meta) != 94:
        return f"xPesquisax: Não é permitido alterar a meta do indicador 901 - % disponibilidade!"
    if tipo == "Hora":
        try:
            if len(meta.split(":")) != 3:
                return f"xPesquisax: Erro de meta para indicador tipo hora! indicador:{registros["id_nome_indicador"]}, meta informada:{meta}. O modelo correto é HH:MM:SS"
        except ValueError:
            return f"xPesquisax: Erro de valor meta! indicador:{registros["id_nome_indicador"]}, meta:{meta}"
    elif tipo == "Inteiro":
        try:
            int(meta)
        except ValueError:
            return f"xPesquisax: Meta deve ser um valor inteiro! indicador:{registros["id_nome_indicador"]}, meta informada:{meta}"
    elif tipo == "Decimal":
        try:
            float(meta)
        except ValueError:
            return f"xPesquisax: Meta deve ser um valor decimal! indicador:{registros["id_nome_indicador"]}, meta informada:{meta}"
    elif tipo == "Percentual":
        try:
            float(meta)
        except ValueError:
            return f"xPesquisax: Meta deve ser um número válido! indicador:{registros["id_nome_indicador"]}, meta informada:{meta}"
    try:
        if moedas != "":
            int(moedas)
    except ValueError:
        return f"xPesquisax: Moedas deve ser um número inteiro! indicador: {registros["id_nome_indicador"]}, moedas informada: {moedas}"
    return None

async def validation_dmm(dmm):
    try:
        qtd_dmm = len(dmm.split(","))
        if qtd_dmm != 5:
            return f"xPesquisax: Selecione extamente 5 dmms! Dmms selecionados: {qtd_dmm}"
    except Exception as e:
        return f"xPesquisax: Erro na validação de dmm: {e}"
    return None