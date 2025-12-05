from datetime import datetime
import uuid
from fastapi.templating import Jinja2Templates
from fastapi import APIRouter, Request, Form, Query, HTTPException, Response, status
from datetime import datetime, date
import calendar
from app.connections_db import get_resultados_indicadores_m3, get_all_atributos, get_excecoes_disponibilidade

templates = Jinja2Templates(directory="app/templates")

async def validation_submit_table(registros, username):
    exceptions = await get_excecoes_disponibilidade()
    agora = datetime.now().strftime("%Y-%m-%d")
    moedas = 0
    validation_conditions = []
    presenca = []
    disp_in = False
    disp_mon = False
    nr_mon = False
    tl_mon = False
    is_exception_atribute = False
    erro_dmm = validation_dmm_consistency(registros)
    atributo_inicial = registros[0]["atributo"]
    atributo_trade = False
    resultados_indicadores_m3 = await get_resultados_indicadores_m3()
    indicadores_processados = set()
    if erro_dmm:
        return erro_dmm
    for dic in registros:
        is_exception_atribute = True if dic["atributo"] in exceptions else False
        moeda_val = int(dic.get("moedas", "0"))
        meta_val = dic.get("meta", "")
        nome_val = dic.get("id_nome_indicador", "").lower()
        id_indicador = dic["id_nome_indicador"].split(" - ")[0]
        if int(dic["moedas"]) > 0 and int(id_indicador) not in resultados_indicadores_m3:
            return f"<p>Não é possível cadastrar o indicador {dic['id_nome_indicador']}, pois ele não tem resultados para os ultimos dois meses+mes atual.</p>"
        try:
            if moeda_val < 0: 
                moeda_val = 0
                dic["moedas"] = 0
            elif moeda_val > 0 and moeda_val < 3:
                return "<p>A monetização mínima é de 3 moedas. O indicador " + dic["id_nome_indicador"] + " possui menos de 3 moedas.</p>"
            chave_indicador = (dic["atributo"], dic["id_nome_indicador"], moeda_val)
            if chave_indicador in indicadores_processados:
                moeda_val = 0  # força a não entrar no cálculo
            else:
                indicadores_processados.add(chave_indicador)
            if dic["atributo"] == atributo_inicial:
                if not atributo_trade:
                    moedas += moeda_val
            else:
                if moedas != 30 and moedas != 35:
                    return "<p>A soma das moedas do atributo " + dic["atributo"] + " deve ser 30 ou 35, soma atual: " + str(moedas) + ".</p>"
                if moedas == 30 and dic["tipo_matriz"].lower() != "administração":
                    try:
                        presenca.append({'atributo': f'{dic["atributo"]}', 'id_nome_indicador': '48 - Presença', 'meta': '2', 'moedas': 5, 'tipo_indicador': 'Decimal', 'acumulado': 'Não', 'esquema_acumulado': 'Diário',
                                        'tipo_matriz': 'Operacional', 'data_inicio': f'{registros[0]["data_inicio"]}', 'data_fim': f'{registros[0]["data_fim"]}', 'periodo': f'{registros[0]["periodo"]}', 'escala': f'{registros[0]["escala"]}',
                                        'tipo_de_faturamento': 'Controle', 'descricao': f'{registros[0]["descricao"]}', 'ativo': 0, 'chamado': f'{registros[0]["chamado"]}', 'criterio': 'Meta AeC', 'area': 'Planejamento', 'responsavel': '', 'gerente': f'{registros[0]["gerente"]}', 
                                        'possui_dmm': f'{registros[0]["possui_dmm"]}', 'dmm': f'{registros[0]["dmm"]}', 'submetido_por': f'{registros[0]["submetido_por"]}', 'data_submetido_por': f'{registros[0]["data_submetido_por"]}', 'qualidade': '', 'da_qualidade': 3, 'data_da_qualidade': '', 
                                        'planejamento': '', 'da_planejamento': 3, 'data_da_planejamento': '', 'exop': '', 'da_exop': 0, 'data_da_exop': '', 'justificativa': '', 'da_superintendente': '', 'id': uuid.uuid4()})
                    except KeyError:
                        presenca.append({'atributo': f'{dic["atributo"]}', 'id_nome_indicador': '48 - Presença', 'meta': '2', 'moedas': 5, 'tipo_indicador': 'Decimal', 'acumulado': 'Não', 'esquema_acumulado': 'Diário',
                                    'tipo_matriz': 'Operacional', 'data_inicio': f'{registros[0]["data_inicio"]}', 'data_fim': f'{registros[0]["data_fim"]}', 'periodo': f'{registros[0]["periodo"]}', 'escala': f'{registros[0]["escala"]}',
                                    'tipo_de_faturamento': 'Controle', 'descricao': f'{registros[0]["descricao"]}', 'ativo': 0, 'chamado': '', 'criterio': 'Meta AeC', 'area': 'Planejamento', 'responsavel': '', 'gerente': f'{registros[0]["gerente"]}', 
                                    'possui_dmm': f'{registros[0]["possui_dmm"]}', 'dmm': f'{registros[0]["dmm"]}', 'submetido_por': f'{username}', 'data_submetido_por': f'{agora}', 'qualidade': '', 'da_qualidade': 3, 'data_da_qualidade': '', 
                                    'planejamento': '', 'da_planejamento': 3, 'data_da_planejamento': '', 'exop': '', 'da_exop': 0, 'data_da_exop': '', 'justificativa': '', 'da_superintendente': '', 'id': uuid.uuid4()})
                atributo_inicial = dic["atributo"]
                atributo_trade = True
                #moedas = 30
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
            if (moeda_val < 8 or int(meta_val) != 94) and not is_exception_atribute:
                return "<p>Disponibilidade não pode ter menos que 8 moedas e deve ter 94 de meta.</p>"
            disp_in = True
            if moeda_val > 0:
                disp_mon = True
        if nome_val == "25 - pausa nr17":
            if moeda_val > 0:
                nr_mon = True
        if nome_val == "15 - tempo logado":
            if moeda_val > 0:
                tl_mon = True
        if (nome_val == "25 - pausa nr17" or nome_val == "15 - tempo logado") and (moeda_val != 0 or meta_val != "00:00:00") and not is_exception_atribute:
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
    if disp_in and disp_mon and (nr_mon or tl_mon):
        return "<p>Não é permitido monetizar Pausa NR17 ou Tempo Logado quando Disponibilidade está monetizada.</p>"
    for dic in presenca:
        registros.append(dic)
    if moedas != 30 and moedas != 35:
        return "<p>A soma de moedas deve ser igual a 30 ou 35.</p>"
    elif moedas == 30 and registros[0]["tipo_matriz"].lower() != "administração":
        try:
            registros.append({'atributo': f'{registros[0]["atributo"]}', 'id_nome_indicador': '48 - Presença', 'meta': '2', 'moedas': 5, 'tipo_indicador': 'Decimal', 'acumulado': 'Não', 'esquema_acumulado': 'Diário',
                            'tipo_matriz': 'Operacional', 'data_inicio': f'{registros[0]["data_inicio"]}', 'data_fim': f'{registros[0]["data_fim"]}', 'periodo': f'{registros[0]["periodo"]}', 'escala': f'{registros[0]["escala"]}',
                            'tipo_de_faturamento': 'Controle', 'descricao': f'{registros[0]["descricao"]}', 'ativo': 0, 'chamado': f'{registros[0]["chamado"]}', 'criterio': 'Meta AeC', 'area': 'Planejamento', 'responsavel': '', 'gerente': f'{registros[0]["gerente"]}', 
                            'possui_dmm': f'{registros[0]["possui_dmm"]}', 'dmm': f'{registros[0]["dmm"]}', 'submetido_por': f'{registros[0]["submetido_por"]}', 'data_submetido_por': f'{registros[0]["data_submetido_por"]}', 'qualidade': '', 'da_qualidade': 3, 'data_da_qualidade': '', 
                            'planejamento': '', 'da_planejamento': 3, 'data_da_planejamento': '', 'exop': '', 'da_exop': 0, 'data_da_exop': '', 'justificativa': '', 'da_superintendente': '', 'id': uuid.uuid4()})
        except KeyError:
            registros.append({'atributo': f'{registros[0]["atributo"]}', 'id_nome_indicador': '48 - Presença', 'meta': '2', 'moedas': 5, 'tipo_indicador': 'Decimal', 'acumulado': 'Não', 'esquema_acumulado': 'Diário',
                        'tipo_matriz': 'Operacional', 'data_inicio': f'{registros[0]["data_inicio"]}', 'data_fim': f'{registros[0]["data_fim"]}', 'periodo': f'{registros[0]["periodo"]}', 'escala': f'{registros[0]["escala"]}',
                        'tipo_de_faturamento': 'Controle', 'descricao': f'{registros[0]["descricao"]}', 'ativo': 0, 'chamado': '', 'criterio': 'Meta AeC', 'area': 'Planejamento', 'responsavel': '', 'gerente': f'{registros[0]["gerente"]}', 
                        'possui_dmm': f'{registros[0]["possui_dmm"]}', 'dmm': f'{registros[0]["dmm"]}', 'submetido_por': f'{username}', 'data_submetido_por': f'{agora}', 'qualidade': '', 'da_qualidade': 3, 'data_da_qualidade': '', 
                        'planejamento': '', 'da_planejamento': 3, 'data_da_planejamento': '', 'exop': '', 'da_exop': 0, 'data_da_exop': '', 'justificativa': '', 'da_superintendente': '', 'id': uuid.uuid4()})

    return validation_conditions, registros

def validation_dmm_consistency(registros: list) -> str | None:
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

async def validation_meta_moedas(registros, meta, moedas, role):
    tipo = registros["tipo_indicador"]
    # area = registros.get("area", "").lower()
    # if "qualidade" in role.lower() and (area != "qualidade" and area != ""):
    #     return f"xPesquisax: Usuário com perfil de Qualidade não pode alterar registros de outras áreas! indicador:{registros["id_nome_indicador"]}"
    # elif "planejamento" in role.lower() and (area != "planejamento" and area != ""):
    #     return f"xPesquisax: Usuário com perfil de Planejamento não pode alterar registros de outras áreas! indicador:{registros["id_nome_indicador"]}"
    if moedas != "":
        if int(moedas) < 3 and int(moedas) > 0:
            return f"A monetização mínima é de 3 moedas. O indicador {registros['id_nome_indicador']} possui {moedas} moedas."
    if registros["id_nome_indicador"].lower() == r"901 - % disponibilidade" and int(meta) != 94:
        return f"Não é permitido alterar a meta do indicador 901 - % disponibilidade!"
    if tipo == "Hora":
        try:
            if len(meta.split(":")) != 3:
                return f"Erro de meta para indicador tipo hora! indicador:{registros["id_nome_indicador"]}, meta informada:{meta}. O modelo correto é HH:MM:SS"
        except ValueError:
            return f"Erro de valor meta! indicador:{registros["id_nome_indicador"]}, meta:{meta}"
    elif tipo == "Inteiro":
        try:
            int(meta)
        except ValueError:
            return f"Meta deve ser um valor inteiro! indicador:{registros["id_nome_indicador"]}, meta informada:{meta}"
    elif tipo == "Decimal":
        try:
            float(meta)
        except ValueError:
            return f"Meta deve ser um valor decimal! indicador:{registros["id_nome_indicador"]}, meta informada:{meta}"
    elif tipo == "Percentual":
        try:
            float(meta)
        except ValueError:
            return f"Meta deve ser um número válido! indicador:{registros["id_nome_indicador"]}, meta informada:{meta}"
    try:
        if moedas != "":
            int(moedas)
    except ValueError:
        return f"Moedas deve ser um número inteiro! indicador: {registros["id_nome_indicador"]}, moedas informada: {moedas}"
    return None

async def validation_dmm(dmm):
    try:
        qtd_dmm = len(dmm.split(","))
        if qtd_dmm != 5:
            return f"Selecione extamente 5 dmms! Dmms selecionados: {qtd_dmm}"
    except Exception as e:
        return f"Erro na validação de dmm: {e}"
    return None

def validation_datas(data_inicio_bd, data_fim_bd, data_inicio_sbmit, data_fim_submit):
    data_original = datetime.strptime(data_inicio_sbmit, '%Y-%m-%d').date()
    ano = data_original.year
    mes = data_original.month
    _, ultimo_dia_do_mes = calendar.monthrange(ano, mes) 
    ultimo_dia_data = date(ano, mes, ultimo_dia_do_mes)
    ultimo_dia_str = ultimo_dia_data.strftime('%Y-%m-%d')
    if data_inicio_sbmit > data_inicio_bd and data_inicio_sbmit > data_fim_bd and data_inicio_sbmit <= data_fim_submit and data_inicio_sbmit <= ultimo_dia_str:
        if data_fim_submit > data_inicio_bd and data_fim_submit > data_fim_bd and data_fim_submit <= ultimo_dia_str:
            pass
        else:
            return True
    else:
        return True
