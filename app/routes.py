from fastapi import APIRouter, Request, Form, Query, HTTPException, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from datetime import datetime
from passlib.context import CryptContext
import uuid
from typing import List 
from app.cache import (
    get_from_cache, set_cache, load_registros, save_registros,
    set_session, get_current_user
)
from app.conexoes_bd import (
    get_indicadores, get_funcao, get_resultados, get_atributos_matricula, get_user_bd, save_user_bd, save_registros_bd,
    query_m0, query_m1, validar_submit, get_atributos_adm_apoio, query_m1_adm_apoio, query_m0_adm_apoio, update_da_adm_apoio
)

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
SESSION_COOKIE = "logged_in"
adms = ["277561"]

def _check_role_or_forbid(user: dict, allowed_roles: list[str]):
    """
    Lança HTTPException(403) se o usuário não estiver autenticado ou não tiver a role permitida.
    """
    if not user:
        return False
    if user.get("role") not in allowed_roles:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Acesso negado.")
    return True

@router.post("/delete/{id}", response_class=HTMLResponse)
def delete_registro(request: Request, id: str):
    registros = load_registros(request)
    registros = [r for r in registros if str(r["id"]) != str(id)]
    save_registros(request, registros)
    return templates.TemplateResponse("_registro.html", {"request": request, "registros": registros})

@router.get("/")
def home():
    return RedirectResponse("/login", status_code=303)

@router.get("/login")
def login_page(request: Request, msg: Optional[str] = Query(None), erro: Optional[str] = Query(None)):
    return templates.TemplateResponse("login.html", {"request": request, "msg": msg, "erro": erro})

@router.post("/login")
def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...)
):
    user = get_user_bd(username)
    if not user:
        return RedirectResponse("/login?erro=Usuário não cadastrado!", status_code=303)
    if not pwd_context.verify(password, user["password"]):
        return RedirectResponse("/login?erro=Senha incorreta!", status_code=303)
    session_token = str(uuid.uuid4())
    set_session(session_token, {"usuario": username, "role": user.get("role")})
    resp = RedirectResponse("/redirect_by_role", status_code=303)
    resp.set_cookie("session_token", session_token, httponly=True)
    resp.set_cookie("logged_in", "true", httponly=True)
    resp.set_cookie("last_active", datetime.utcnow().isoformat(), httponly=True)
    resp.set_cookie("username", username, httponly=True)
    return resp

@router.get("/redirect_by_role")
def redirect_by_role(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)

    role = user.get("role")
    if role == "operacao":
        return RedirectResponse("/matriz")
    elif role in ["apoio qualidade", "apoio planejamento"]:
        return RedirectResponse("/indexApoio")
    elif role == "adm":
        return RedirectResponse("/indexAdm")
    else:
        raise HTTPException(status_code=403, detail="Role inválida")

@router.post("/logout")
def logout(request: Request):
    resp = RedirectResponse("/login", status_code=303)
    resp.delete_cookie(SESSION_COOKIE)
    resp.delete_cookie("last_active")
    resp.delete_cookie("username")
    resp.delete_cookie("session_token")
    save_registros(request, [])
    return resp

@router.get("/register")
def register_page(request: Request, erro: Optional[str] = Query(None)):
    return templates.TemplateResponse("register.html", {"request": request, "erro": erro})

@router.post("/register")
def register_user(request: Request, username: str = Form(...), password: str = Form(...)):
    if get_user_bd(username):
        return RedirectResponse("/register?erro=Usuário já cadastrado!", status_code=303)
    role = None
    if username in adms:
        role = "adm"
    else:
        funcao = None
        try:
            funcao = get_funcao(username)
        except Exception as e:
            funcao = f"Erro ao obter funcao: {e}"
        funcao_upper = funcao.upper() if funcao else ""
        if "COORDENADOR DE QUALIDADE" in funcao_upper or "GERENTE DE QUALIDADE" in funcao_upper:
            role = "apoio qualidade"
        elif "COORDENADOR DE PLANEJAMENTO" in funcao_upper or "GERENTE DE PLANEJAMENTO" in funcao_upper:
            role = "apoio planejamento"
        elif "COORDENADOR DE OPERACAO" in funcao_upper or "GERENTE DE OPERACAO" in funcao_upper:
            role = "operacao"
        elif "ANALISTA DESENVOLVIMENTO OPERACIONAL" in funcao_upper:
            role = "adm"
        else:
            role = None
    if not role:
        return RedirectResponse("/register?erro=Função não autorizada para cadastro.", status_code=303)
    hashed_password = pwd_context.hash(password)
    save_user_bd(username, hashed_password, role)
    return RedirectResponse("/login?msg=Usuário cadastrado com sucesso!", status_code=303)

@router.get("/matriz")
def matriz_page(request: Request):
    logged_in = request.cookies.get(SESSION_COOKIE)
    if not logged_in or logged_in != "true":
        return RedirectResponse("/login", status_code=303)
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    _check_role_or_forbid(user, ["operacao"])
    username = request.cookies.get("username")
    indicadores = get_indicadores()
    #operacoes = get_operacao(username)
    lista_atributos = get_atributos_matricula(username)
    atributos = sorted(lista_atributos, key=lambda item: item.get('atributo') or '')
    registros = load_registros(request)
    return templates.TemplateResponse("index.html", {
        "request": request,
        "registros": registros,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos,
        #"operacoes": operacoes
    })

@router.get("/indexApoio")
def index_apoio(request: Request):
    logged_in = request.cookies.get(SESSION_COOKIE)
    if not logged_in or logged_in != "true":
        return RedirectResponse("/login", status_code=303)
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    _check_role_or_forbid(user, ["apoio qualidade", "apoio planejamento"])
    username = request.cookies.get("username")
    indicadores = get_indicadores()
    # operacoes = get_operacao(username)
    lista_atributos = get_atributos_matricula(username)
    atributos = get_atributos_adm_apoio() #sorted(lista_atributos, key=lambda item: item.get('atributo') or '')
    registros = load_registros(request)
    return templates.TemplateResponse("indexApoio.html", {
        "request": request,
        "registros": registros,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos,
        #"operacoes": operacoes
    })

@router.get("/indexAdm")
def index_adm(request: Request):
    logged_in = request.cookies.get(SESSION_COOKIE)
    if not logged_in or logged_in != "true":
        return RedirectResponse("/login", status_code=303)
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    _check_role_or_forbid(user, ["adm"])
    username = request.cookies.get("username")
    indicadores = get_indicadores()
    # operacoes = get_operacoes_adm(username)
    atributos = get_atributos_adm_apoio()
    registros = load_registros(request)
    return templates.TemplateResponse("indexAdm.html", {
        "request": request,
        "registros": registros,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos
        #"operacoes": operacoes
    })

# @router.post("/get_atributos", response_class=HTMLResponse)
# def get_atributos_by_operacao(request: Request, operacao: str = Form(...)):
#     atributos = get_atributos(operacao)
#     return templates.TemplateResponse("_atributos.html", {
#         "request": request,
#         "atributos": atributos
#     })

@router.post("/add", response_class=HTMLResponse)
def add_registro(
    request: Request,
    nome: str = Form(...),
    meta: str = Form(...),
    moeda: str = Form(...),
    criterio_final: Optional[str] = Form(None),
    area: str = Form(...),
    tipo_faturamento: str = Form(...),
    escala: str = Form(...),
    acumulado: str = Form(...),
    tipo_matriz: str = Form(...),
    esquema_acumulado: str = Form(...),
    descricao: Optional[str] = Form(None),
    ativo: Optional[str] = Form(None),
    chamado: Optional[str] = Form(None),
    possuiDmm: str = Form(...),
    dmm: str = Form(...),
    atributo: str = Form(...),
    tipo_indicador: str = Form(...),
    data_inicio: str = Form(...),
    data_fim: str = Form(...),
    periodo: str = Form(...),
    gerente: str = Form(...),
    responsavel: str = Form(...)
    ):
    registros = load_registros(request)
    novo_id = str(uuid.uuid4())
    novo = {
        "id": novo_id,
        "atributo": atributo, "nome": nome, "meta": meta, "moeda": moeda,"tipo_indicador": tipo_indicador,"acumulado": acumulado,"esquema_acumulado": esquema_acumulado,
        "tipo_matriz": tipo_matriz,"data_inicio": data_inicio,"data_fim": data_fim,"periodo": periodo,"escala": escala,"tipo_faturamento": tipo_faturamento,
        "descricao": descricao,"ativo": ativo or "","chamado": chamado,"criterio_final": criterio_final,"area": area,"responsavel": responsavel,"gerente": gerente,
        "possuiDmm": possuiDmm,"dmm": dmm
    }
    if not atributo or not nome or not meta or not moeda or not data_inicio or not data_fim or not escala or not tipo_faturamento or not criterio_final or not responsavel or not possuiDmm:  
        raise HTTPException(
            status_code=422,
            detail="xIndicadorx: Preencha todos os campos obrigatórios!"
    )
    if len(dmm.split(",")) < 5 and len(dmm.split(",")) > 1:
        raise HTTPException(
            status_code=422,
            detail="xIndicadorx: Selecione exatamente 5 DMM!"
        )
    registros.append(novo)
    save_registros(request, registros)
    html_content = templates.TemplateResponse(
    "_registro.html", 
    {"request": request, "registros": registros} 
    )
    response = Response(content=html_content.body, media_type="text/html")
    response.headers["HX-Trigger"] = '{"mostrarSucesso": "xIndicadorx: Novo registro adicionado com sucesso!"}'
    return response

@router.post("/pesquisarm0", response_class=HTMLResponse)
def pesquisar_m0(request: Request, atributo: str = Form(...)):
    registros = []
    if not atributo:
        raise HTTPException(
            status_code=422,
            detail="xFiltrox : Selecione um atributo primeiro!"
        )
    registros = query_m0(atributo)
    html_content = templates.TemplateResponse(
    "_pesquisa.html", 
    {"request": request, "registros": registros, "show_checkbox": True} 
    )
    response = Response(content=html_content.body, media_type="text/html")
    if len(registros) > 0:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Pesquisa realizada com sucesso!"}'
    else:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Sua pesquisa não trouxe resultados!"}'
    return response

@router.post("/pesquisarm1", response_class=HTMLResponse)
def pesquisar_m1(request: Request, atributo: str = Form(...)):
    registros = []
    if not atributo:
        raise HTTPException(
            status_code=422,
            detail="xFiltrox: Selecione um atributo primeiro!"
        )
    registros = query_m1(atributo)
    html_content = templates.TemplateResponse(
    "_pesquisa.html", 
    {"request": request, "registros": registros, "show_checkbox": True} 
    )
    response = Response(content=html_content.body, media_type="text/html")
    if len(registros) > 0:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Pesquisa realizada com sucesso!"}'
    else:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Sua pesquisa não trouxe resultados!"}'
    return response

@router.post("/pesquisarm1admapoio", response_class=HTMLResponse)
def pesquisar_m1_adm_apoio(request: Request, atributo: str = Form(...)):
    registros = []
    if not atributo:
        raise HTTPException(
            status_code=422,
            detail="xFiltrox: Selecione um atributo primeiro!"
        )
    registros = query_m1_adm_apoio(atributo)
    html_content = templates.TemplateResponse(
    "_pesquisa.html", 
    {"request": request, "registros": registros,  "show_checkbox": True} 
    )
    response = Response(content=html_content.body, media_type="text/html")
    if len(registros) > 0:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Pesquisa realizada com sucesso!"}'
    else:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Sua pesquisa não trouxe resultados!"}'
    return response

@router.post("/pesquisarm0admapoio", response_class=HTMLResponse)
def pesquisar_m0_adm_apoio(request: Request, atributo: str = Form(...)):
    registros = []
    if not atributo:
        raise HTTPException(
            status_code=422,
            detail="xFiltrox: Selecione um atributo primeiro!"
        )
    registros = query_m0_adm_apoio(atributo)
    html_content = templates.TemplateResponse(
    "_pesquisa.html", 
    {"request": request, "registros": registros,  "show_checkbox": True} 
    )
    response = Response(content=html_content.body, media_type="text/html")
    if len(registros) > 0:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Pesquisa realizada com sucesso!"}'
    else:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Sua pesquisa não trouxe resultados!"}'
    return response

@router.post("/submit_table", response_class=HTMLResponse)
def submit_table(request: Request):
    registros = load_registros(request)
    username = request.cookies.get("username", "anon")
    if not registros:
        return "<p>Nenhum registro para submeter.</p>"
    moedas = 0
    for dic in registros:
        if dic["moeda"] == "":
            pass
        else:
            moedas += int(dic["moeda"])
        if validar_submit(dic["atributo"], dic["periodo"], dic["nome"], dic["data_inicio"], dic["data_fim"]):
            return "<p>Este indicador ja foi submetido para o periodo e atributo selecionado.</p>"
        if dic["tipo_indicador"] == "Hora":
            try:
                # horas = int(dic["meta"].split(':')[0])
                # minutos = int(dic["meta"].split(':')[1])
                # total_segundos = (horas * 3600) + (minutos * 60)
                # dic["meta"] = total_segundos
                if len(dic["meta"].split(':')) < 2:
                    return "<p>O valor digitado em meta não foi um valro de hora no formato HH:MM.</p>"
            except Exception as e:
                return "<p>Erro ao converter o tempo: " + str(e) + "</p>"
    if moedas == 30 or moedas == 35:
        save_registros_bd(registros, username)
        return "<p>Tabela submetida com sucesso!</p>"
    else:
        return "<p>A soma de moedas deve ser igual a 30.</p>"
    
@router.post("/trazer_resultados", response_class=HTMLResponse)
def trazer_resultados(request: Request, atributo: str = Form(...), nome: str = Form(...)):
    if len(nome.split(" - ")) == 1:
        raise HTTPException(
            status_code=422,
            detail="xIndicadorx: Selecione um atributo e um indicador primeiro!"
        )
    id_indicador = nome.split(" - ")[0]
    query = get_resultados(atributo, id_indicador)
    if not query:
        raise HTTPException(
            status_code=422,
            detail="xIndicadorx: Nenhum resultado encontrado para o indicador e atributo selecionados."
        )
    # row = query[0]
    m1 = query[0] if len(query) > 1 else None
    m0 = query[1] if len(query) > 1 else query[0]
    return templates.TemplateResponse(
        "_resultados.html", 
        {
            "request": request,
            "meta_sugerida": m0[6] if m0[6] else "",
            "meta_escolhida": m0[9] if m0[9] else "",
            "atingimento_projetado": round(m0[8]*100, 2) if m0[8] else "",
            "resultado_m0": round(m0[4], 2) if m1 else "",
            "atingimento_m0": round(m0[5]*100, 2) if m1 else "",
            "resultado_m1": round(m1[4], 2) if m1 else round(m0[4], 2),
            "atingimento_m1": round(m1[5]*100, 2) if m1 else round(m0[5]*100, 2),
            "max_data": m1[10] if m1 else m0[10]
        }
    )
    # return templates.TemplateResponse(
    #     "_resultados.html", 
    #     {
    #         "request": request,
    #         "meta_sugerida": row[9],
    #         "meta_escolhida": row[10],
    #         "atingimento_projetado": row[21],
    #         "resultado_m0": row[12],
    #         "atingimento_m0": row[13],
    #         "resultado_m1": row[15],
    #         "atingimento_m1": row[16],
    #         "max_data": row[17]
    #     }
    # )

@router.post("/duplicate_search_results", response_class=HTMLResponse)
def duplicate_search_results(
    request: Request, 
    atributo: str = Form(...), 
    tipo_pesquisa: str = Form(...),
    data_inicio: str = Form(...), 
    data_fim: str = Form(...), 
    periodo: str = Form(...),
    registro_ids: List[str] = Form([], alias="registro_ids"),
    ):
    if not data_inicio or not data_fim or not periodo:
          raise HTTPException(
              status_code=422,
              detail="xPesquisax: Selecione as datas de início e fim antes de duplicar!"
          )
    if not registro_ids:
        raise HTTPException(
            status_code=422,
            detail="xPesquisax: Selecione pelo menos um registro para duplicar."
        )
    cache_key = ""
    user = get_current_user(request)
    role = user.get("role")    
    if tipo_pesquisa == "m0":
        if role == 'operacao':
            cache_key = f"pesquisa_m0:{atributo}"
        elif role == 'adm' or role == 'apoio qualidade' or role == 'apoio planejamento':
            cache_key = f"pesquisa_m0_adm_apoio:{atributo}"
        else:
            raise HTTPException(status_code=422, detail="xPesquisax: Role invalida!")
    elif tipo_pesquisa == "m1":
        if role == 'operacao':
            cache_key = f"pesquisa_m1:{atributo}"
        elif role == 'adm' or role == 'apoio qualidade' or role == 'apoio planejamento':
            cache_key = f"pesquisa_m1_adm_apoio:{atributo}"
        else:
            raise HTTPException(status_code=422, detail="xPesquisax: Role invalida!")
    else:
        raise HTTPException(
            status_code=422,
            detail="xPesquisax: Tipo de pesquisa inválido (deve ser 'm0' ou 'm1')."
        )
    registros_da_pesquisa = get_from_cache(cache_key)
    if not registros_da_pesquisa:
        raise HTTPException(
            status_code=422,
            detail="xPesquisax: Nenhum resultado de pesquisa encontrado no cache para duplicar. Execute a pesquisa primeiro!"
        )
    ids_selecionados = set(registro_ids)
    registros_a_duplicar = [
        r for r in registros_da_pesquisa 
        if str(r.get("id")) in ids_selecionados
    ]
    if not registros_a_duplicar:
        raise HTTPException(
            status_code=422,
            detail="xPesquisax: Os registros selecionados não foram encontrados no cache da pesquisa."
        )
    registros_atuais = load_registros(request)
    for novo_registro in registros_a_duplicar:
        registro_copia = novo_registro.copy()
        registro_copia["id"] = str(uuid.uuid4())
        registro_copia["data_inicio"] = data_inicio
        registro_copia["data_fim"] = data_fim
        registro_copia["periodo"] = periodo 
        registros_atuais.append(registro_copia)
    save_registros(request, registros_atuais)
    html_content = templates.TemplateResponse(
        "_registro.html", 
        {"request": request, "registros": registros_atuais} 
    ) 
    response = Response(content=html_content.body, media_type="text/html")
    response.headers["HX-Trigger"] = '{"mostrarSucesso": "xPesquisax: Registros da pesquisa duplicados e adicionados com sucesso!"}'
    return response

@router.post("/update_registro/{registro_id}/{campo}", response_class=HTMLResponse)
def update_registro(request: Request, registro_id: str, campo: str, novo_valor: str = Form(..., alias="value")):
    registros = load_registros(request)
    registro_encontrado = None
    for reg in registros:
        if str(reg.get("id")) == registro_id:
            registro_encontrado = reg
            break
    if not registro_encontrado:
        return Response(status_code=404, content=f"Registro ID {registro_id} não encontrado.")
    if campo not in ["meta", "moeda"]:
        return Response(status_code=400, content="Campo inválido para edição.")
    valor_limpo = novo_valor.strip()
    tipo_indicador = registro_encontrado.get("tipo_indicador")
    if campo == 'moeda':
        try:
            pass
        except ValueError:
            error_message = f"O campo {campo} deve ser um número inteiro."
            response = Response(content=f'{registro_encontrado.get(campo) or ""}', status_code=400)
            response.headers["HX-Retarget"] = "#mensagens-registros" 
            response.headers["HX-Reswap"] = "innerHTML"
            response.headers["HX-Trigger"] = f'{{"mostrarErro": "{error_message}"}}'
            return response
    elif tipo_indicador in ["Percentual"] and campo != 'moeda':
        try:
            float(valor_limpo.replace(',', '.')) 
        except ValueError:
            error_message = f"O campo {campo} para o tipo '{tipo_indicador}' deve ser um número válido."
            response = Response(content=f'{registro_encontrado.get(campo) or ""}', status_code=400)
            response.headers["HX-Retarget"] = "#mensagens-registros" 
            response.headers["HX-Reswap"] = "innerHTML"
            response.headers["HX-Trigger"] = f'{{"mostrarErro": "{error_message}"}}'
            return response
    elif tipo_indicador in ["Inteiro"] and campo != 'moeda':
        try:
            int(valor_limpo.replace(',', '.'))
        except ValueError:
            error_message = f"O campo {campo} para o tipo '{tipo_indicador}' deve ser um valor número válido."
            response = Response(content=f'{registro_encontrado.get(campo) or ""}', status_code=400)
            response.headers["HX-Retarget"] = "#mensagens-registros"
            response.headers["HX-Reswap"] = "innerHTML"
            response.headers["HX-Trigger"] = f'{{"mostrarErro": "{error_message}"}}'
            return response
    elif tipo_indicador in ["Decimal"] and campo != 'moeda':
        try:
            float(valor_limpo.replace(',', '.'))
        except ValueError:
            error_message = f"O campo {campo} para o tipo '{tipo_indicador}' deve ser um valor número válido."
            response = Response(content=f'{registro_encontrado.get(campo) or ""}', status_code=400)
            response.headers["HX-Retarget"] = "#mensagens-registros"
            response.headers["HX-Reswap"] = "innerHTML"
            response.headers["HX-Trigger"] = f'{{"mostrarErro": "{error_message}"}}'
            return response
    elif tipo_indicador in ["Hora"] and campo != 'moeda':
        if len(valor_limpo.split(':')) < 2:
            error_message = f"O campo {campo} para o tipo '{tipo_indicador}' deve ser um valor número válido."
            response = Response(content=f'{registro_encontrado.get(campo) or ""}', status_code=400)
            response.headers["HX-Retarget"] = "#mensagens-registros"
            response.headers["HX-Reswap"] = "innerHTML"
            response.headers["HX-Trigger"] = f'{{"mostrarErro": "{error_message}"}}'
            return response      
    registro_encontrado[campo] = valor_limpo
    save_registros(request, registros)
    return f'{registro_encontrado.get(campo) or ""}'

@router.get("/edit_campo/{registro_id}/{campo}", response_class=HTMLResponse)
def edit_campo_get(request: Request, registro_id: str, campo: str):
    registros = load_registros(request)
    valor = ""
    for reg in registros:
        if str(reg.get("id")) == registro_id:
            valor = reg.get(campo)
            break
    return f"""
    <td hx-trigger="dblclick" hx-get="/edit_campo/{registro_id}/{campo}" hx-target="this" hx-swap="outerHTML">
        <form hx-post="/update_registro/{registro_id}/{campo}" hx-target="this" hx-swap="outerHTML">
            <input name="value" 
                   type="text" 
                   value="{valor or ''}"
                   class="in-place-edit-input" 
                   autofocus
                   hx-trigger="blur, keyup[enter]" 
                   hx-swap="outerHTML"
                   hx-confirm="Confirma a alteração do campo {campo}?">
        </form>
    </td>
    """

@router.post("/processar_acordo", response_class=HTMLResponse)
def processar_acordo(
    request: Request, 
    registro_ids: List[str] = Form([], alias="registro_ids"),
    status_acao: str = Form(..., alias="status_acao"),
    cache_key: str = Form(..., alias="cache_key") 
):
    user = get_current_user(request)
    _check_role_or_forbid(user, ["adm", "apoio qualidade", "apoio planejamento"])
    role = user.get("role", "default")
    if not registro_ids:
        raise HTTPException(
            status_code=422,
            detail="xPesquisax: Selecione pelo menos um registro para dar Acordo ou Não Acordo."
        )
    print(status_acao)
    registros_pesquisa = get_from_cache(cache_key)
    if not registros_pesquisa:
         raise HTTPException(status_code=404, detail="xPesquisax: Cache de pesquisa não encontrado ou expirado. Refaça a pesquisa.")
    ids_selecionados = set(registro_ids)
    registros_apos_acao = []
    for r in registros_pesquisa:
        if str(r.get("id")) not in ids_selecionados:
            registros_apos_acao.append(r)
        else:
            atributo, id, periodo = r.get("atributo"), r.get("nome"), r.get("periodo")
            update_da_adm_apoio(atributo, periodo, id, role, status_acao, user.get("usuario"))
            pass
    set_cache(cache_key, registros_apos_acao)
    return templates.TemplateResponse(
        "_pesquisa.html", 
        {
            "request": request, 
            "registros": registros_apos_acao,
            "show_checkbox": True
        }
    )