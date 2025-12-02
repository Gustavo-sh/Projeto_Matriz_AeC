from fastapi import APIRouter, Request, Form, Query, HTTPException, Response, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from datetime import datetime, timedelta
from passlib.context import CryptContext
from urllib.parse import urlparse
import uuid
from typing import List 
import pandas as pd
from io import BytesIO
from fastapi.responses import StreamingResponse
import json
from fastapi import UploadFile, File
from app.cache import (
    get_from_cache, set_cache, load_registros, save_registros, set_session, get_current_user
)
from app.connections_db import (
    get_indicadores, get_funcao, get_resultados, get_atributos_matricula, get_user_bd, save_user_bd, save_registros_bd,
    get_atributos_adm, update_da_adm_apoio, batch_validar_submit_query, get_num_atendentes, import_from_excel, 
    get_acordos_apoio, get_nao_acordos_apoio, get_atributos_apoio, get_atributos_gerente, update_meta_moedas_bd, get_nao_acordos_exop,
    get_all_atributos_cadastro_apoio, get_matrizes_administrativas_pg_adm, get_matrizes_nao_cadastradas, get_matrizes_alteradas_apoio, update_dmm_bd, query_mes, 
    get_factibilidade, insert_log_meta_moedas,
)
from app.validations import validation_submit_table, validation_import_from_excel, validation_meta_moedas, validation_dmm, validation_datas

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
SESSION_COOKIE = "logged_in"
adms = ["277561", "117699", "154658", "160031", "086939"]
adm_acordo = ["277561"]
EXPECTED_COLUMNS = [
    'atributo', 'id_nome_indicador', 'meta_sugerida', 'resultado', 'atingimento', 'meta', 'moedas', 'tipo_indicador', 
        'acumulado', 'esquema_acumulado', 'tipo_matriz', 'data_inicio', 
        'data_fim', 'periodo', 'escala', 'tipo_de_faturamento', 
        'descricao', 'ativo', 'chamado', 'criterio', 'gerente', 'possui_dmm', 'dmm', 'submetido_por', 
        'data_submetido_por', 'qualidade', 'da_qualidade', 'data_da_qualidade', 
        'planejamento', 'da_planejamento', 'data_da_planejamento', 'exop', 'da_exop', 'data_da_exop'
]

# EXPECTED_COLUMNS = [
#     'atributo', 'id_nome_indicador', 'meta_sugerida', 'resultado', 'atingimento', 'meta', 'moedas', 'tipo_indicador', 
#         'acumulado', 'esquema_acumulado', 'tipo_matriz', 'data_inicio', 
#         'data_fim', 'periodo', 'escala', 'tipo_de_faturamento', 
#         'descricao', 'ativo', 'chamado', 'criterio', 'area', 
#         'responsavel', 'gerente', 'possui_dmm', 'dmm', 'submetido_por', 
#         'data_submetido_por', 'qualidade', 'da_qualidade', 'data_da_qualidade', 
#         'planejamento', 'da_planejamento', 'data_da_planejamento', 'exop', 'da_exop', 'data_da_exop'
# ]

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
async def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...)
):
    user = await get_user_bd(username)
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
    resp.set_cookie("role", user.get("role"), httponly=True)
    return resp

@router.get("/redirect_by_role")
def redirect_by_role(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    role = user.get("role")
    if role == "operacao":
        return RedirectResponse("/matriz/operacao")
    elif role in ["apoio qualidade", "apoio planejamento"]:
        return RedirectResponse("/matriz/apoio")
    elif role == "adm":
        return RedirectResponse("/matriz/adm")
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
async def register_user(request: Request, username: str = Form(...), password: str = Form(...)):
    if await get_user_bd(username):
        return RedirectResponse("/register?erro=Usuário já cadastrado!", status_code=303)
    role = None
    if username in adms:
        role = "adm"
    else:
        funcao = None
        try:
            funcao = await get_funcao(username)
        except Exception as e:
            funcao = f"Erro ao obter funcao: {e}"
        funcao_upper = funcao.upper() if funcao else ""
        if "COORDENADOR DE QUALIDADE" in funcao_upper or "GERENTE DE QUALIDADE" in funcao_upper:
            role = "apoio qualidade"
        elif "COORDENADOR DE PLANEJAMENTO" in funcao_upper or "GERENTE DE PLANEJAMENTO" in funcao_upper:
            role = "apoio planejamento"
        elif "GERENTE DE OPERACAO" in funcao_upper:
            role = "operacao"
        elif "SUPERINTENDENTE DE OPERACAO" in funcao_upper:
            role = "operacao"
        elif "DESENVOLVIMENTO OPERACIONAL" in funcao_upper:
            role = "adm"
    if not role:
        return RedirectResponse("/register?erro=Função não autorizada para cadastro.", status_code=303)
    hashed_password = pwd_context.hash(password)
    await save_user_bd(username, hashed_password, role)
    return RedirectResponse("/login?msg=Usuário cadastrado com sucesso!", status_code=303)

@router.get("/matriz/operacao")
async def matriz_page(request: Request):
    logged_in = request.cookies.get(SESSION_COOKIE)
    if not logged_in or logged_in != "true":
        return RedirectResponse("/login", status_code=303)
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    _check_role_or_forbid(user, ["operacao"])
    username = request.cookies.get("username")
    indicadores = await get_indicadores()
    lista_atributos = await get_atributos_matricula(username)
    atributos = sorted(lista_atributos, key=lambda item: item.get('atributo') or '')
    matrizes_alteradas = await get_matrizes_alteradas_apoio(username)
    registros = load_registros(request)
    area = None
    funcao = await get_funcao(username)
    if "qualidade" in funcao.lower():
        area = "Qualidade"
    elif "planejamento" in funcao.lower():
        area = "Planejamento"
    return templates.TemplateResponse("indexOperacao.html", {
        "request": request,
        "registros": registros,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos,
        "role_": user.get("role"),
        "area": area,
        "matrizes_alteradas": matrizes_alteradas
    })

@router.get("/matriz/apoio")
async def index_apoio(request: Request):
    logged_in = request.cookies.get(SESSION_COOKIE)
    if not logged_in or logged_in != "true":
        return RedirectResponse("/login", status_code=303)
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    _check_role_or_forbid(user, ["apoio qualidade", "apoio planejamento"])
    username = request.cookies.get("username")
    indicadores = await get_indicadores()
    
    registros = load_registros(request)
    area = None
    funcao = await get_funcao(username)
    if "qualidade" in funcao.lower():
        area = "Qualidade"
    elif "planejamento" in funcao.lower():
        area = "Planejamento"
    atributos = await get_atributos_apoio(area)
    return templates.TemplateResponse("indexApoio.html", {
        "request": request,
        "registros": registros,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos,
        "role": user.get("role"),
        "area": area
    })

@router.get("/matriz/apoio/cadastro")
async def index_apoio(request: Request):
    logged_in = request.cookies.get(SESSION_COOKIE)
    if not logged_in or logged_in != "true":
        return RedirectResponse("/login", status_code=303)
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    _check_role_or_forbid(user, ["apoio qualidade", "apoio planejamento", "adm"])
    username = request.cookies.get("username")
    indicadores = await get_indicadores()
    area = None
    funcao = await get_funcao(username)
    if "qualidade" in funcao.lower():
        area = "QUALID"
    elif "planejamento" in funcao.lower():
        area = "PLAN"
    atributos = await get_all_atributos_cadastro_apoio(area)
    registros = load_registros(request)
    funcao = await get_funcao(username)
    if area == "QUALID":
        area = "Qualidade"
    elif area == "PLAN":
        area = "Planejamento"
    else:
        area = None
    return templates.TemplateResponse("indexApoioCadastro.html", {
        "request": request,
        "registros": registros,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos,
        "role_": user.get("role"),
        "area": area
    })

@router.get("/matriz/adm")
async def index_adm(request: Request):
    logged_in = request.cookies.get(SESSION_COOKIE)
    if not logged_in or logged_in != "true":
        return RedirectResponse("/login", status_code=303)
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    _check_role_or_forbid(user, ["adm"])
    username = request.cookies.get("username")
    indicadores = await get_indicadores()
    atributos = await get_atributos_adm()
    registros = load_registros(request)
    area = None
    funcao = await get_funcao(username)
    if "qualidade" in funcao.lower():
        area = "Qualidade"
    elif "planejamento" in funcao.lower():
        area = "Planejamento"
    return templates.TemplateResponse("indexAdm.html", {
        "request": request,
        "registros": registros,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos,
        "role_": user.get("role"),
        "area": area
    })

@router.get("/matriz/adm/acordo")
async def index_adm(request: Request):
    logged_in = request.cookies.get(SESSION_COOKIE)
    if not logged_in or logged_in != "true":
        return RedirectResponse("/login", status_code=303)
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    _check_role_or_forbid(user, ["adm"])
    username = request.cookies.get("username")
    indicadores = await get_indicadores()
    atributos = await get_atributos_adm()
    registros = load_registros(request)
    return templates.TemplateResponse("indexAdmAcordo.html", {
        "request": request,
        "registros": registros,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos,
        "role_": user.get("role")
    })

@router.post("/add", response_class=HTMLResponse)
async def add_registro(
    request: Request,
    nome: str = Form(...),
    meta: str = Form(...),
    moeda: str = Form(...),
    criterio_final: Optional[str] = Form(None),
    #area: str = Form(...),
    tipo_faturamento: str = Form(...),
    escala: str = Form(...),
    acumulado: str = Form(...),
    tipo_matriz: str = Form(...),
    esquema_acumulado: str = Form(...),
    descricao: Optional[str] = Form(None),
    ativo: Optional[str] = Form(None),
    chamado: Optional[str] = Form(None),
    atributo: str = Form(...),
    tipo_indicador: str = Form(...),
    data_inicio: str = Form(...),
    data_fim: str = Form(...),
    periodo: str = Form(...),
    gerente: str = Form(...),
    #responsavel: str = Form(...)
    ):
    registros = load_registros(request)
    novo_id = str(uuid.uuid4())
    fact = await get_factibilidade(atributo, nome.split(" - ")[0])
    novo = {
        "id": novo_id,
        "atributo": atributo, "id_nome_indicador": nome, "meta_sugerida": fact[0]["metasugerida"] if fact else '', "resultado": fact[0]["resultado"] if fact else '', "atingimento": fact[0]["atingimento"] if fact else '',
        "meta": meta, "moedas": moeda,"tipo_indicador": tipo_indicador,"acumulado": acumulado,"esquema_acumulado": esquema_acumulado,
        "tipo_matriz": tipo_matriz,"data_inicio": data_inicio,"data_fim": data_fim,"periodo": periodo,"escala": escala,"tipo_de_faturamento": tipo_faturamento,
        "descricao": descricao or '',"ativo": ativo or "","chamado" or '': chamado,"criterio": criterio_final, "gerente": gerente,
        "possui_dmm": 'Não',"dmm": ''
    }
    if not atributo or not nome or not meta or not moeda or not data_inicio or not data_fim or not escala or not tipo_faturamento or not criterio_final:  
        raise HTTPException(
            status_code=422,
            detail="xIndicadorx: Preencha todos os campos obrigatórios!"
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

@router.post("/pesquisar_mes", response_class=HTMLResponse)
async def pesquisar_mes(request: Request, atributo: str = Form(...), mes: str = Form(...)):
    try:
        registros = []

        current_page = request.headers.get("hx-current-url", "desconhecido")
        path = urlparse(current_page).path.lower()

        if not atributo:
            raise HTTPException(
                status_code=422,
                detail="xFiltrox: Selecione um atributo primeiro!"
            )

        username = request.cookies.get("username", "anon")

        funcao = await get_funcao(username)
        area = None

        if funcao:
            if "qualidade" in funcao.lower():
                area = "Qualidade"
            elif "planejamento" in funcao.lower():
                area = "Planejamento"

        if "cadastro" in path:
            page = "cadastro"
            show_das = None
        else:
            page = "demais"
            show_das = True

        # if "/matriz/apoio" in path:
        #   show_checkbox = False

        show_checkbox = True
        if mes == "m+1":
            show_checkbox = False
            if "/matriz/apoio" in path or "/matriz/adm" in path:
                show_checkbox = True

        registros = await query_mes(atributo, username, page, area, mes)

        registros = [
            dic for dic in registros
            if dic.get("id_nome_indicador").lower() != "48 - presença"
        ]


        for dic in registros:
            if isinstance(dic.get("id_nome_indicador"), str) and \
               dic.get("id_nome_indicador").lower() == "901 - % disponibilidade":
                dic["meta_sugerida"] = 94.0

        if "operacao" in funcao.lower():
            html_content = templates.TemplateResponse("_pesquisaOperacao.html", {
                "request": request,
                "registros": registros,
                "show_checkbox": show_checkbox,
                "show_das": show_das
            })
        else:
            html_content = templates.TemplateResponse("_pesquisa.html", {
                "request": request,
                "registros": registros,
                "show_checkbox": show_checkbox,
                "show_das": show_das
            })

        response = Response(content=html_content.body, media_type="text/html")

        if registros:
            response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Pesquisa realizada com sucesso!"}'
        else:
            response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Sua pesquisa não trouxe resultados!"}'

        return response

    except Exception as e:
        return Response(
            content=f"Erro inesperado: {str(e)}",
            status_code=500
        )


@router.post("/pesquisar_acordos", response_class=HTMLResponse)
async def pesquisar_acordos(request: Request):
    registros = []
    current_page = request.headers.get("hx-current-url", "desconhecido")
    registros = await get_acordos_apoio()
    path = urlparse(current_page).path.lower()
    show_das = None
    if "cadastro" in path:
        show_das = None
    else:
        show_das = True
    html_content = templates.TemplateResponse(
    "_pesquisa.html", 
    {"request": request, "registros": registros, "show_checkbox": True, "show_das": show_das}
    )
    response = Response(content=html_content.body, media_type="text/html")
    if len(registros) > 0:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Pesquisa realizada com sucesso!"}'
    else:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Sua pesquisa não trouxe resultados!"}'
    return response

@router.post("/pesquisar_nao_acordos", response_class=HTMLResponse)
async def pesquisar_nao_acordos(request: Request):
    registros = []
    current_page = request.headers.get("hx-current-url", "desconhecido")
    registros = await get_nao_acordos_apoio()
    path = urlparse(current_page).path.lower()
    show_das = None
    if "cadastro" in path:
        show_das = None
    else:
        show_das = True
    html_content = templates.TemplateResponse(
    "_pesquisa.html", 
    {"request": request, "registros": registros, "show_checkbox": True, "show_das": show_das}
    )
    response = Response(content=html_content.body, media_type="text/html")
    if len(registros) > 0:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Pesquisa realizada com sucesso!"}'
    else:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Sua pesquisa não trouxe resultados!"}'
    return response

@router.post("/pesquisar_nao_acordos_exop", response_class=HTMLResponse)
async def pesquisar_nao_acordos_exop(request: Request):
    registros = []
    current_page = request.headers.get("hx-current-url", "desconhecido")
    registros = await get_nao_acordos_exop()
    path = urlparse(current_page).path.lower()
    show_das = None
    if "cadastro" in path:
        show_das = None
    else:
        show_das = True
    html_content = templates.TemplateResponse(
    "_pesquisa.html", 
    {"request": request, "registros": registros, "show_checkbox": True, "show_das": show_das}
    )
    response = Response(content=html_content.body, media_type="text/html")
    if len(registros) > 0:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Pesquisa realizada com sucesso!"}'
    else:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Sua pesquisa não trouxe resultados!"}'
    return response

@router.post("/pesquisar_matrizes_administrativas", response_class=HTMLResponse)
async def matrizes_administrativas_pg_adm(request: Request, tipo: str = Form(...)):
    current_page = request.headers.get("hx-current-url", "desconhecido")
    registros = await get_matrizes_administrativas_pg_adm(tipo)
    path = urlparse(current_page).path.lower()
    show_das = None
    if "cadastro" in path:
        show_das = None
    else:
        show_das = True
    html_content = templates.TemplateResponse(
    "_pesquisa.html", 
    {"request": request, "registros": registros, "show_checkbox": True, "show_das": show_das, "show_just": True}
    )
    response = Response(content=html_content.body, media_type="text/html")
    if len(registros) > 0:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Pesquisa realizada com sucesso!"}'
    else:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Sua pesquisa não trouxe resultados!"}'
    return response


@router.post("/all_atributes_operacao", response_class=HTMLResponse)
async def all_atributes_operacao(request: Request, tipo_pesquisa: str = Form(...)):
    registros = []
    current_page = request.headers.get("hx-current-url", "desconhecido")
    username = request.cookies.get("username", "anon")
    atributos = await get_atributos_matricula(username)

    atributos_format = " ,".join(f"'{a["atributo"]}'" for a in atributos)
    print(atributos_format)
    
    registros = await get_atributos_gerente(tipo_pesquisa, atributos_format, username)

    for dic in registros:
        if dic.get("id_nome_indicador").lower() == "48 - presença":
            registros.remove(dic)

    path = urlparse(current_page).path.lower()
    show_das = None
    if "cadastro" in path:
        show_das = None
    else:
        show_das = True
    html_content = templates.TemplateResponse(
    "_pesquisaOperacao.html", 
    {"request": request, "registros": registros, "show_checkbox": False, "show_das": show_das}
    )
    response = Response(content=html_content.body, media_type="text/html")
    if len(registros) > 0:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Pesquisa realizada com sucesso!"}'
    else:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xFiltrox: Sua pesquisa não trouxe resultados!"}'
    return response

@router.post("/submit_table", response_class=HTMLResponse)
async def submit_table(request: Request):
    registros = load_registros(request)
    username = request.cookies.get("username", "anon")
    if not registros:
        return "<p>Nenhum registro para submeter.</p>"
    num_atendentes = await get_num_atendentes(registros[0]["atributo"]) if "opera" in registros[0]["tipo_matriz"].lower() else None
    if "opera" in registros[0]["tipo_matriz"].lower():
        if num_atendentes == 0 or num_atendentes == '0':
            return "<p>Não é possível submeter a matriz, pois o atributo selecionado não possui nenhum atendente de nível 1.</p>"
    results = None
    try:
        results = await validation_submit_table(registros, username)
    except Exception as e:
        return f"<p>Erro Inesperado: {e}.</p>" 
    if isinstance(results, str):
        return results
    validation_conditions, registros = results
    existing_records = await batch_validar_submit_query(validation_conditions)
    for existing_row in existing_records:
        atributo_bd, periodo_bd, id_nome_indicador_bd, data_inicio_bd, data_fim_bd = existing_row
        for cond in validation_conditions:
            if (cond['atributo'] == atributo_bd and 
                cond['periodo'] == periodo_bd and 
                cond['id_nome_indicador'] == id_nome_indicador_bd):
                if validation_datas(data_inicio_bd, data_fim_bd, cond["data_inicio_sbmit"], cond["data_fim_submit"]):
                    return (
                        f"<p>O indicador {cond['id_nome_indicador']} ja foi submetido para o periodo - {cond['periodo']} e atributo - {cond['atributo']}.\nSe deseja alterar essa matriz, gentileza acessar link de alteração de matriz no botão links importantes.</p>"  
                    )
                    
    await save_registros_bd(registros, username, None, None)
    response = Response(
        content="<p>Tabela submetida com sucesso! A tabela ficará disponível caso queira replica-la para outros atributos.</p>",
        status_code=status.HTTP_200_OK,
        media_type="text/html"
    )
    response.headers["HX-Trigger"] = '{"mostrarSucesso": "Tabela submetida com sucesso"}' 
    return response

@router.post("/duplicate_search_results", response_class=HTMLResponse)
async def duplicate_search_results(
    request: Request,
    atributo: str = Form(...),
    tipo_pesquisa: str = Form(...),
    data_inicio: str = Form(...),
    data_fim: str = Form(...),
    periodo: str = Form(...),
    registro_ids: List[str] = Form([], alias="registro_ids")
):
    try:
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
        
        if not atributo:
            raise HTTPException(
                status_code=422,
                detail="xPesquisax: Selecione o atributo antes de duplicar."
            )

        current_page = request.headers.get("hx-current-url", "desconhecido").lower()
        path = urlparse(current_page).path.lower()

        page = "cadastro" if "cadastro" in path else "demais"

        user = get_current_user(request)
        role = user.get("role")

        # ✅ Nova estrutura de cache unificada
        cache_key = f"pesquisa_{tipo_pesquisa}:{atributo}:{page}"

        registros_da_pesquisa = get_from_cache(cache_key)

        if not registros_da_pesquisa:
            raise HTTPException(
                status_code=422,
                detail="xPesquisax: Nenhum resultado de pesquisa encontrado. Refaça a pesquisa antes de duplicar."
            )

        ids_selecionados = set(registro_ids)

        registros_a_duplicar = [
            r for r in registros_da_pesquisa
            if str(r.get("id")) in ids_selecionados
        ]

        if not registros_a_duplicar:
            raise HTTPException(
                status_code=422,
                detail="xPesquisax: Os registros selecionados não foram encontrados."
            )

        registros_atuais = load_registros(request)

        for registro in registros_a_duplicar:
            if registro.get("id_nome_indicador").lower() == "48 - presença":
                continue

            copia = registro.copy()
            copia["id"] = str(uuid.uuid4())
            copia["data_inicio"] = data_inicio
            copia["data_fim"] = data_fim
            copia["periodo"] = periodo
            copia["dmm"] = ''
            copia["possui_dmm"] = 'Não'
            copia["ativo"] = 0

            registros_atuais.append(copia)

        save_registros(request, registros_atuais)

        html_content = templates.TemplateResponse(
            "_registro.html",
            {"request": request, "registros": registros_atuais}
        )

        response = Response(content=html_content.body, media_type="text/html")
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "xPesquisax: Registros duplicados com sucesso!"}'
        return response

    except HTTPException:
        raise

    except Exception as e:
        return Response(
            content=f"xPesquisax: Erro inesperado ao duplicar registros. ({str(e)})",
            status_code=500
        )


@router.post("/update_registro/{registro_id}/{campo}", response_class=HTMLResponse)
def update_registro(request: Request, registro_id: str, campo: str, novo_valor: str = Form(..., alias="value")):
    user = get_current_user(request)
    registros = load_registros(request)
    registro_encontrado = None
    for reg in registros:
        if str(reg.get("id")) == registro_id:
            registro_encontrado = reg
            break 
    if not registro_encontrado:
        return Response(status_code=404, content=f"Registro ID {registro_id} não encontrado.")
    if campo not in ["meta", "moeda", "ativo"]: 
        return Response(status_code=400, content="Campo inválido para edição.")
    if campo == 'ativo':
        _check_role_or_forbid(user, ["adm"])
    valor_limpo = novo_valor.strip()
    valor_processado = valor_limpo 
    tipo_indicador = registro_encontrado.get("tipo_indicador")
    try:
        if campo == "moeda":
            if valor_limpo == '':
                valor_processado = 0
            else:
                valor_processado = int(valor_limpo.replace(',', '.'))
        if campo == "ativo":
            if valor_limpo == '':
                valor_processado = 0
            else:
                valor_processado = int(valor_limpo)
        elif tipo_indicador in ["Percentual"] and campo != "moeda":
            float(valor_limpo.replace(',', '.'))
        elif tipo_indicador in ["Inteiro"] and campo != "moeda":
            int(valor_limpo.replace(',', '.'))
        elif tipo_indicador in ["Decimal"] and campo != "moeda":
            float(valor_limpo.replace(',', '.'))
        elif tipo_indicador in ["Hora"] and campo != "moeda":
            partes = valor_limpo.split(":")
            if len(partes) < 3:
                return Response(status_code=404, content=f"Hora inválida: {novo_valor}.")
    except ValueError:
        error_message = f"Valor inválido para o campo {campo}."
        response = Response(content=f'{registro_encontrado.get(campo) or ""}', status_code=400)
        response.headers["HX-Retarget"] = "#mensagens-registros"
        response.headers["HX-Reswap"] = "innerHTML"
        response.headers["HX-Trigger"] = f'{{"mostrarErro": "{error_message}"}}'
        return response
    if campo == 'moeda':
        campo = 'moedas'
    registro_encontrado[campo] = valor_processado 
    save_registros(request, registros)
    return f'{registro_encontrado.get(campo) or ""}'

@router.get("/edit_campo/{registro_id}/{campo}", response_class=HTMLResponse)
def edit_campo_get(request: Request, registro_id: str, campo: str):
    user = get_current_user(request)
    input_type = "text"
    if campo == "ativo":
        _check_role_or_forbid(user, ["adm"])
        input_type = "number"   
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
                    type="{input_type}" 
                    value="{valor or ''}"
                    class="in-place-edit-input" 
                    autofocus
                    hx-trigger="focusout, keyup[enter]" 
                    hx-confirm="Confirma a alteração do campo {campo}?">
        </form>
    </td>
    """

@router.post("/processar_acordo", response_class=HTMLResponse)
async def processar_acordo(
    request: Request, 
    status_acao: str = Form(..., alias="status_acao"),
    cache_key: str = Form(..., alias="cache_key")
):
    user = get_current_user(request)
    _check_role_or_forbid(user, ["adm", "apoio qualidade", "apoio planejamento"])
    role = user.get("role", "default").lower().strip()

    status_acao = status_acao.lower().strip()
    cache_key = cache_key.strip()

    try:
        registros_pesquisa = get_from_cache(cache_key)
    except Exception:
        raise HTTPException(
            status_code=422,
            detail="xPesquisax: Erro ao acessar o cache da pesquisa."
        )

    if not registros_pesquisa:
        raise HTTPException(
            status_code=422,
            detail="xPesquisax: Cache de pesquisa não encontrado ou expirado. Refaça a pesquisa."
        )

    current_page = request.headers.get("hx-current-url", "desconhecido")
    try:
        path = urlparse(current_page).path.lower()
    except Exception:
        path = "desconhecido"

    show_das = None if "cadastro" in path else True

    registros_apos_acao = []
    updates_a_executar = []
    trava_da_exop = []

    for r in registros_pesquisa:
        atributo = str(r.get("atributo", "")).strip()
        periodo = str(r.get("periodo", "")).strip()

        updates_a_executar.append((atributo, periodo))
        trava_da_exop.append(r)

    if updates_a_executar:
        username = user.get("usuario")

        try:
            if role == "adm":
                for dic in trava_da_exop:
                    if int(dic.get("da_qualidade", 0)) == 0 or int(dic.get("da_planejamento", 0)) == 0:
                        raise HTTPException(
                            status_code=422,
                            detail="xPesquisax: Validação da qualidade ou do planejamento está ausente para o atributo selecionado."
                        )
        except Exception as e:
            raise HTTPException(
                status_code=422,
                detail=f"xPesquisax: Não foi possível validar os DA's das areas de apoio. ({e})"
            )
                    
        # if role == "adm":
        #     for dic in trava_da_exop:
        #         try:
        #             area = dic.get("area", "").lower().strip()
        #             indicador = dic.get("id_nome_indicador", "")
        #         except Exception:
        #             raise HTTPException(
        #                 status_code=422,
        #                 detail="xPesquisax: Não foi possível validar os dados selecionados."
        #             )

        #         if area == "qualidade":
        #             try:
        #                 if int(dic.get("da_qualidade", 0)) == 0 and "opera" in dic.get("tipo_matriz", "").lower():
        #                     raise HTTPException(
        #                         status_code=422,
        #                         detail=f"xPesquisax: O indicador {indicador} não tem De Acordo da Qualidade."
        #                     )
        #             except Exception:
        #                 raise HTTPException(
        #                     status_code=422,
        #                     detail=f"xPesquisax: Não foi possível verificar o De Acordo da Qualidade do indicador {indicador}."
        #                 )

        #         elif area == "planejamento":
        #             try:
        #                 if int(dic.get("da_planejamento", 0)) == 0 and "opera" in dic.get("tipo_matriz", "").lower():
        #                     raise HTTPException(
        #                         status_code=422,
        #                         detail=f"xPesquisax: O indicador {indicador} não tem De Acordo do Planejamento."
        #                     )
        #             except Exception:
        #                 raise HTTPException(
        #                     status_code=422,
        #                     detail=f"xPesquisax: Não foi possível verificar o De Acordo do Planejamento do indicador {indicador}."
        #                 )

        try:
            await update_da_adm_apoio(updates_a_executar, role, status_acao, username)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"xPesquisax: Erro ao atualizar os registros ({e})."
            )

    response = Response(content="", media_type="text/html")
    response.headers["HX-Trigger"] = json.dumps({
        "mostrarSucesso": {"value": "xPesquisax: DA atualizado com sucesso! Irá refletir no sistema quando o tempo da cache expirar."}
    })
    return response



# @router.post("/processar_acordo", response_class=HTMLResponse)
# async def processar_acordo(
#     request: Request, 
#     registro_ids: List[str] = Form([], alias="registro_ids"),
#     status_acao: str = Form(..., alias="status_acao"),
#     cache_key: str = Form(..., alias="cache_key") 
# ):
#     user = get_current_user(request)
#     _check_role_or_forbid(user, ["adm", "apoio qualidade", "apoio planejamento"])
#     role = user.get("role", "default")
#     if not registro_ids:
#         raise HTTPException(
#             status_code=422,
#             detail="xPesquisax: Selecione pelo menos um registro para dar Acordo ou Não Acordo."
#         )
#     registros_pesquisa = get_from_cache(cache_key)
#     print(cache_key)
#     if not registros_pesquisa:
#          raise HTTPException(status_code=422, detail="xPesquisax: Cache de pesquisa não encontrado ou expirado. Refaça a pesquisa.")
#     ids_selecionados = set(registro_ids)
#     registros_apos_acao = []
#     updates_a_executar = []
#     trava_da_exop = []
#     current_page = request.headers.get("hx-current-url", "desconhecido").lower()
#     path = urlparse(current_page).path.lower()
#     show_das = None
#     if "cadastro" in path:
#         show_das = None
#     else:
#         show_das = True
#     for r in registros_pesquisa:
#         if str(r.get("id")) not in ids_selecionados:
#             registros_apos_acao.append(r)
#         else:
#             atributo = r.get("atributo")
#             id_nome_indicador = r.get("id_nome_indicador") 
#             periodo = r.get("periodo")
#             updates_a_executar.append((atributo, periodo, id_nome_indicador)) 
#             trava_da_exop.append(r)
#     if role == 'adm':
#         updates_a_executar.append((atributo, periodo, '48 - Presença'))
#     if updates_a_executar:
#         role = user.get("role", "default")
#         username = user.get("usuario")
#         if role == "adm":
#             for dic in trava_da_exop:
#                 if dic["area"] == "Qualidade":
#                     try:
#                         if int(dic["da_qualidade"]) == 0:
#                             raise HTTPException(status_code=422, detail="xPesquisax: O indicar " + dic["id_nome_indicador"] + " não tem de acordo da qualidade.")
#                     except Exception:
#                         raise HTTPException(status_code=422, detail="xPesquisax: Não foi possível verificar o de acordo da qualidade do indicador " + dic["id_nome_indicador"])
#                 elif dic["area"] == "Planejamento":
#                     try:
#                         if int(dic["da_planejamento"]) == 0:
#                             raise HTTPException(status_code=422, detail="xPesquisax: O indicar " + dic["id_nome_indicador"] + " não tem de acordo da planejamento.")
#                     except Exception:
#                         raise HTTPException(status_code=422, detail="xPesquisax: Não foi possível verificar o de acordo da planejamento do indicador " + dic["id_nome_indicador"])
#         await update_da_adm_apoio(updates_a_executar, role, status_acao, username) 
#     CACHE_TTL = timedelta(minutes=1)
#     set_cache(cache_key, registros_apos_acao, CACHE_TTL)
#     return templates.TemplateResponse(
#         "_pesquisa.html", 
#         {
#             "request": request, 
#             "registros": registros_apos_acao,
#             "show_checkbox": True,
#             "show_das": show_das
#         }
#     )

@router.post("/update_meta_moedas", response_class=HTMLResponse)
async def update_meta_moedas(
    request: Request, 
    registro_ids: List[str] = Form([], alias="registro_ids"),
    meta: str = Form(..., alias="meta_duplicar"),
    moedas: str = Form(..., alias="moedas_duplicar"),
    cache_key: str = Form(..., alias="cache_key") 
):
    user = get_current_user(request)
    username = user.get("usuario")
    role = user.get("role")
    if not registro_ids:
        raise HTTPException(
            status_code=422,
            detail="xPesquisax: Selecione pelo menos um registro para alterar a meta."
        )
    if not meta:
        raise HTTPException(
            status_code=422,
            detail="xPesquisax: Preencha pelo menos o campo meta para efetuar a alteração."
        )
    registros_pesquisa = get_from_cache(cache_key)
    if len(registro_ids) > 1:
        raise HTTPException(
            status_code=422,
            detail="xPesquisax: Selecione apenas um campo para alterar."
        )
    if not registros_pesquisa:
        raise HTTPException(status_code=422, detail="xPesquisax: Cache de pesquisa não encontrado ou expirado. Refaça a pesquisa.")
    ids_selecionados = set(registro_ids)
    registros_apos_acao = []
    updates_a_executar = []
    registros_selecionados = []
    current_page = request.headers.get("hx-current-url", "desconhecido").lower()
    path = urlparse(current_page).path.lower()
    show_das = None
    if "cadastro" in path:
        show_das = None
    else:
        show_das = True
    for r in registros_pesquisa:
        if str(r.get("id")) not in ids_selecionados:
            if r.get("id_nome_indicador").lower() != "48 - presença":
                registros_apos_acao.append(r)
        else:
            erro = await validation_meta_moedas(r, meta, moedas, role)
            if erro:
                raise HTTPException(status_code=422, detail=erro)
            atributo = r.get("atributo")
            id_nome_indicador = r.get("id_nome_indicador") 
            periodo = r.get("periodo")
            updates_a_executar.append((atributo, periodo, id_nome_indicador)) 
            registros_selecionados.append(r)
    if updates_a_executar:
        await update_meta_moedas_bd(updates_a_executar, meta, moedas, role, username) 
        await insert_log_meta_moedas(registros_selecionados, meta, username)
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, registros_apos_acao, CACHE_TTL)
    return templates.TemplateResponse(
        "_pesquisa.html", 
        {
            "request": request, 
            "registros": registros_apos_acao,
            "show_checkbox": True,
            "show_das": show_das
        }
    )

@router.post("/update_dmm", response_class=HTMLResponse)
async def update_dmm(
    request: Request, 
):
    form = await request.form()
    dmm = (form.get("dmm_apoio") or "").strip()
    cache_key = (form.get("cache_key_pesquisa_dmm") or form.get("cache_key_pesquisa") or form.get("cache_key") or "").strip()
    erro = await validation_dmm(dmm)
    if erro:
        raise HTTPException(status_code=422, detail=erro)
    if not dmm:
        raise HTTPException(
            status_code=422,
            detail="xFiltrox: Coloquei exatamente 5 dmms para efetuar a alteração."
        )
    registros_pesquisa = get_from_cache(cache_key)
    if not registros_pesquisa:
        raise HTTPException(status_code=422, detail="xFiltrox: Cache de pesquisa não encontrado ou expirado. Refaça a pesquisa.")
    current_page = request.headers.get("hx-current-url", "desconhecido").lower()
    path = urlparse(current_page).path.lower()
    show_das = None
    if "cadastro" in path:
        show_das = None
    else:
        show_das = True
    await update_dmm_bd(registros_pesquisa[0]["atributo"], registros_pesquisa[0]["periodo"], dmm)
    for r in registros_pesquisa:
        r["possui_dmm"] = "Sim"
        r["dmm"] = dmm
    registros_apos_acao = [dic for dic in registros_pesquisa if dic["id_nome_indicador"].lower() != "48 - presença"]
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, registros_pesquisa, CACHE_TTL)
    return templates.TemplateResponse(
        "_pesquisa.html", 
        {
            "request": request, 
            "registros": registros_apos_acao,
            "show_checkbox": True,
            "show_das": show_das
        }
    )


@router.post("/clear_registros", response_class=HTMLResponse)
def clear_registros_route(request: Request):
    """
    Limpa os registros atuais do usuário no cache (session).
    Retorna uma string vazia para o HTMX limpar o elemento alvo no frontend.
    """
    try:
        save_registros(request, []) 
        return ""
    except Exception as e:
        print(f"Erro ao limpar registros: {e}")
        return HTMLResponse(content=f"<div style='color: red;'>Erro interno ao limpar os registros: {e}</div>", status_code=500)
    
@router.get("/export_table")
async def export_table(request: Request,  atributo: str = Query(...), tipo: str | None = Query(None, alias="duplicar_tipo_pesquisa"), cache_key: str = Query(None, alias="cache_key")):
    user = get_current_user(request)
    username = user.get("usuario")
    if not user:
        raise HTTPException(status_code=401, detail="Sessão inválida")
    if not tipo:
        raise HTTPException(status_code=422, detail="O tipo de pesquisa não foi recebido.")
    
    current_page = request.headers.get("hx-current-url", "desconhecido")
    page = None
    path = urlparse(current_page).path.lower()
    if "cadastro" in path:
        page = "cadastro"
    else:
        page = "demais"
    possible_keys = []
    if tipo == "m0_all" or tipo == "m1_all" or tipo == "m+1_all":
        possible_keys = [f"all_atributos:{tipo}:{username}"]
    elif tipo in ["m0_all_apoio", "m1_all_apoio", "m+1_all_apoio"]:
        possible_keys = [f"matrizes_administrativas:{tipo}:{username}"]
    elif tipo in ["m0_administrativas", "m+1_administrativas"]:
        possible_keys = [f"matrizes_administrativas_pg_adm:{tipo}"]
    else:
        if not atributo:
            raise HTTPException(status_code=422, detail="Informe o parâmetro 'atributo' para exportar.")
        if cache_key:
            possible_keys = [cache_key]
        else:
            tipo_map = {
                "m0": f"pesquisa_m0:{atributo}:{page}",
                "m1": f"pesquisa_m1:{atributo}:{page}",
                "m+1": f"pesquisa_m+1:{atributo}:{page}"
            }
            key = tipo_map.get(tipo)
            possible_keys = [key]

    registros_pesquisa = get_from_cache(possible_keys[0])

    if not registros_pesquisa:
        raise HTTPException(status_code=422, detail="Nenhum resultado de pesquisa encontrado no cache. Execute a pesquisa primeiro.")

    colunas = EXPECTED_COLUMNS
    df = pd.DataFrame(registros_pesquisa)
    final_cols = [c for c in colunas if c in df.columns]
    df = df[final_cols]
    colunas_to_drop = ['qualidade', 'da_qualidade', 'data_da_qualidade', 
        'planejamento', 'da_planejamento', 'data_da_planejamento']
    if "apoio" in tipo:
        df = df.drop(columns=colunas_to_drop)
    output = BytesIO()
    df.to_excel(output, index=False, sheet_name='Pesquisa', engine='openpyxl')
    output.seek(0)

    filename = f"pesquisa_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )

@router.get("/export_atributos_sem_matriz")
async def export_atributos_sem_matriz(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Sessão inválida")

    registros_pesquisa = await get_matrizes_nao_cadastradas()

    df = pd.DataFrame(registros_pesquisa, columns=['atributos'])

    output = BytesIO()
    df.to_excel(output, index=False, sheet_name='Matrizes_Nao_Cadastradas', engine='openpyxl')
    output.seek(0)

    filename = f"matrizes_nao_cadastradas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )

@router.post("/upload_excel", response_class=HTMLResponse)
async def upload_excel(request: Request, file: UploadFile = File(...)):
    username = request.cookies.get("username")
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        content = f"""<div id="upload_result" hx-swap-oob="true" class=mensagens-import>
        <p>xImportx: Envie um arquivo Excel (.xlsx ou .xls).</p></div>"""
        return HTMLResponse(content=content)
    try:
        content = await file.read()
        df = await run_in_threadpool(pd.read_excel, BytesIO(content))
    except Exception as e:
        await file.close()
        return HTMLResponse(
            content=f"""<div id="upload_result" hx-swap-oob="true" class=mensagens-import>
            <p>xImportx: Erro ao ler o arquivo Excel: {e}</p></div>"""
        )
    finally:
        await file.close()
    if df.empty:
        content = f"""<div id="upload_result" hx-swap-oob="true" class=mensagens-import>
        <p>xImportx: O arquivo Excel está vazio.</p></div>"""
        return HTMLResponse(content=content)
    df_cols = [c.strip() for c in df.columns]
    if df_cols != EXPECTED_COLUMNS:
        content = f"""<div id="upload_result" hx-swap-oob="true" class=mensagens-import>
        <p>xImportx: As colunas do arquivo não correspondem ao modelo esperado.<br>\nEsperado: {EXPECTED_COLUMNS}<br>Recebido: {df_cols}</p></div>"""
        return HTMLResponse(content=content)
    def clean_value(v):
        if isinstance(v, str):
            v = v.strip().replace("–", "-").replace("—", "-")
            v = v.replace("\n", " ").replace("\r", " ").replace("\xa0", " ")
            if v.lower() in ("nan", "none", "null", ""):
                return ""
            return v
        if pd.isna(v):
            return ""
        return v
    for col in df.columns:
        df[col] = df[col].apply(clean_value)
    date_cols = [c for c in df.columns if "data" in c.lower()]
    for col in date_cols:
        try:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        except Exception:
            df[col] = ""
    df = df.fillna("")
    def to_int_safe(v):
        try:
            if v == "" or pd.isna(v):
                return 0
            return int(float(v))
        except Exception:
            return 0
    for col in ["ativo", "qualidade", "da_qualidade", "planejamento", "da_planejamento", "exop", "da_exop"]:
        if col in df.columns:
            df[col] = df[col].apply(to_int_safe)
    records = df.to_dict(orient="records")
    valid_records = await validation_import_from_excel(records, request)
    if valid_records:
        return valid_records
    try:
        import_results = await import_from_excel(records, username)
        if import_results != 'True':
            content = f"""<div id="upload_result" hx-swap-oob="true" class=mensagens-import>
            <p>xImportx: Erro ao inserir os atributos, pois já existem dados para {import_results}</p></div>"""
            return HTMLResponse(content=content)
    except Exception as e:
        content = f"""<div id="upload_result" hx-swap-oob="true" class=mensagens-import>
        <p>xImportx: Erro ao inserir os registros na tabela Robbyson.dbo.Matriz_Geral: {e}.</p></div>"""
        return HTMLResponse(content=content)
    content = f"""<div id="upload_result" hx-swap-oob="true" class=mensagens-import>
    <p>xImportx: Todos os registros foram inseridos na tabela Robbyson.dbo.Matriz_Geral.</p></div>"""
    return HTMLResponse(content=content)

@router.post("/replicar_registros", response_class=HTMLResponse)
async def replicar_registros(request: Request, atributos_replicar: list[str] = Form(...)):
    user = None
    matricula = None
    try:
        user = get_current_user(request)
        matricula = user.get("usuario")
    except Exception:
        return HTMLResponse("<p>Erro: usuário não autenticado.</p>")

    atributos_destino = [a.strip() for a in atributos_replicar if a and a.strip()]
    if not atributos_destino:
        return HTMLResponse("<p>Nenhum atributo selecionado para replicação.</p>")

    registros = load_registros(request)

    if not registros:
        return HTMLResponse("<p>Não há registros carregados no cache para replicar.</p>")

    if isinstance(registros, str):
        try:
            registros = json.loads(registros)
        except Exception:
            return HTMLResponse("<p>Erro ao interpretar registros do cache.</p>")

    if not isinstance(registros, list) or not registros:
        return HTMLResponse("<p>Cache de registros inválido ou vazio.</p>")

    atributo_atual = registros[0].get("atributo")
    if not atributo_atual:
        return HTMLResponse("<p>Erro: não foi possível identificar o atributo atual nos registros.</p>")
    
    print(registros)

    novos_registros = []
    for destino in atributos_destino:
        for r in registros:
            novo = dict(r)
            novo["atributo"] = destino
            novo["submetido_por"] = matricula
            novo["data_submetido_por"] = datetime.now().strftime("%Y-%m-%d")
            novo["qualidade"] = 0
            novo["da_qualidade"] = 0
            novo["data_da_qualidade"] = ''
            novo["planejamento"] = 0
            novo["da_planejamento"] = 0
            novo["data_da_planejamento"] = ''
            novo["exop"] = 0
            novo["da_exop"] = 0
            novo["data_da_exop"] = ''
            if "id" in novo:
                novo["id"] = ""
            novos_registros.append(novo)
    results = await validation_submit_table(novos_registros, matricula)
    if isinstance(results, str):
        return results
    validation_conditions, registros = results
    existing_records = await batch_validar_submit_query(validation_conditions)
    for existing_row in existing_records:
        atributo_bd, periodo_bd, id_nome_indicador_bd, data_inicio_bd, data_fim_bd = existing_row
        for cond in validation_conditions:
            if (cond['atributo'] == atributo_bd and 
                cond['periodo'] == periodo_bd and 
                cond['id_nome_indicador'] == id_nome_indicador_bd):
                if validation_datas(data_inicio_bd, data_fim_bd, cond["data_inicio_sbmit"], cond["data_fim_submit"]):
                    return f"<p>O indicador {cond['id_nome_indicador']} ja foi submetido para o periodo - {cond['periodo']} e atributo - {cond['atributo']}.</p>" 
                
    if not novos_registros:
        return HTMLResponse("<p>Nenhum registro válido para replicar.</p>")

    try:
        await import_from_excel(novos_registros, matricula)
    except Exception as e:
        return HTMLResponse(f"<p>Erro ao inserir registros no banco: {e}</p>")

    return HTMLResponse(
        f"<p>Sucesso: {len(registros)} registros replicados do atributo "
        f"<strong>{atributo_atual}</strong> para "
        f"<strong>{', '.join(atributos_destino)}</strong>.</p>"
    )