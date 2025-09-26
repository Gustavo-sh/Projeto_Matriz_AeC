
from fastapi import APIRouter, Request, Form, Query, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from datetime import datetime, timedelta
from passlib.context import CryptContext
import pyodbc
import calendar
import uuid

# IMPORTS FROM CACHE: mantive as antes e adicionei set_session e get_current_user
from app.cache import (
    get_from_cache, set_cache, load_registros, save_registros,
    set_session, get_current_user
)
from app.conexoes_bd import get_atributos, get_indicadores, get_operacao, get_funcao, get_operacoes_adm, get_resultados
from app.excels import (
    generate_registros_excel, generate_users_excel, get_user,
    save_user, save_registros_to_excel
)

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
SESSION_COOKIE = "logged_in"
adms = ["277561"]


# --- Helper para checar role (usado dentro das rotas para preservar comportamento de redirect) ---
def _check_role_or_forbid(user: dict, allowed_roles: list[str]):
    """
    Lança HTTPException(403) se o usuário não estiver autenticado ou não tiver a role permitida.
    """
    if not user:
        # sem sessão -> tratamos como não autenticado (quem chama deve redirecionar)
        return False
    if user.get("role") not in allowed_roles:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Acesso negado.")
    return True


@router.post("/delete/{id}", response_class=HTMLResponse)
def delete_registro(request: Request, id: int):
    registros = load_registros(request)
    registros = [r for r in registros if r["id"] != id]
    save_registros(request, registros)
    return templates.TemplateResponse("_registro.html", {"request": request, "registros": registros})


@router.get("/")
def home():
    generate_users_excel()
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
    generate_registros_excel()
    user = get_user(username)

    if not user:
        return RedirectResponse("/login?erro=Usuário não cadastrado!", status_code=303)

    if not pwd_context.verify(password, user["password"]):
        return RedirectResponse("/login?erro=Senha incorreta!", status_code=303)

    # cria sessão no redis com token e role
    session_token = str(uuid.uuid4())
    set_session(session_token, {"usuario": username, "role": user.get("role")})

    resp = RedirectResponse("/redirect_by_role", status_code=303)
    # Cookie da sessão (utilizado pelo get_current_user)
    resp.set_cookie("session_token", session_token, httponly=True)
    # manter cookies originais para compatibilidade (username, logged_in, last_active)
    resp.set_cookie("logged_in", "true", httponly=True)
    resp.set_cookie("last_active", datetime.utcnow().isoformat(), httponly=True)
    resp.set_cookie("username", username, httponly=True)
    return resp


@router.get("/redirect_by_role")
def redirect_by_role(request: Request):
    # tenta recuperar usuário via session_token (armazenado no redis)
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
    return resp


@router.get("/register")
def register_page(request: Request, erro: Optional[str] = Query(None)):
    return templates.TemplateResponse("register.html", {"request": request, "erro": erro})


@router.post("/register")
def register_user(request: Request, username: str = Form(...), password: str = Form(...)):
    if get_user(username):
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

        print(funcao)
        funcao_upper = funcao.upper() if funcao else ""

        if "COORDENADOR DE QUALIDADE" in funcao_upper or "GERENTE DE QUALIDADE" in funcao_upper:
            role = "apoio qualidade"
        elif "COORDENADOR DE PLANEJAMENTO" in funcao_upper or "GERENTE DE PLANEJAMENTO" in funcao_upper:
            role = "apoio planejamento"
        elif "COORDENADOR DE OPERACAO" in funcao_upper or "GERENTE DE OPERACAO" in funcao_upper:
            role = "operacao"
        else:
            role = None

    if not role:
        return RedirectResponse("/register?erro=Função não autorizada para cadastro.", status_code=303)

    hashed_password = pwd_context.hash(password)
    save_user(username, hashed_password, role)

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
    operacoes = get_operacao(username)
    registros = load_registros(request)
    return templates.TemplateResponse("index.html", {
        "request": request,
        "registros": registros,
        "indicadores": indicadores,
        "username": username,
        "atributos": [],
        "operacoes": operacoes
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
    operacoes = get_operacao(username)
    registros = load_registros(request)
    return templates.TemplateResponse("indexApoio.html", {
        "request": request,
        "registros": registros,
        "indicadores": indicadores,
        "username": username,
        "atributos": [],
        "operacoes": operacoes
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
    operacoes = get_operacoes_adm(username)
    registros = load_registros(request)
    return templates.TemplateResponse("indexAdm.html", {
        "request": request,
        "registros": registros,
        "indicadores": indicadores,
        "username": username,
        "atributos": [],
        "operacoes": operacoes
    })

@router.post("/get_atributos", response_class=HTMLResponse)
def get_atributos_by_operacao(request: Request, operacao: str = Form(...)):
    atributos = get_atributos(operacao)
    return templates.TemplateResponse("_atributos.html", {
        "request": request,
        "atributos": atributos
    })

@router.post("/add", response_class=HTMLResponse)
def add_registro(
    request: Request,
    nome: str = Form(...),
    meta: str = Form(...),
    moeda: int = Form(...),
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
    periodo: str = Form(...)
):
    registros = load_registros(request)

    novo = {
        "id": len(registros) + 1,
        "nome": nome, "meta": meta, "moeda": moeda,
        "criterio_final": criterio_final, "area": area,
        "tipo_faturamento": tipo_faturamento, "escala": escala,
        "acumulado": acumulado, "tipo_matriz": tipo_matriz,
        "esquema_acumulado": esquema_acumulado, "descricao": descricao,
        "ativo": ativo or "", "chamado": chamado, "possuiDmm": possuiDmm,
        "dmm": dmm, "atributo": atributo,
        "tipo_indicador": tipo_indicador,
        "data_inicio": data_inicio,
        "data_fim": data_fim,
        "periodo": periodo
    }
    registros.append(novo)
    save_registros(request, registros)

    return templates.TemplateResponse("_registro.html", {"request": request, "registros": registros})


@router.post("/pesquisar", response_class=HTMLResponse)
def pesquisar(request: Request, atributo: str = Form(...)):
    cache_key = f"pesquisa:{atributo}"
    registros = []

    cached = get_from_cache(cache_key)
    if cached:
        registros = cached
    else:
        conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
        cur = conn.cursor()
        cur.execute(f"""
            SELECT distinct [Id_Indicador], [Nome_indicador], [Meta], [Ganho_G1],
                            [Responsavel], [Escala], [Acumulado], [Data_inicio], [Data_fim],
                            [tipo_matriz], [esquema_acumulado], [descricao], [ativo],
                            [chamado]
            FROM [Robbyson].[rby].[Import_Matriz]
            WHERE Atributo = '{atributo}'
              AND Data_inicio = dateadd(d,1,eomonth(GETDATE(),-1))
        """)
        resultados = cur.fetchall()
        cur.close()
        conn.close()

        registros = [{
            "id_indicador": row[0], "nome_indicador": row[1], "meta": row[2],
            "ganho_g1": row[3], "responsavel": row[4], "escala": row[5],
            "acumulado": row[6], "data_inicio": row[7], "data_fim": row[8],
            "tipo_matriz": row[9], "esquema_acumulado": row[10],
            "descricao": row[11], "ativo": row[12], "chamado": row[13],
        } for row in resultados]

        set_cache(cache_key, registros)

    return templates.TemplateResponse("_pesquisa.html", {"request": request, "registros": registros})


@router.post("/submit_table", response_class=HTMLResponse)
def submit_table(request: Request):
    registros = load_registros(request)
    username = request.cookies.get("username", "anon")

    if not registros:
        return "<p>Nenhum registro para submeter.</p>"

    save_registros_to_excel(registros, username)

    return "<p>Tabela submetida com sucesso!</p>"

@router.post("/trazer_resultados", response_class=HTMLResponse)
def trazer_resultados(request: Request, atributo: str = Form(...), nome: str = Form(...)):
    # Aqui você chama sua função que traz os dados
    if len(nome.split(" - ")) == 1:
        raise HTTPException(
            status_code=422,
            detail="Selecione um atributo e um indicador primeiro!"
        )
    
    print(nome.split(" - "))
    id_indicador = nome.split(" - ")[0]
    query = get_resultados(atributo, id_indicador)

    # Supondo que a query retorna uma lista de dicts ou tuplas
    # Pegamos o primeiro registro (ou você pode tratar mais se precisar)
    if not query:
        return HTMLResponse("<div>Nenhum resultado encontrado</div>")
    
    row = query[0]  # pega o primeiro resultado

    # Renderiza um template parcial que contém os inputs preenchidos
    return templates.TemplateResponse(
        "_resultados.html",  # novo template parcial só para os campos de metas
        {
            "request": request,
            "meta_sugerida": row[9],
            "meta_escolhida": row[10],
            "atingimento_projetado": row[21],
            "resultado_m0": row[12],
            "atingimento_m0": row[13],
            "resultado_m1": row[15],
            "atingimento_m1": row[16],
            "max_data": row[17]
        }
    )