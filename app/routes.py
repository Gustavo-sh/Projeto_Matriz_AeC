from fastapi import APIRouter, Request, Form, Query, HTTPException, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from datetime import datetime
from passlib.context import CryptContext
import uuid
from typing import List 
import pandas as pd
from io import BytesIO
from fastapi.responses import StreamingResponse
from app.cache import (
    get_from_cache, set_cache, load_registros, save_registros,
    set_session, get_current_user
)
from app.conexoes_bd import (
    get_indicadores, get_funcao, get_resultados, get_atributos_matricula, get_user_bd, save_user_bd, save_registros_bd, get_resultados_indicadores_m3,
    query_m0, query_m1, get_atributos_adm_apoio, update_da_adm_apoio, batch_validar_submit_query, validar_datas, get_num_atendentes
)
from app.validation import validation_submit_table

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
SESSION_COOKIE = "logged_in"
adms = ["277561", "117699"]

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
        elif "COORDENADOR DE OPERACAO" in funcao_upper or "GERENTE DE OPERACAO" in funcao_upper:
            role = "operacao"
        elif "ANALISTA DESENVOLVIMENTO OPERACIONAL" in funcao_upper:
            role = "adm"
        else:
            role = None
    if not role:
        return RedirectResponse("/register?erro=Função não autorizada para cadastro.", status_code=303)
    hashed_password = pwd_context.hash(password)
    await save_user_bd(username, hashed_password, role)
    return RedirectResponse("/login?msg=Usuário cadastrado com sucesso!", status_code=303)

@router.get("/matriz")
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
    registros = load_registros(request)
    return templates.TemplateResponse("index.html", {
        "request": request,
        "registros": registros,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos,
        "role_": user.get("role")
    })

@router.get("/indexApoio")
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
    atributos = await get_atributos_adm_apoio()
    registros = load_registros(request)
    return templates.TemplateResponse("indexApoio.html", {
        "request": request,
        "registros": registros,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos,
        "role_": user.get("role")
    })

@router.get("/indexAdm")
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
    atributos = await get_atributos_adm_apoio()
    registros = load_registros(request)
    return templates.TemplateResponse("indexAdm.html", {
        "request": request,
        "registros": registros,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos,
        "role_": user.get("role")
    })

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
async def pesquisar_m0(request: Request, atributo: str = Form(...)):
    registros = []
    if not atributo:
        raise HTTPException(
            status_code=422,
            detail="xFiltrox : Selecione um atributo primeiro!"
        )
    registros = await query_m0(atributo)

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
async def pesquisar_m1(request: Request, atributo: str = Form(...)):
    registros = []
    if not atributo:
        raise HTTPException(
            status_code=422,
            detail="xFiltrox: Selecione um atributo primeiro!"
        )
    user = get_current_user(request)
    role = "operacao" if "operacao" in user.get("role") else "adm_apoio"
    registros = await query_m1(atributo, role)
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

@router.post("/submit_table", response_class=HTMLResponse)
async def submit_table(request: Request):
    registros = load_registros(request)
    username = request.cookies.get("username", "anon")
    if not registros:
        return "<p>Nenhum registro para submeter.</p>"
    num_atendentes = await get_num_atendentes(registros[0]["atributo"]) if registros[0]["tipo_matriz"] == "OPERAÇÃO" else None
    if registros[0]["tipo_matriz"] == "OPERAÇÃO":
        if num_atendentes == 0 or num_atendentes == '0':
            return "<p>Não é possível submeter a matriz, pois o atributo selecionado não possui nenhum atendente de nível 1.</p>"
    results = None
    try:
        results = await validation_submit_table(registros)
    except Exception as e:
        return f"<p>Erro Inesperado: {e}.</p>" 
    results = await validation_submit_table(registros)
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
                if validar_datas(data_inicio_bd, data_fim_bd, cond["data_inicio_sbmit"], cond["data_fim_submit"]):
                    return "<p>Este indicador ja foi submetido para o periodo e atributo selecionado.</p>"     
    await save_registros_bd(registros, username)
    save_registros(request, [])
    response = Response(
        content="<p>Tabela submetida com sucesso! Atualizando página...</p>",
        status_code=status.HTTP_200_OK,
        media_type="text/html"
    )
    response.headers["HX-Trigger"] = '{"mostrarSucesso": "Tabela submetida com sucesso"}' 
    return response
    
@router.post("/trazer_resultados", response_class=HTMLResponse)
async def trazer_resultados(request: Request, atributo: str = Form(...), nome: str = Form(...)):
    if len(nome.split(" - ")) == 1:
        raise HTTPException(
            status_code=422,
            detail="xIndicadorx: Selecione um atributo e um indicador primeiro!"
        )
    id_indicador = nome.split(" - ")[0]
    query = await get_resultados(atributo, id_indicador)
    if not query:
        raise HTTPException(
            status_code=422,
            detail="xIndicadorx: Nenhum resultado encontrado para o indicador e atributo selecionados."
        )
    m1 = query[0] if len(query) > 1 else None
    m0 = query[1] if len(query) > 1 else query[0]
    return templates.TemplateResponse(
        "_resultados.html", 
        {
            "request": request,
            "meta_sugerida": m0[6] if m0[6] else "",
            "meta_escolhida": m0[7] if m0[7] else "",
            "atingimento_projetado": m0[8] if m0[8] else "",
            "resultado_m0": m0[4] if m1 else "",
            "atingimento_m0": m0[5] if m1 else "",
            "resultado_m1": m1[4] if m1 else m0[4],
            "atingimento_m1": m1[5] if m1 else m0[5],
            "max_data": m0[10] if m0 else m1[10]
        }
    )

@router.post("/duplicate_search_results", response_class=HTMLResponse)
def duplicate_search_results(
    request: Request, 
    atributo: str = Form(...), 
    tipo_pesquisa: str = Form(...),
    data_inicio: str = Form(...), 
    data_fim: str = Form(...), 
    periodo: str = Form(...),
    registro_ids: List[str] = Form([], alias="registro_ids"),
    dmm: str = Form(None, alias="dmm"),
    possuiDmm: str = Form(None, alias="possuiDmmDuplicar")
    ):
    if not data_inicio or not data_fim or not periodo:
          raise HTTPException(
              status_code=422,
              detail="xPesquisax: Selecione as datas de início e fim antes de duplicar!"
          )
    if dmm:
        if len(dmm.split(",")) < 5:
            raise HTTPException(
              status_code=422,
              detail="xPesquisax: Selecione exatamente 5 DMMS!"
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
            cache_key = f"pesquisa_m0:{atributo}"
        else:
            raise HTTPException(status_code=422, detail="xPesquisax: Role invalida!")
    elif tipo_pesquisa == "m1":
        if role == 'operacao':
            cache_key = f"pesquisa_m1:{atributo}"
        elif role == 'adm' or role == 'apoio qualidade' or role == 'apoio planejamento':
            cache_key = f"pesquisa_m1:{atributo}"
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
    if possuiDmm == "Sim" and (not dmm):
         raise HTTPException(
              status_code=422,
              detail="xPesquisax: Se 'Sim' for selecionado, selecione os 5 Dmms!"
          )
    for novo_registro in registros_a_duplicar:
        registro_copia = novo_registro.copy()
        registro_copia["id"] = str(uuid.uuid4())
        registro_copia["data_inicio"] = data_inicio
        registro_copia["data_fim"] = data_fim
        registro_copia["periodo"] = periodo 
        registro_copia["dmm"] = dmm
        registro_copia["possuiDmm"] = possuiDmm
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
        if campo == "ativo":
            valor_processado = int(valor_limpo)
        elif tipo_indicador in ["Percentual"]:
            float(valor_limpo.replace(',', '.'))
        elif tipo_indicador in ["Inteiro"]:
            int(valor_limpo.replace(',', '.'))
        elif tipo_indicador in ["Decimal"]:
            float(valor_limpo.replace(',', '.'))
        elif tipo_indicador in ["Hora"] and campo != "moeda":
            partes = valor_limpo.split(":")
            if len(partes) < 3:
                raise ValueError("Hora inválida")
    except ValueError:
        error_message = f"Valor inválido para o campo {campo}."
        response = Response(content=f'{registro_encontrado.get(campo) or ""}', status_code=400)
        response.headers["HX-Retarget"] = "#mensagens-registros"
        response.headers["HX-Reswap"] = "innerHTML"
        response.headers["HX-Trigger"] = f'{{"mostrarErro": "{error_message}"}}'
        return response

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
    registros_pesquisa = get_from_cache(cache_key)
    if not registros_pesquisa:
         raise HTTPException(status_code=422, detail="xPesquisax: Cache de pesquisa não encontrado ou expirado. Refaça a pesquisa.")
    ids_selecionados = set(registro_ids)
    registros_apos_acao = []
    updates_a_executar = []
    for r in registros_pesquisa:
        if str(r.get("id")) not in ids_selecionados:
            registros_apos_acao.append(r)
        else:
            atributo = r.get("atributo")
            id_nome_indicador = r.get("nome") 
            periodo = r.get("periodo")
            updates_a_executar.append((atributo, periodo, id_nome_indicador)) 
    if updates_a_executar:
        role = user.get("role", "default")
        username = user.get("usuario")
        await update_da_adm_apoio(updates_a_executar, role, status_acao, username) 
    set_cache(cache_key, registros_apos_acao)
    return templates.TemplateResponse(
        "_pesquisa.html", 
        {
            "request": request, 
            "registros": registros_apos_acao,
            "show_checkbox": True
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
    
@router.post("/export_table")
async def export_table_excel(request: Request):
    user = get_current_user(request)
    _check_role_or_forbid(user, ["adm", "apoio qualidade", "apoio planejamento"]) 
    try:
        body = await request.json()
        registros_pesquisa = body.get("table_data", [])
    except Exception as e:
        print(f"Erro ao receber ou processar JSON da tabela: {e}")
        raise HTTPException(status_code=400, detail="Erro ao processar dados da tabela.")
    if not registros_pesquisa:
        raise HTTPException(status_code=422, detail="A tabela de pesquisa está vazia. Não há dados para exportar.")
    df = pd.DataFrame(registros_pesquisa)
    output = BytesIO()
    try:
        df.to_excel(output, index=False, sheet_name='Resultados da Pesquisa', engine='openpyxl') 
    except Exception as e:
        print(f"Erro ao gerar Excel com Pandas: {e}")
        raise HTTPException(status_code=500, detail="Erro interno ao gerar arquivo Excel.")
    output.seek(0) # Volta para o início do buffer
    filename = f"pesquisa_tabela_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        output,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={
            'Content-Disposition': f'attachment; filename="{filename}"'
        }
    )