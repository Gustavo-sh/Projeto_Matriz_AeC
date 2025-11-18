from datetime import datetime
import uuid
from app.cache import get_from_cache, set_cache
from datetime import datetime, date
import calendar
from app.database import get_db_connection
import asyncio 
from datetime import timedelta

async def get_user_bd(username):
    cache_key = "user: " + username
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select * from Robbyson.dbo.Acessos_Matriz (nolock) where username = ?
            """,(username))
            resultados = [{"username": i[0], "password": i[1], "role": i[2]} for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    if len(resultados) == 1:
        return resultados[0]
    set_cache(cache_key, None)
    return None

async def save_user_bd(username, hashed_password, role):
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
            insert into Robbyson.dbo.Acessos_Matriz (username, password, role) values ('{username}', '{hashed_password}', '{role}')
            """)
            cur.commit()
            cur.close()
    await loop.run_in_executor(None, _sync_db_call)

# async def save_registros_bd(registros, username):
#     data = datetime.now()
#     username_sql = str(username) if username is not None else 'NULL'
#     data_sql = f"'{data}'" 
#     loop = asyncio.get_event_loop()
#     def _sync_db_call():
#         values_clauses = []
#         for i in registros:
#             row_values = [
#                 f"'{i['atributo']}'", f"'{i['id_nome_indicador']}'", f"'{i['meta']}'", f"'{i['moedas']}'",
#                 f"'{i['tipo_indicador']}'", f"'{i['acumulado']}'", f"'{i['esquema_acumulado']}'",
#                 f"'{i['tipo_matriz']}'", f"'{i['data_inicio']}'", f"'{i['data_fim']}'",
#                 f"'{i['periodo']}'", f"'{i['escala']}'", f"'{i['tipo_de_faturamento']}'",
#                 f"'{i['descricao']}'", f"'{i['ativo']}'", f"'{i['chamado']}'",
#                 f"'{i['criterio']}'", f"'{i['area']}'", f"'{i['responsavel']}'",
#                 f"'{i['gerente']}'", f"'{i['possui_dmm']}'", f"'{i['dmm']}'",
#                 username_sql, 
#                 data_sql,
#                 "'', '', '', '', '', '', '', '', ''"
#             ]
#             values_clauses.append(f"({', '.join(row_values)})")
#         full_values_string = ",\n".join(values_clauses)
#         with get_db_connection() as conn:
#             cur = conn.cursor()
            
#             if full_values_string:
#                 batch_insert_query = f"""
#                     insert into Robbyson.dbo.Matriz_Geral values 
#                     {full_values_string}
#                 """
#                 cur.execute(batch_insert_query)
#                 cur.commit()
#             cur.close()
            
#     await loop.run_in_executor(None, _sync_db_call)

async def save_registros_bd(registros, username, justificativa, ativo):
    data = datetime.now()
    username_val = str(username) if username is not None else None
    data_val = data

    NUM_COLUNAS_DADOS = 22 
    NUM_COLUNAS_ADICIONAIS = 2 
    NUM_COLUNAS_VAZIAS = 11
    TOTAL_COLUNAS = NUM_COLUNAS_DADOS + NUM_COLUNAS_ADICIONAIS + NUM_COLUNAS_VAZIAS

    loop = asyncio.get_event_loop()

    def _sync_db_call():
        all_rows = []

        for i in registros:
            # Forçar apenas o campo 'meta' como string
            meta_val = str(i.get('meta')) if i.get('meta') is not None else ''

            row_data = [
                i.get('atributo'),
                i.get('id_nome_indicador'),
                meta_val,  # apenas meta forçado para str
                i.get('moedas'),
                i.get('tipo_indicador'),
                i.get('acumulado'),
                i.get('esquema_acumulado'),
                i.get('tipo_matriz'),
                i.get('data_inicio'),
                i.get('data_fim'),
                i.get('periodo'),
                i.get('escala'),
                i.get('tipo_de_faturamento'),
                i.get('descricao'),
                ativo if ativo is not None else i.get('ativo'),
                i.get('chamado'),
                i.get('criterio'),
                i.get('area'),
                i.get('responsavel'),
                i.get('gerente'),
                i.get('possui_dmm'),
                i.get('dmm'),
                username_val,
                data_val,
                '', '', '', '', '', '', '', '', '', justificativa if justificativa is not None else '', ''
            ]
            all_rows.append(tuple(row_data))

        if not all_rows:
            return

        colunas = (
            "atributo,id_nome_indicador,meta,moedas,tipo_indicador,acumulado,esquema_acumulado,"
            "tipo_matriz,data_inicio,data_fim,periodo,escala,tipo_de_faturamento,descricao,ativo,"
            "chamado,criterio,area,responsavel,gerente,possui_dmm,dmm,submetido_por,data_submetido_por,"
            "qualidade,da_qualidade,data_da_qualidade,planejamento,da_planejamento,data_da_planejamento,"
            "exop,da_exop,data_da_exop,justificativa,da_superintendente"
        )

        placeholders = ", ".join(["?"] * TOTAL_COLUNAS)

        insert_query = f"""
            INSERT INTO Robbyson.dbo.Matriz_Geral ({colunas})
            VALUES ({placeholders})
        """

        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.executemany(insert_query, all_rows)
            conn.commit()
            cur.close()

    await loop.run_in_executor(None, _sync_db_call)


# async def import_from_excel(registros):
#     retorno = None
#     loop = asyncio.get_event_loop()
#     def _sync_db_call():
#         values_clauses = []
#         for i in registros:
#             row_values = [
#                 f"'{i['atributo']}'", f"'{i['id_nome_indicador']}'", f"'{i['meta']}'", f"'{i['moedas']}'",
#                 f"'{i['tipo_indicador']}'", f"'{i['acumulado']}'", f"'{i['esquema_acumulado']}'",
#                 f"'{i['tipo_matriz']}'", f"'{i['data_inicio']}'", f"'{i['data_fim']}'",
#                 f"'{i['periodo']}'", f"'{i['escala']}'", f"'{i['tipo_de_faturamento']}'",
#                 f"'{i['descricao']}'", f"'{i['ativo']}'", f"'{i['chamado']}'",
#                 f"'{i['criterio']}'", f"'{i['area']}'", f"'{i['responsavel']}'",
#                 f"'{i['gerente']}'", f"'{i['possui_dmm']}'", f"'{i['dmm']}'",
#                 f"'{i['submetido_por']}'", f"'{i['data_submetido_por']}'", f"'{i['qualidade']}'",
#                 f"'{i['da_qualidade']}'", f"'{i['data_da_qualidade']}'", f"'{i['planejamento']}'", f"'{i['da_planejamento']}'",
#                 f"'{i['data_da_planejamento']}'", f"'{i['exop']}'", f"'{i['da_exop']}'",f"'{i['data_da_exop']}'"
#             ]
#             values_clauses.append(f"({', '.join(row_values)})")
#         full_values_string = ",\n".join(values_clauses)
#         with get_db_connection() as conn:
#             cur = conn.cursor()
#             if full_values_string:
#                 batch_insert_query = f"""
#                     insert into Robbyson.dbo.Matriz_Geral values 
#                     {full_values_string}
#                 """
#                 try:
#                     cur.execute(batch_insert_query)
#                     cur.commit()
#                     return True
#                 except:
#                     cur.rollback()
#             cur.close()
#     return await loop.run_in_executor(None, _sync_db_call)

async def import_from_excel(registros, username):
    data = datetime.now()
    TOTAL_COLUNAS = 35
    loop = asyncio.get_event_loop()

    def _sync_db_call():
        all_rows = []

        for i in registros:
            # Forçar apenas o campo 'meta' como string
            meta_val = str(i.get('meta')) if i.get('meta') is not None else ''

            row_data = [
                i.get('atributo'),
                i.get('id_nome_indicador'),
                meta_val,  # meta sempre string
                i.get('moedas'),
                i.get('tipo_indicador'),
                i.get('acumulado'),
                i.get('esquema_acumulado'),
                i.get('tipo_matriz'),
                i.get('data_inicio'),
                i.get('data_fim'),
                i.get('periodo'),
                i.get('escala'),
                i.get('tipo_de_faturamento'),
                i.get('descricao'),
                i.get('ativo'),
                i.get('chamado'),
                i.get('criterio'),
                i.get('area'),
                i.get('responsavel'),
                i.get('gerente'),
                i.get('possui_dmm'),
                i.get('dmm'),
                username,
                data,
                i.get('qualidade'),
                i.get('da_qualidade'),
                i.get('data_da_qualidade'),
                i.get('planejamento'),
                i.get('da_planejamento'),
                i.get('data_da_planejamento'),
                i.get('exop'),
                i.get('da_exop'),
                i.get('data_da_exop'),
                i.get('justificativa'),
                i.get('da_superintendente')
            ]
            all_rows.append(tuple(row_data))

        if not all_rows:
            return

        colunas = (
            "atributo,id_nome_indicador,meta,moedas,tipo_indicador,acumulado,esquema_acumulado,"
            "tipo_matriz,data_inicio,data_fim,periodo,escala,tipo_de_faturamento,descricao,ativo,"
            "chamado,criterio,area,responsavel,gerente,possui_dmm,dmm,submetido_por,data_submetido_por,"
            "qualidade,da_qualidade,data_da_qualidade,planejamento,da_planejamento,data_da_planejamento,"
            "exop,da_exop,data_da_exop,justificativa,da_superintendente"
        )

        placeholders = ", ".join(["?"] * TOTAL_COLUNAS)

        insert_query = f"""
            INSERT INTO Robbyson.dbo.Matriz_Geral ({colunas})
            VALUES ({placeholders})
        """

        with get_db_connection() as conn:
            cur = conn.cursor()
            erro = None
            try:
                cur.executemany(insert_query, all_rows)
                conn.commit()
                erro = 'True'
            except Exception as e:
                erro = e
                conn.rollback()
            finally:
                cur.close()
                return str(erro)

    return await loop.run_in_executor(None, _sync_db_call)

# async def batch_validar_submit_query(validation_conditions):
#     or_clauses = []
#     for cond in validation_conditions:
#         atributo = cond['atributo']
#         periodo = cond['periodo']
#         id_nome_indicador = cond['id_nome_indicador']
#         clause = f"""
#             (Atributo = '{atributo}' 
#             AND periodo = '{periodo}' 
#             AND id_nome_indicador = '{id_nome_indicador}')
#         """
#         or_clauses.append(clause)
        
#     if not or_clauses:
#         return []
#     full_where_clause = " OR ".join(or_clauses)
#     loop = asyncio.get_event_loop()
#     def _sync_db_call():
#         with get_db_connection() as conn:
#             cur = conn.cursor()
#             cur.execute(f"""
#                 select Atributo, periodo, id_nome_indicador, data_inicio, data_fim from Robbyson.dbo.Matriz_Geral (nolock)
#                 WHERE {full_where_clause}
#             """)
#             resultados_db = cur.fetchall()
#             cur.close()
#             return resultados_db        
#     return await loop.run_in_executor(None, _sync_db_call)

async def batch_validar_submit_query(validation_conditions):
    or_clauses = []
    all_data_for_query = []
    placeholder_clause = "(Atributo = ? AND periodo = ? AND id_nome_indicador = ?)"
    for cond in validation_conditions:
        or_clauses.append(placeholder_clause)
        all_data_for_query.extend([
            cond.get('atributo'), 
            cond.get('periodo'), 
            cond.get('id_nome_indicador')
        ])
    if not or_clauses:
        return []
    full_where_clause = " OR ".join(or_clauses)
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            validation_query = f"""
                SELECT Atributo, periodo, id_nome_indicador, data_inicio, data_fim 
                FROM Robbyson.dbo.Matriz_Geral (nolock)
                WHERE {full_where_clause}
            """
            cur.execute(validation_query, all_data_for_query)
            resultados_db = cur.fetchall()
            cur.close()
            return resultados_db
            
    return await loop.run_in_executor(None, _sync_db_call)

async def get_all_atributos():
    cache_key = "all_atributos"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select distinct atributo from [robbyson].[rlt].[hmn] (nolock) where (data = convert(date, getdate()-1)) and (atributo is not null) 
            """)
            resultados = [i[0] for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    set_cache(cache_key, resultados)
    return resultados

async def query_mes(atributo, username, page, area, mes):
    cache_key = f"pesquisa_{mes}:{atributo}:{page}"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    tipo_pesquisa_mg = None
    tipo_pesquisa_fef = None
    if mes == 'm0':
        tipo_pesquisa_mg = -1
        tipo_pesquisa_fef = -1
    elif mes == 'm+1':
        tipo_pesquisa_mg = 0
        tipo_pesquisa_fef = -1
    elif mes == 'm1':
        tipo_pesquisa_mg = -2
        tipo_pesquisa_fef = -2
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            if page == "demais":
                # if area is not None:
                #     cur.execute(f"""
                #     select * from Robbyson.dbo.Matriz_Geral (nolock)
                #     WHERE Atributo = ?
                #     and area = ?
                #     and tipo_matriz like 'OPERA%'
                #     AND periodo = dateadd(d,1,eomonth(GETDATE(),-1))
                #     AND ativo in (0, 1)
                #     """,(atributo,area,))
                cur.execute(f"""
                set nocount on
                select id_indicador, id_formato into #formatos from rby_indicador

                select fef.atributo, fef.id, fef.metasugerida, fef.resultado, fef.atingimento, f.id_formato 
                into #fef
                from Robbyson.dbo.factibilidadeEfaixas fef (nolock)
                left join #formatos f on f.id_indicador = fef.id
                where fef.data = DATEADD(DD, 1, EOMONTH(DATEADD(MM, ?, GETDATE())))

                select mg.atributo, id_nome_indicador, 
                case when fef.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.metasugerida AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') 
                when fef.id_formato = 3 then format(fef.metasugerida, 'P') else CAST(ROUND(fef.metasugerida, 2) AS NVARCHAR(MAX)) end as metasugerida, 
                case when fef.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.resultado AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') 
                when fef.id_formato = 3 then format(fef.resultado, 'P') else CAST(ROUND(fef.resultado, 2) AS NVARCHAR(MAX)) end as resultado, 
                format(fef.atingimento, 'P') as atingimento, mg.meta, moedas, tipo_indicador, 
                acumulado, esquema_acumulado, tipo_matriz, data_inicio, data_fim, periodo, escala, tipo_de_faturamento, descricao, ativo, 
                chamado, criterio, area, responsavel, gerente, possui_dmm, dmm, submetido_por, data_submetido_por, qualidade, da_qualidade, 
                data_da_qualidade, planejamento, da_planejamento, data_da_planejamento, exop, da_exop, data_da_exop
                from Robbyson.dbo.Matriz_Geral mg (nolock)
                left join #fef fef on fef.id = LTRIM(RTRIM(LEFT(id_nome_indicador, CHARINDEX('-', id_nome_indicador) - 1)))
                and fef.atributo = ?
                WHERE mg.atributo = ?
                and tipo_matriz like 'OPERA%'
                AND periodo = dateadd(d,1,eomonth(GETDATE(),?))
                AND ativo in (0, 1)
                order by moedas desc

                drop table #formatos
                drop table #fef
                """,(tipo_pesquisa_fef, atributo, atributo, tipo_pesquisa_mg))
            elif page == "cadastro":
                cur.execute(f"""
                set nocount on
                select id_indicador, id_formato into #formatos from rby_indicador

                select fef.atributo, fef.id, fef.metasugerida, fef.resultado, fef.atingimento, f.id_formato 
                into #fef
                from Robbyson.dbo.factibilidadeEfaixas fef (nolock)
                left join #formatos f on f.id_indicador = fef.id
                where fef.data = DATEADD(DD, 1, EOMONTH(DATEADD(MM, ?, GETDATE())))

                select mg.atributo, id_nome_indicador, 
                case when fef.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.metasugerida AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') 
                when fef.id_formato = 3 then format(fef.metasugerida, 'P') else CAST(ROUND(fef.metasugerida, 2) AS NVARCHAR(MAX)) end as metasugerida, 
                case when fef.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.resultado AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') 
                when fef.id_formato = 3 then format(fef.resultado, 'P') else CAST(ROUND(fef.resultado, 2) AS NVARCHAR(MAX)) end as resultado, 
                format(fef.atingimento, 'P') as atingimento, mg.meta, moedas, tipo_indicador, 
                acumulado, esquema_acumulado, tipo_matriz, data_inicio, data_fim, periodo, escala, tipo_de_faturamento, descricao, ativo, 
                chamado, criterio, area, responsavel, gerente, possui_dmm, dmm, submetido_por, data_submetido_por, qualidade, da_qualidade, 
                data_da_qualidade, planejamento, da_planejamento, data_da_planejamento, exop, da_exop, data_da_exop
                from Robbyson.dbo.Matriz_Geral mg (nolock)
                left join #fef fef on fef.id = LTRIM(RTRIM(LEFT(id_nome_indicador, CHARINDEX('-', id_nome_indicador) - 1)))
                and fef.atributo = ?
                WHERE mg.atributo = ?
                and tipo_matriz like 'ADMINISTRA%'
                AND periodo = dateadd(d,1,eomonth(GETDATE(),?))
                AND ativo in (0, 1)
                order by moedas desc

                drop table #formatos
                drop table #fef
                """,(tipo_pesquisa_fef, atributo, atributo, tipo_pesquisa_mg))
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    # registros = [{
    #     "atributo": row[0], "id_nome_indicador": row[1], "meta": row[2], "moedas": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
    #     "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_de_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
    #     "criterio": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possui_dmm": row[20], "dmm": row[21],
    #     "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
    #     "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], "id": str(uuid.uuid4())
    # } for row in resultados]
    registros = [{
        "atributo": row[0], "id_nome_indicador": row[1], "meta_sugerida": row[2], "resultado": row[3], "atingimento": row[4], "meta": row[5], "moedas": row[6], "tipo_indicador": row[7], "acumulado": row[8], "esquema_acumulado": row[9],
        "tipo_matriz": row[10], "data_inicio": row[11], "data_fim": row[12], "periodo": row[13], "escala": row[14], "tipo_de_faturamento": row[15], "descricao": row[16], "ativo": row[17], "chamado": row[18],
        "criterio": row[19], "area": row[20], "responsavel": row[21], "gerente": row[22], "possui_dmm": row[23], "dmm": row[24],
        "submetido_por": row[25], "data_submetido_por": row[26], "qualidade": row[27], "da_qualidade": row[28], "data_da_qualidade": row[29],
        "planejamento": row[30], "da_planejamento": row[31], "data_da_planejamento": row[32], "exop": row[33], "da_exop": row[34], "data_da_exop": row[35], "id": str(uuid.uuid4())
    } for row in resultados]
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, registros, CACHE_TTL)
    return registros

async def query_m0(atributo, username, page, area):
    cache_key = f"pesquisa_m0:{atributo}:{page}"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            if page == "demais":
                # if area is not None:
                #     cur.execute(f"""
                #     select * from Robbyson.dbo.Matriz_Geral (nolock)
                #     WHERE Atributo = ?
                #     and area = ?
                #     and tipo_matriz like 'OPERA%'
                #     AND periodo = dateadd(d,1,eomonth(GETDATE(),-1))
                #     AND ativo in (0, 1)
                #     """,(atributo,area,))
                cur.execute(f"""
                set nocount on
                select id_indicador, id_formato into #formatos from rby_indicador

                select fef.atributo, fef.id, fef.metasugerida, fef.resultado, fef.atingimento, f.id_formato 
                into #fef
                from Robbyson.dbo.factibilidadeEfaixas fef (nolock)
                left join #formatos f on f.id_indicador = fef.id
                where fef.data = DATEADD(DD, 1, EOMONTH(DATEADD(MM, -1, GETDATE())))

                select mg.atributo, id_nome_indicador, 
                case when fef.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.metasugerida AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') 
                when fef.id_formato = 3 then format(fef.metasugerida, 'P') else CAST(ROUND(fef.metasugerida, 2) AS NVARCHAR(MAX)) end as metasugerida, 
                case when fef.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.resultado AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') 
                when fef.id_formato = 3 then format(fef.resultado, 'P') else CAST(ROUND(fef.resultado, 2) AS NVARCHAR(MAX)) end as resultado, 
                format(fef.atingimento, 'P') as atingimento, mg.meta, moedas, tipo_indicador, 
                acumulado, esquema_acumulado, tipo_matriz, data_inicio, data_fim, periodo, escala, tipo_de_faturamento, descricao, ativo, 
                chamado, criterio, area, responsavel, gerente, possui_dmm, dmm, submetido_por, data_submetido_por, qualidade, da_qualidade, 
                data_da_qualidade, planejamento, da_planejamento, data_da_planejamento, exop, da_exop, data_da_exop
                from Robbyson.dbo.Matriz_Geral mg (nolock)
                left join #fef fef on fef.id = LTRIM(RTRIM(LEFT(id_nome_indicador, CHARINDEX('-', id_nome_indicador) - 1)))
                and fef.atributo = ?
                WHERE mg.atributo = ?
                and tipo_matriz like 'OPERA%'
                AND periodo = dateadd(d,1,eomonth(GETDATE(),-1))
                AND ativo in (0, 1)

                drop table #formatos
                drop table #fef
                """,(atributo, atributo))
            elif page == "cadastro":
                cur.execute(f"""
                set nocount on
                select id_indicador, id_formato into #formatos from rby_indicador

                select fef.atributo, fef.id, fef.metasugerida, fef.resultado, fef.atingimento, f.id_formato 
                into #fef
                from Robbyson.dbo.factibilidadeEfaixas fef (nolock)
                left join #formatos f on f.id_indicador = fef.id
                where fef.data = DATEADD(DD, 1, EOMONTH(DATEADD(MM, -1, GETDATE())))

                select mg.atributo, id_nome_indicador, 
                case when fef.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.metasugerida AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') 
                when fef.id_formato = 3 then format(fef.metasugerida, 'P') else CAST(ROUND(fef.metasugerida, 2) AS NVARCHAR(MAX)) end as metasugerida, 
                case when fef.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.resultado AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') 
                when fef.id_formato = 3 then format(fef.resultado, 'P') else CAST(ROUND(fef.resultado, 2) AS NVARCHAR(MAX)) end as resultado, 
                format(fef.atingimento, 'P') as atingimento, mg.meta, moedas, tipo_indicador, 
                acumulado, esquema_acumulado, tipo_matriz, data_inicio, data_fim, periodo, escala, tipo_de_faturamento, descricao, ativo, 
                chamado, criterio, area, responsavel, gerente, possui_dmm, dmm, submetido_por, data_submetido_por, qualidade, da_qualidade, 
                data_da_qualidade, planejamento, da_planejamento, data_da_planejamento, exop, da_exop, data_da_exop
                from Robbyson.dbo.Matriz_Geral mg (nolock)
                left join #fef fef on fef.id = LTRIM(RTRIM(LEFT(id_nome_indicador, CHARINDEX('-', id_nome_indicador) - 1)))
                and fef.atributo = ?
                WHERE mg.atributo = ?
                and tipo_matriz like 'ADMINISTRA%'
                AND periodo = dateadd(d,1,eomonth(GETDATE(),-1))
                AND ativo in (0, 1)

                drop table #formatos
                drop table #fef
                """,(atributo, atributo))
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    # registros = [{
    #     "atributo": row[0], "id_nome_indicador": row[1], "meta": row[2], "moedas": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
    #     "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_de_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
    #     "criterio": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possui_dmm": row[20], "dmm": row[21],
    #     "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
    #     "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], "id": str(uuid.uuid4())
    # } for row in resultados]
    registros = [{
        "atributo": row[0], "id_nome_indicador": row[1], "meta_sugerida": row[2], "resultado": row[3], "atingimento": row[4], "meta": row[5], "moedas": row[6], "tipo_indicador": row[7], "acumulado": row[8], "esquema_acumulado": row[9],
        "tipo_matriz": row[10], "data_inicio": row[11], "data_fim": row[12], "periodo": row[13], "escala": row[14], "tipo_de_faturamento": row[15], "descricao": row[16], "ativo": row[17], "chamado": row[18],
        "criterio": row[19], "area": row[20], "responsavel": row[21], "gerente": row[22], "possui_dmm": row[23], "dmm": row[24],
        "submetido_por": row[25], "data_submetido_por": row[26], "qualidade": row[27], "da_qualidade": row[28], "data_da_qualidade": row[29],
        "planejamento": row[30], "da_planejamento": row[31], "data_da_planejamento": row[32], "exop": row[33], "da_exop": row[34], "data_da_exop": row[35], "id": str(uuid.uuid4())
    } for row in resultados]
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, registros, CACHE_TTL)
    return registros

async def query_m1(atributo, username, page, area):
    cache_key = f"pesquisa_m1:{atributo}:{page}"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            if page == "demais":
                # if area is not None:
                #     cur.execute(f"""
                #     select * from Robbyson.dbo.Matriz_Geral (nolock)
                #     WHERE Atributo = ?
                #     and area = ?
                #     and tipo_matriz like 'OPERA%'
                #     AND periodo = dateadd(d,1,eomonth(GETDATE(),-2))
                #     AND ativo in (0, 1)
                #     """,(atributo,area,))
                cur.execute(f"""
                select * from Robbyson.dbo.Matriz_Geral (nolock)
                WHERE Atributo = ?
                and tipo_matriz like 'OPERA%'
                AND periodo = dateadd(d,1,eomonth(GETDATE(),-2))
                AND ativo in (0, 1)
                """,(atributo,))
            elif page == "cadastro":
                cur.execute(f"""
                select * from Robbyson.dbo.Matriz_Geral (nolock)
                WHERE Atributo = ?
                and tipo_matriz like 'ADMINISTRA%'
                AND periodo = dateadd(d,1,eomonth(GETDATE(),-2))
                AND ativo in (0, 1)
                """,(atributo,))
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{
        "atributo": row[0], "id_nome_indicador": row[1], "meta": row[2], "moedas": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
        "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_de_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
        "criterio": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possui_dmm": row[20], "dmm": row[21],
        "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
        "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], "id": str(uuid.uuid4())
    } for row in resultados]
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, registros, CACHE_TTL)
    return registros

async def query_m_mais1(atributo, username, page, area):
    cache_key = f"pesquisa_m+1:{atributo}:{page}"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            if page == "demais":
                # if area is not None:
                #     cur.execute(f"""
                #     select * from Robbyson.dbo.Matriz_Geral (nolock)
                #     WHERE Atributo = ?
                #     and area = ?
                #     and tipo_matriz like 'OPERA%'
                #     AND periodo = dateadd(d,1,eomonth(GETDATE()))
                #     AND ativo in (0, 1)
                #     """,(atributo,area,))
                cur.execute(f"""
                select * from Robbyson.dbo.Matriz_Geral (nolock)
                WHERE Atributo = ?
                and tipo_matriz like 'OPERA%'
                AND periodo = dateadd(d,1,eomonth(GETDATE()))
                AND ativo in (0, 1)
                """,(atributo,))
            elif page == "cadastro":
                cur.execute(f"""
                select * from Robbyson.dbo.Matriz_Geral (nolock)
                WHERE Atributo = ?
                and tipo_matriz like 'ADMINISTRA%'
                AND periodo = dateadd(d,1,eomonth(GETDATE()))
                AND ativo in (0, 1)
                """,(atributo,))
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{
        "atributo": row[0], "id_nome_indicador": row[1], "meta": row[2], "moedas": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
        "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_de_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
        "criterio": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possui_dmm": row[20], "dmm": row[21],
        "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
        "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], "id": str(uuid.uuid4())
    } for row in resultados]
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, registros, CACHE_TTL)
    return registros

async def update_da_adm_apoio(lista_de_updates: list, role, tipo, username): 
    role_defined = None
    tipo_defined = 1 if tipo == 'Acordo' else 2
    if role == "apoio qualidade":
        role_defined = "qualidade"
    elif role == "apoio planejamento":
        role_defined = "planejamento"
    elif role == "adm":
        role_defined = "exop"
    else:
        return None
    agora = datetime.now()
    campo_usuario = role_defined
    campo_da = "da_"+role_defined
    campo_data = "data_da_"+role_defined
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            for update_item in lista_de_updates:
                atributo, periodo, id_nome_indicador = update_item
                if role_defined == "exop":
                    cur.execute(f"""
                    UPDATE dbo.Matriz_Geral
                    SET 
                        ativo = 1,
                        {campo_usuario} = ?,
                        {campo_da} = ?,
                        {campo_data} = ?
                    WHERE 
                        Atributo = ? AND 
                        periodo = ? AND 
                        id_nome_indicador = ?
                """, (username, tipo_defined, agora, atributo, periodo, id_nome_indicador))
                else:
                    cur.execute(f"""
                    UPDATE dbo.Matriz_Geral
                    SET 
                        {campo_usuario} = ?,
                        {campo_da} = ?,
                        {campo_data} = ?
                    WHERE 
                        Atributo = ? AND 
                        periodo = ? AND 
                        id_nome_indicador = ?
                """, (username, tipo_defined, agora, atributo, periodo, id_nome_indicador))
            conn.commit() 
            cur.close()
    await loop.run_in_executor(None, _sync_db_call)

async def update_dmm_bd(atributo, periodo, dmm): 
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
            UPDATE dbo.Matriz_Geral
            SET 
                dmm = ?
            WHERE 
                Atributo = ? AND 
                periodo = ?
            """, (str(dmm), atributo, periodo,))
            conn.commit() 
            cur.close()
    await loop.run_in_executor(None, _sync_db_call)


# async def update_da_adm_10(updates: list, tipo, username): 
#     agora = datetime.now()
#     tipo_defined = 1 if tipo == 'Acordo' else 2
#     loop = asyncio.get_event_loop()
#     def _sync_db_call():
#         with get_db_connection() as conn:
#             cur = conn.cursor()
#             try:
#                 for update_item in updates:
#                     atributo, periodo, id_nome_indicador = update_item.get('atributo'), update_item.get('periodo'), update_item.get('id_nome_indicador')
#                     if tipo_defined == 1:
#                         cur.execute(f"""
#                         UPDATE dbo.Matriz_Geral
#                         SET 
#                             ativo = 0
#                         WHERE 
#                             Atributo = ? AND 
#                             periodo = ? AND 
#                             id_nome_indicador = ? AND
#                             ativo = 1
#                     """, (atributo, periodo, id_nome_indicador))
#                         cur.execute(f"""
#                         UPDATE dbo.Matriz_Geral
#                         SET 
#                             ativo = 1,
#                             da_superintendente = 1,
#                             exop = ? ,
#                             data_da_exop = ? 
#                         WHERE 
#                             Atributo = ? AND 
#                             periodo = ? AND 
#                             id_nome_indicador = ? AND
#                             ativo = 10
#                     """, (username, agora, atributo, periodo, id_nome_indicador))
#                     else:
#                         cur.execute(f"""
#                         UPDATE dbo.Matriz_Geral
#                         SET 
#                             da_superintendente = 2
#                         WHERE 
#                             Atributo = ? AND 
#                             periodo = ? AND 
#                             id_nome_indicador = ? AND
#                             ativo = 11
#                     """, (atributo, periodo, id_nome_indicador))
#                 conn.commit() 
#             finally:
#                 cur.close()
#     await loop.run_in_executor(None, _sync_db_call)

async def insert_log_meta_moedas(registros_selecionados, meta, username):
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            for registro in registros_selecionados:
                cur.execute(f"""
                INSERT INTO dbo.log_alt_apoio_matriz_geral (atributo, id_nome_indicador, meta_antiga, nova_meta, alterado_por, resultado_m0, gerente, periodo)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (registro["atributo"], registro["id_nome_indicador"], registro["meta"], meta, username, registro.get("resultado_m0"), registro.get("submetido_por"), registro["periodo"]))
            conn.commit() 
            cur.close()
    await loop.run_in_executor(None, _sync_db_call)

async def update_meta_moedas_bd(lista_de_updates: list, meta, moedas, role, username): 
    agora = datetime.now()
    role_defined = None
    if role == "apoio qualidade":
        role_defined = "qualidade"
    elif role == "apoio planejamento":
        role_defined = "planejamento"
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            for update_item in lista_de_updates:
                atributo, periodo, id_nome_indicador = update_item
                if moedas != '':
                    cur.execute(f"""
                    UPDATE dbo.Matriz_Geral
                    SET 
                        meta = ?,
                        moedas = ?
                    WHERE 
                        Atributo = ? AND 
                        periodo = ? AND 
                        id_nome_indicador = ?
                """, (meta, moedas, atributo, periodo, id_nome_indicador))
                else:
                    if role_defined == "qualidade":
                        cur.execute(f"""
                        UPDATE dbo.Matriz_Geral
                        SET 
                            meta = ?,
                            qualidade = ?,
                            da_qualidade = 1,
                            data_da_qualidade = ?
                        WHERE 
                            Atributo = ? AND 
                            periodo = ? AND 
                            id_nome_indicador = ?
                    """, (meta, username, agora, atributo, periodo, id_nome_indicador))
                    elif role_defined == "planejamento":
                        cur.execute(f"""
                        UPDATE dbo.Matriz_Geral
                        SET 
                            meta = ?,
                            planejamento = ?,
                            da_planejamento = 1,
                            data_da_planejamento = ?
                        WHERE 
                            Atributo = ? AND 
                            periodo = ? AND 
                            id_nome_indicador = ?
                    """, (meta, username, agora, atributo, periodo, id_nome_indicador))
                    cur.execute(f"""
                    UPDATE dbo.Matriz_Geral
                    SET 
                        meta = ?
                    WHERE 
                        Atributo = ? AND 
                        periodo = ? AND 
                        id_nome_indicador = ?
                    """, (meta, atributo, periodo, id_nome_indicador))
            conn.commit() 
            cur.close()
    await loop.run_in_executor(None, _sync_db_call)


def validar_datas(data_inicio_bd, data_fim_bd, data_inicio_sbmit, data_fim_submit):
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

async def get_matrizes_alteradas_apoio(username):
    cache_key = f"matrizes_alteradas_apoio:{username}"
    cached = get_from_cache(cache_key)
    if cached:
        return cached

    loop = asyncio.get_event_loop()

    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT atributo,
                       id_nome_indicador,
                       meta_antiga,
                       nova_meta,
                       alterado_por,
                       resultado_m0,
                       periodo
                FROM robbyson.dbo.log_alt_apoio_matriz_geral (NOLOCK)
                WHERE gerente = ?
                and periodo >= dateadd(d,1,eomonth(GETDATE(),-1))
            """, (username,))

            rows = cur.fetchall()
            cur.close()

            return [
                {
                    "atributo": r[0],
                    "id_nome_indicador": r[1],
                    "meta_antiga": r[2],
                    "meta_nova": r[3],
                    "alterado_por": r[4],
                    "resultado_m0": r[5],
                    "periodo": r[6],
                }
                for r in rows
            ]

    resultados = await loop.run_in_executor(None, _sync_db_call)
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, resultados, ttl=CACHE_TTL)

    return resultados


async def get_resultados_indicadores_m3():
    cache_key = "resultados_indicadores_m3"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT [IDINDICADOR]
                FROM [Robbyson].[ext].[indicadoresgeral]
                where data >= dateadd(d,1,eomonth(GETDATE(),-3))
                and resultado > 0
            """)
            resultados = [i[0] for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    CACHE_TTL = timedelta(hours=24)
    set_cache(cache_key, resultados, ttl=CACHE_TTL)
    return resultados

async def get_matriculas_cadastro_adm():
    cache_key = "matriculas_cadastro_adm"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
               select * from dbo.listagem_matriz_administrativa
            """)
            resultados = {f'{i[0]}': i[1] for i in cur.fetchall()}
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    CACHE_TTL_24H = timedelta(hours=24)
    set_cache(cache_key, resultados, ttl=CACHE_TTL_24H)
    return resultados

async def get_fact_m0(atributo, id):
    cache_key = f"fact_m0:{atributo}:{id}"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                set nocount on
                select id_indicador, id_formato into #formatos from rby_indicador

                select concat(fef.id, ' - ', fef.nome_indicador) as id_nome_indicador, 
                case when f.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.metasugerida AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss')
                when f.id_formato = 3 then format(fef.metasugerida, 'P') else CAST(ROUND(fef.metasugerida, 2) AS NVARCHAR(MAX)) end as metasugerida, 
                case when f.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.resultado AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss')
                when f.id_formato = 3 then format(fef.resultado, 'P') else CAST(ROUND(fef.resultado, 2) AS NVARCHAR(MAX)) end as resultado, 
                format(fef.atingimento, 'P') as atingimento
                from Robbyson.dbo.factibilidadeEfaixas fef (nolock)
                left join #formatos f on fef.id = f.id_indicador
                where atributo = ?
                and id = ?
                and data = dateadd(d,1,eomonth(GETDATE(),-1))

                drop table #formatos
            """, (atributo,id,))
            resultados = [{"id_nome_indicador": i[0], "metasugerida": i[1], "resultado": i[2], "atingimento": i[3] } for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    set_cache(cache_key, resultados)
    return resultados

async def get_indicadores():
    cache_key = "indicadores"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT id_indicador, indicador, 
                case when indicador like '%Semanal%' then 'Sim' 
                when indicador like '%Mensal%' then 'Não' else 'Não' end as Acumulado,
                case when indicador like '%Semanal%' then 'Semanal'
                when indicador like '%Mensal%' then 'Mensal' else 'Diario' end as Esquema_acumulado,
                case when id_formato = 1 then 'Inteiro'
                when id_formato = 2 then 'Decimal'
                when id_formato = 3 then 'Percentual' 
                when id_formato = 4 then 'Hora' else '' end as Formato
                FROM rby_indicador (nolock)
                WHERE indicador <> 'Descontinuado' 
                AND indicador <> 'INdicador Disponivel'
            """)
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    indicadores = [{"id": str(i[0]), "text": i[1], "acumulado": i[2], "esquema_acumulado": i[3], "formato": str(i[4])} for i in resultados]
    set_cache(cache_key, indicadores)
    return indicadores

async def get_atributos_adm():
    cache_key = "atributos_adm"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
            set nocount on
            select distinct atributo, count(matricula) as atendentes
            into #base_hmn
            from rlt.hmn (nolock) 
            where data = convert(date, getdate()-1)
            and atributo is not null
            and ((TipoHierarquia = 'OPERAÇÃO' and NivelHierarquico = 'OPERACIONAL') or (tipohierarquia = 'ADMINISTRAÇÃO' and nivelhierarquico = 'OPERACIONAL'))
            group by atributo, TipoHierarquia

            select distinct atributo, 
            case when GERENTE is not null then GERENTE
            when GERENTEPLENO is not null then GERENTEPLENO
            when GERENTESENIOR is not null then GERENTESENIOR
            else GERENTE_EXECUTIVO end as Gerente,
            TipoHierarquia
            into #hmn_geral
            from rlt.hmn (nolock) 
            where data = convert(date, getdate()-1)
            and atributo in (select atributo from #base_hmn)

            select atributo, Gerente, TipoHierarquia
            from #hmn_geral hmn
            where gerente is not null

            drop table #base_hmn
            drop table #hmn_geral
            """)
            resultados = [{"atributo": i[0], "gerente": i[1], "tipo": i[2]} for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, resultados, CACHE_TTL)
    return resultados

async def get_atributos_apoio(area):
    cache_key = "atributos_apoio"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            if area == "Qualidade":
                cur.execute(f"""
                select distinct atributo, gerente, tipo_matriz from Robbyson.dbo.Matriz_Geral (nolock) where (gerente <> '' and tipo_matriz like 'OPERA%') and da_qualidade = 0 and periodo >= dateadd(d,1,eomonth(GETDATE(),-1))
                """)
            elif area == "Planejamento":
                cur.execute(f"""
                select distinct atributo, gerente, tipo_matriz from Robbyson.dbo.Matriz_Geral (nolock) where (gerente <> '' and tipo_matriz like 'OPERA%') and da_planejamento = 0 and periodo >= dateadd(d,1,eomonth(GETDATE(),-1))
                """)
            resultados = [{"atributo": i[0], "gerente": i[1], "tipo": i[2]} for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, resultados, CACHE_TTL)
    return resultados


async def get_nao_acordos_apoio():
    cache_key = f"nao_acordos_apoio"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select * from matriz_geral (nolock) where da_qualidade = 2 or da_planejamento = 2
            """)
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{
        "atributo": row[0], "id_nome_indicador": row[1], "meta": row[2], "moedas": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
        "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_de_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
        "criterio": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possui_dmm": row[20], "dmm": row[21],
        "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
        "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], "id": str(uuid.uuid4())
    } for row in resultados]
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, registros, CACHE_TTL)
    return registros

async def get_nao_acordos_exop():
    cache_key = f"nao_acordos_exop"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select * from matriz_geral (nolock) where da_exop = 2 and ativo = 0
            """)
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{
        "atributo": row[0], "id_nome_indicador": row[1], "meta": row[2], "moedas": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
        "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_de_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
        "criterio": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possui_dmm": row[20], "dmm": row[21],
        "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
        "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], "id": str(uuid.uuid4())
    } for row in resultados]
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, registros, CACHE_TTL)
    return registros

async def get_matrizes_nao_cadastradas():
    cache_key = f"matrizes_nao_cadastradas"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                set nocount on
                select distinct atributo, count(matricula) as atendentes
                into #atrb_valid_hmn
                from rlt.hmn (nolock) 
                where data = convert(date, getdate()-1)
                and tipohierarquia = 'operação' and nivelhierarquico = 'operacional'
                and funcaorm not like '%analista%'
                and situacaohominum in ('ativo', 'treinamento')
                group by atributo, funcaorm

                select distinct atributo 
                into #in_mg_not_in_hmn 
                from robbyson.dbo.matriz_geral (nolock)
                where periodo = '2025-11-01'
                except
                select atributo 
                from #atrb_valid_hmn
                where atendentes > 0

                select atributo 
                from #atrb_valid_hmn
                where atendentes > 0
                and atributo is not null
                except
                select distinct atributo
                from robbyson.dbo.matriz_geral mg (nolock)
                where mg.atributo not in (select inmg.atributo from #in_mg_not_in_hmn inmg)
                and mg.periodo = '2025-11-01'

                drop table #atrb_valid_hmn
                drop table #in_mg_not_in_hmn
            """)
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [row[0] for row in resultados]
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, registros, CACHE_TTL)
    return registros

async def get_acordos_apoio():
    cache_key = f"acordos_apoio"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select * from matriz_geral (nolock) where ((area = 'Qualidade' and da_qualidade = 1) or (area = 'Planejamento' and da_planejamento = 1)) and (da_exop = 0)
            """)
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{
        "atributo": row[0], "id_nome_indicador": row[1], "meta": row[2], "moedas": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
        "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_de_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
        "criterio": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possui_dmm": row[20], "dmm": row[21],
        "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
        "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], "id": str(uuid.uuid4())
    } for row in resultados]
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, registros, CACHE_TTL)
    return registros

async def get_atributos_cadastro_apoio(produto):
    cache_key = f"atributos_cadastro_apoio_{produto}"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
            select distinct atributo, case when GERENTE is not null then GERENTE
            when GERENTEPLENO is not null then GERENTEPLENO
            when GERENTESENIOR is not null then GERENTESENIOR
            else GERENTE_EXECUTIVO end as Gerente from [robbyson].[rlt].[hmn] (nolock) where data = convert(date, getdate()-1) 
            and atributo like '%{produto}%'
            and tipohierarquia = 'ADMINISTRAÇÃO' and nivelhierarquico = 'OPERACIONAL'
            and SituacaoHominum in ('ativo', 'treinamento')
            """)
            resultados = [{"atributo": i[0], "gerente": i[1], "tipo": "ADMINISTRAÇÃO"} for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, resultados, CACHE_TTL)
    return resultados

async def get_all_atributos_cadastro_apoio(area):
    cache_key = f"all_atributos_cadastro_apoio:{area}"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
            set nocount on

            select distinct atributo, case when GERENTE is not null then GERENTE
            when GERENTEPLENO is not null then GERENTEPLENO
            when GERENTESENIOR is not null then GERENTESENIOR
            else GERENTE_EXECUTIVO end as Gerente 
            into #at from [robbyson].[rlt].[hmn] (nolock) where data = convert(date, getdate()-1) 
            and tipohierarquia = 'ADMINISTRAÇÃO' and nivelhierarquico = 'OPERACIONAL'
            and SituacaoHominum in ('ativo', 'treinamento')
            and atributo is not null
            and atributo like '%{area}%'

            select * from #at where Gerente is NOT NULL

            drop table #at
            """)
            resultados = [{"atributo": i[0], "gerente": i[1], "tipo": "ADMINISTRAÇÃO"} for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, resultados, CACHE_TTL)
    return resultados

async def get_num_atendentes(atributo):
    cache_key = f"num_atendentes:{atributo}"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
            select count(matricula) as matriculas from [robbyson].[rlt].[hmn] (nolock) where data = convert(date, getdate()-1) 
            and atributo like '%{atributo}%'
            and tipohierarquia = 'OPERAÇÃO' and nivelhierarquico = 'OPERACIONAL'
            and SituacaoHominum in ('ativo', 'treinamento')
            """)
            resultados = [i[0] for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    set_cache(cache_key, resultados[0])
    return resultados[0]

async def get_atributos_matricula(matricula):
    cache_key = f"atributos_matricula:{matricula}"
    cached = get_from_cache(cache_key)
    resultados = None
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
            SET NOCOUNT ON

            select atributo, count(matricula) as matriculas into #qtd from [robbyson].[rlt].[hmn] (nolock) 
                        where data = convert(date, getdate()-1) 
                        and tipohierarquia = 'OPERAÇÃO' and nivelhierarquico = 'OPERACIONAL'
                        and SituacaoHominum in ('ativo', 'treinamento')
                        group by atributo
                        order by count(matricula) DESC

            select distinct hmn.atributo, case when GERENTE is not null then GERENTE
                        when GERENTEPLENO is not null then GERENTEPLENO
                        when GERENTESENIOR is not null then GERENTESENIOR
                        else null end as Gerente, TipoHierarquia from [robbyson].[rlt].[hmn] hmn (nolock) 
                        where (data = convert(date, getdate()-1)) and (hmn.atributo is not null) 
                        and (MatrGERENTE = {matricula} or MatrGERENTEPLENO = {matricula} or MatrGERENTESENIOR = {matricula} or MatrCOORDENADOR = {matricula})
                        and hmn.atributo in (select atributo from #qtd)

            drop table #qtd
            """)
            resultados = [{"atributo": i[0], "gerente": i[1], "tipo": i[2]} for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    set_cache(cache_key, resultados)
    return resultados

async def get_funcao(matricula):
    cache_key = f"funcao:{matricula}"
    cached = get_from_cache(cache_key)
    resultados = None
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select distinct funcaorm 
                from [robbyson].[rlt].[hmn] (nolock) 
                where data = convert(date, getdate()-1) 
                and matricula = '{matricula}'
            """)
            resultados = [i[0] for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    if resultados:
        set_cache(cache_key, resultados[0])
        return resultados[0]
    return None

async def get_atributos_gerente(tipo, atributos, username):
    cache_key = f"all_atributos:{tipo}:{username}"
    cached = get_from_cache(cache_key)
    resultados = None
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            if tipo == "m0_all":
                cur.execute(f"""
                    select * from matriz_geral (nolock) where atributo in ({atributos}) and periodo = dateadd(d,1,eomonth(GETDATE(),-1)) and tipo_matriz like 'OPERA%' order by atributo
                """)
            elif tipo == "m+1_all":
                cur.execute(f"""
                    select * from matriz_geral (nolock) where atributo in ({atributos}) and periodo = dateadd(d,1,eomonth(GETDATE())) and tipo_matriz like 'OPERA%' order by atributo
                """)
            else:
                cur.execute(f"""
                    select * from matriz_geral (nolock) where atributo in ({atributos}) and periodo = dateadd(d,1,eomonth(GETDATE(),-2)) and tipo_matriz like 'OPERA%' order by atributo
                """)
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{
        "atributo": row[0], "id_nome_indicador": row[1], "meta": row[2], "moedas": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
        "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_de_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
        "criterio": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possui_dmm": row[20], "dmm": row[21],
        "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
        "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], "id": str(uuid.uuid4())
    } for row in resultados]
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, registros, CACHE_TTL)
    return registros

async def get_matrizes_administrativas_pg_adm(tipo):
    cache_key = f"matrizes_administrativas_pg_adm:{tipo}"
    cached = get_from_cache(cache_key)
    resultados = None
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            if tipo == "m0_administrativas":
                cur.execute(f"""
                    select * from matriz_geral (nolock) where tipo_matriz like 'ADMINISTRA%' and periodo = dateadd(d,1,eomonth(GETDATE(),-1)) and ativo = 0
                """)
            elif tipo == "m+1_administrativas":
                cur.execute(f"""
                    select * from matriz_geral (nolock) where tipo_matriz like 'ADMINISTRA%' and periodo = dateadd(d,1,eomonth(GETDATE())) and ativo = 0
                """)
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{
        "atributo": row[0], "id_nome_indicador": row[1], "meta": row[2], "moedas": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
        "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_de_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
        "criterio": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possui_dmm": row[20], "dmm": row[21],
        "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
        "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], "id": str(uuid.uuid4())
    } for row in resultados]
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, registros, CACHE_TTL)
    return registros


async def get_matrizes_administrativas(tipo, atributos, username):
    cache_key = f"matrizes_administrativas:{tipo}:{username}"
    cached = get_from_cache(cache_key)
    resultados = None
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            if tipo == "m0_all_apoio":
                cur.execute(f"""
                    select * from matriz_geral (nolock) where atributo in ({atributos}) and periodo = dateadd(d,1,eomonth(GETDATE(),-1))
                """)
            elif tipo == "m+1_all_apoio":
                cur.execute(f"""
                    select * from matriz_geral (nolock) where atributo in ({atributos}) and periodo = dateadd(d,1,eomonth(GETDATE()))
                """)
            else:
                cur.execute(f"""
                    select * from matriz_geral (nolock) where atributo in ({atributos}) and periodo = dateadd(d,1,eomonth(GETDATE(),-2))
                """)
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{
        "atributo": row[0], "id_nome_indicador": row[1], "meta": row[2], "moedas": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
        "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_de_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
        "criterio": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possui_dmm": row[20], "dmm": row[21],
        "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
        "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], "id": str(uuid.uuid4())
    } for row in resultados]
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, registros, CACHE_TTL)
    return registros

async def get_matrizes_ativo_10():
    cache_key = "matrizes_ativo_10"
    cached = get_from_cache(cache_key)
    resultados = None
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select * from matriz_geral (nolock) where ativo = 10 and periodo >= dateadd(d,1,eomonth(GETDATE(),-1))
            """)
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{
        "atributo": row[0], "id_nome_indicador": row[1], "meta": row[2], "moedas": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
        "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_de_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
        "criterio": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possui_dmm": row[20], "dmm": row[21],
        "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
        "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], 
        "justificativa": row[33], "da_superintendente": row[34], "id": str(uuid.uuid4())
    } for row in resultados]
    CACHE_TTL = timedelta(minutes=1)
    set_cache(cache_key, registros, CACHE_TTL)
    return registros

async def get_nome(matricula):
    cache_key = f"nome:{matricula}"
    cached = get_from_cache(cache_key)
    resultados = None
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select nome from rlt.hmn (nolock) where matricula = {matricula} and data = CONVERT(date, getdate()-1)
            """)
            resultados = [i[0] for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    if resultados:
        set_cache(cache_key, resultados[0])
        return resultados[0]
    return None


async def get_resultados(atributo, id_indicador):
    cache_key = f"resultados:{atributo+id_indicador}"
    cached = get_from_cache(cache_key)
    resultados = None
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
            SET NOCOUNT ON

            select id_indicador, id_formato into #formatos from rby_indicador

            select data, atributo, id, nome_indicador, 
            case when #formatos.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(resultado AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss')else CAST(resultado AS NVARCHAR(MAX)) end as resultados,
            format(atingimento, 'P') as atingimentos, 
            case when metasugerida is null then null
            when #formatos.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(metasugerida AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') else CAST(metasugerida AS NVARCHAR(MAX)) end as metasugerida, 
            CASE 
                WHEN factibilidade = 'Meta AeC' THEN 'Meta AeC' 
                ELSE Meta_Escolhida 
            END AS Meta_Escolhida, 
            format(atingimento_projetado, 'P') as atingimento_projetado, 
            case when #formatos.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(meta AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') else CAST(meta AS NVARCHAR(MAX)) end as meta,
            data_atualizacao as MaxData, #formatos.id_formato from [Robbyson].[dbo].[factibilidadeEfaixas] fef
            left join #formatos on #formatos.id_indicador = fef.id
            where data = DATEADD(DD, 1, EOMONTH(DATEADD(MM, -2, GETDATE())))
            and id = {id_indicador} and atributo like '%{atributo}%'

            union ALL

            select data, atributo, id, nome_indicador, 
            case when #formatos.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(resultado AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss')else CAST(resultado AS NVARCHAR(MAX)) end as resultados,
            format(atingimento, 'P') as atingimentos, 
            case when metasugerida is null then null
            when #formatos.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(metasugerida AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') else CAST(metasugerida AS NVARCHAR(MAX)) end as metasugerida, 
            CASE 
                WHEN factibilidade = 'Meta AeC' THEN 'Meta AeC' 
                ELSE Meta_Escolhida 
            END AS Meta_Escolhida, 
            format(atingimento_projetado, 'P') as atingimento_projetado, 
            case when #formatos.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(meta AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') else CAST(meta AS NVARCHAR(MAX)) end as meta,
            data_atualizacao as MaxData, #formatos.id_formato from [Robbyson].[dbo].[factibilidadeEfaixas] fef
            left join #formatos on #formatos.id_indicador = fef.id
            where data = DATEADD(DD, 1, EOMONTH(DATEADD(MM, -1, GETDATE())))
            and id = {id_indicador} and atributo like '%{atributo}%'

            drop table #formatos

            """)
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    set_cache(cache_key, resultados)
    return resultados