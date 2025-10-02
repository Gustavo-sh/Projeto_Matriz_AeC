from datetime import datetime
import uuid
import pyodbc
from app.cache import get_from_cache, set_cache
import re

# Parte de Registro e Usuarios
def get_user_bd(username):
    cache_key = "user: " + username
    cached = get_from_cache(cache_key)
    if cached:
        return cached

    conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
    cur = conn.cursor()
    cur.execute(f"""
        select * from Robbyson.dbo.Acessos_Matriz (nolock) where username = '{username}' 
    """)
    resultados = [{"username": i[0], "password": i[1], "role": i[2]} for i in cur.fetchall()]
    cur.close()
    conn.close()

    if len(resultados) == 1:
        return resultados[0]
    set_cache(cache_key, None)
    return None

def save_user_bd(username, hashed_password, role):
    conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
    cur = conn.cursor()
    cur.execute(f"""
        insert into Robbyson.dbo.Acessos_Matriz (username, password, role) values ('{username}', '{hashed_password}', '{role}')
    """)
    conn.commit()
    cur.close()
    conn.close()

def save_registros_bd(registros, username):
    data = datetime.now()
    conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
    cur = conn.cursor()
    for i in registros:
        cur.execute(f"""
            insert into Robbyson.dbo.Matriz_Geral values ('{i["atributo"]}', '{i["nome"]}', '{i["meta"]}', '{i["moeda"]}', '{i["tipo_indicador"]}', '{i["acumulado"]}', '{i["esquema_acumulado"]}',
            '{i["tipo_matriz"]}', '{i["data_inicio"]}', '{i["data_fim"]}', '{i["periodo"]}', '{i["escala"]}', '{i["tipo_faturamento"]}', '{i["descricao"]}', '{i["ativo"]}', '{i["chamado"]}',
            '{i["criterio_final"]}', '{i["area"]}', '{i["responsavel"]}', '{i["gerente"]}', '{i["possuiDmm"]}', '{i["dmm"]}', '{username}', '{data}', '', '', '', '', '', '', '', '', '')
        """)
    conn.commit()
    cur.close()
    conn.close()

# Parte de consultas gerais

def query_m0(atributo):
    cache_key = f"pesquisa_m0:{atributo}"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
    cur = conn.cursor()
    cur.execute(f"""
        select * from Robbyson.dbo.Matriz_Geral (nolock)
        WHERE Atributo = '{atributo}'
        AND periodo = dateadd(d,1,eomonth(GETDATE(),-1))
    """)

    resultados = cur.fetchall()
    cur.close()
    conn.close()

    registros = [{
        "atributo": row[0], "nome": row[1], "meta": row[2], "moeda": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
        "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
        "criterio_final": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possuiDmm": row[20], "dmm": row[21],
        "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
        "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], "id": str(uuid.uuid4())
    } for row in resultados]
    set_cache(cache_key, registros)
    return registros

def query_m1(atributo):
    cache_key = f"pesquisa_m1:{atributo}"
    cached = get_from_cache(cache_key)
    if cached:
        return cached
    
    conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
    cur = conn.cursor()
    cur.execute(f"""
        select * from Robbyson.dbo.Matriz_Geral (nolock)
        WHERE Atributo = '{atributo}'
        AND periodo = dateadd(d,1,eomonth(GETDATE(),-2))
    """)

    resultados = cur.fetchall()
    cur.close()
    conn.close()

    registros = [{
        "atributo": row[0], "nome": row[1], "meta": row[2], "moeda": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
        "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
        "criterio_final": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possuiDmm": row[20], "dmm": row[21],
        "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
        "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], "id": str(uuid.uuid4())
    } for row in resultados]
    set_cache(cache_key, registros)
    return registros

def query_m1_adm_apoio(atributo):
    cache_key = f"pesquisa_m1_adm_apoio:{atributo}"
    # cached = get_from_cache(cache_key)
    # if cached:
    #     return cached
    
    conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
    cur = conn.cursor()
    cur.execute(f"""
        select * from Robbyson.dbo.Matriz_Geral (nolock)
        WHERE Atributo = '{atributo}'
        AND periodo = dateadd(d,1,eomonth(GETDATE()))
    """)

    resultados = cur.fetchall()
    cur.close()
    conn.close()

    registros = [{
        "atributo": row[0], "nome": row[1], "meta": row[2], "moeda": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
        "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
        "criterio_final": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possuiDmm": row[20], "dmm": row[21],
        "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
        "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], "id": str(uuid.uuid4())
    } for row in resultados]
    set_cache(cache_key, registros)
    return registros

def query_m0_adm_apoio(atributo):
    cache_key = f"pesquisa_m0_adm_apoio:{atributo}"
    # cached = get_from_cache(cache_key)
    # if cached:
    #     return cached
    
    conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
    cur = conn.cursor()
    cur.execute(f"""
        select * from Robbyson.dbo.Matriz_Geral (nolock)
        WHERE Atributo = '{atributo}'
        AND periodo = dateadd(d,1,eomonth(GETDATE(),-1))
    """)

    resultados = cur.fetchall()
    cur.close()
    conn.close()

    registros = [{
        "atributo": row[0], "nome": row[1], "meta": row[2], "moeda": row[3], "tipo_indicador": row[4], "acumulado": row[5], "esquema_acumulado": row[6],
        "tipo_matriz": row[7], "data_inicio": row[8], "data_fim": row[9], "periodo": row[10], "escala": row[11], "tipo_faturamento": row[12], "descricao": row[13], "ativo": row[14], "chamado": row[15],
        "criterio_final": row[16], "area": row[17], "responsavel": row[18], "gerente": row[19], "possuiDmm": row[20], "dmm": row[21],
        "submetido_por": row[22], "data_submetido_por": row[23], "qualidade": row[24], "da_qualidade": row[25], "data_da_qualidade": row[26],
        "planejamento": row[27], "da_planejamento": row[28], "data_da_planejamento": row[29], "exop": row[30], "da_exop": row[31], "data_da_exop": row[32], "id": str(uuid.uuid4())
    } for row in resultados]
    set_cache(cache_key, registros)
    return registros

def update_da_adm_apoio(atributo, periodo, id_nome_indicador, role, tipo, username):
    role_defined = None
    tipo_defined = None
    if role == "apoio_qualidade":
        role_defined = "qualidade"
    elif role == "apoio_planejamento":
        role_defined = "planejamento"
    elif role == "adm":
        role_defined = "exop"
    else:
        return None
    if tipo == 'Acordo':
        tipo_defined = 1
    elif tipo == 'Nao Acordo':
        tipo_defined = 2
    agora = datetime.now()
    conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
    cur = conn.cursor()

    cur.execute(f"""
    update dbo.Matriz_Geral set {role_defined} = '{username}', {"da_"+role_defined} = '{tipo_defined}', {"data_da_"+role_defined} = '{agora}'
    WHERE Atributo = '{atributo}'
    AND periodo = '{periodo}'
    AND id_nome_indicador = '{id_nome_indicador}'
""")

    conn.commit()
    cur.close()
    conn.close()


def validar_submit(atributo, periodo, id_nome_indicador):
    conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
    cur = conn.cursor()
    cur.execute(f"""
        select * from Robbyson.dbo.Matriz_Geral (nolock)
        WHERE Atributo = '{atributo}'
        AND periodo = '{periodo}'
        AND id_nome_indicador = '{id_nome_indicador}'
    """)

    resultados = cur.fetchall()
    cur.close()
    conn.close()

    if len(resultados) > 0:
        return True
    return False

def get_indicadores():
    cache_key = "indicadores"
    cached = get_from_cache(cache_key)
    if cached:
        return cached

    conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
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
    conn.close()

    indicadores = [{"id": str(i[0]), "text": i[1], "acumulado": i[2], "esquema_acumulado": i[3], "formato": str(i[4])} for i in resultados]
    set_cache(cache_key, indicadores)
    return indicadores

# def get_operacao(matricula):
#     cache_key = f"operacao:{matricula}"
#     cached = get_from_cache(cache_key)
#     if cached:
#         return cached

#     pattern = re.compile(r"( CPG| SP| MSS| JPA| MOC| ARP| BH| RJ| JN| ORION| PLAN| PLANEJAMENTO| QUALIDADE)")
#     conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
#     cur = conn.cursor()
#     cur.execute(f"""
#         select descricao_cr_funcionariorm from [robbyson].[rlt].[hmn] (nolock) 
#         where data = convert(date, getdate()-1) and matricula = {matricula}
#     """)
#     resultados = [pattern.sub("", i[0]) for i in cur.fetchall()]
#     cur.close()
#     conn.close()

#     set_cache(cache_key, resultados)
#     return resultados

# def get_operacoes_adm(matricula='adm'):
#     cache_key = f"operacao:{matricula}"
#     cached = get_from_cache(cache_key)
#     if cached:
#         return cached

#     pattern = re.compile(r"( CPG| SP| MSS| JPA| MOC| ARP| BH| RJ| JN| ORION| PLAN| PLANEJAMENTO| QUALIDADE)")
#     conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
#     cur = conn.cursor()
#     cur.execute(f"""
#         select distinct descricao_cr_funcionariorm from [robbyson].[rlt].[hmn] (nolock) 
#         where data = convert(date, getdate()-1)
#     """)
#     resultados = [pattern.sub("", i[0]) for i in cur.fetchall()]
#     cur.close()
#     conn.close()

#     set_cache(cache_key, resultados)
#     return resultados

# def get_atributos(operacao):
#     cache_key = f"atributos:{operacao}"
#     cached = get_from_cache(cache_key)
#     if cached:
#         return cached

#     conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
#     cur = conn.cursor()
#     cur.execute(f"""
#         select distinct atributo, case when GERENTE is not null then GERENTE
# when GERENTEPLENO is not null then GERENTEPLENO
# when GERENTESENIOR is not null then GERENTESENIOR
# else null end as Gerente from [robbyson].[rlt].[hmn] (nolock) 
#         where data = convert(date, getdate()-1) and operacaohominum like '%{operacao}%'
#     """)
#     resultados = [{"atributo": i[0], "gerente": i[1]} for i in cur.fetchall()]
#     cur.close()
#     conn.close()

#     set_cache(cache_key, resultados)
#     return resultados

def get_atributos_adm_apoio():
    conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
    cur = conn.cursor()
    cur.execute(f"""
       select distinct atributo, gerente, tipo_matriz from Robbyson.dbo.Matriz_Geral (nolock) where (gerente <> '' and tipo_matriz <> '')
    """)
    resultados = [{"atributo": i[0], "gerente": i[1], "tipo": i[2]} for i in cur.fetchall()]
    cur.close()
    conn.close()
    return resultados

def get_atributos_matricula(matricula):
    cache_key = f"atributos_matricula:{matricula}"
    cached = get_from_cache(cache_key)
    if cached:
        return cached

    conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
    cur = conn.cursor()
    cur.execute(f"""
    select distinct atributo, case when GERENTE is not null then GERENTE
    when GERENTEPLENO is not null then GERENTEPLENO
    when GERENTESENIOR is not null then GERENTESENIOR
    else null end as Gerente, TipoHierarquia from [robbyson].[rlt].[hmn] (nolock) 
    where (data = convert(date, getdate()-1)) and (atributo is not null) 
    and (MatrGERENTE = {matricula} or MatrGERENTEPLENO = {matricula} or MatrGERENTESENIOR = {matricula} or MatrCOORDENADOR = {matricula})
    """)
    resultados = [{"atributo": i[0], "gerente": i[1], "tipo": i[2]} for i in cur.fetchall()]
    cur.close()
    conn.close()

    set_cache(cache_key, resultados)
    return resultados

def get_funcao(matricula):
    cache_key = f"funcao:{matricula}"
    cached = get_from_cache(cache_key)
    if cached:
        return cached

    conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
    cur = conn.cursor()
    cur.execute(f"""
        select distinct funcaorm 
        from [robbyson].[rlt].[hmn] (nolock) 
        where data = convert(date, getdate()-1) 
          and matricula = '{matricula}'
    """)
    resultados = [i[0] for i in cur.fetchall()]
    cur.close()
    conn.close()

    if resultados:
        set_cache(cache_key, resultados[0])
        return resultados[0]

    return None

def get_resultados(atributo, id_indicador):
    cache_key = f"resultados:{atributo+id_indicador}"
    cached = get_from_cache(cache_key)
    if cached:
        return cached

    conn = pyodbc.connect("Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;")
    cur = conn.cursor()
    cur.execute(f"""
------------------------------------------------------------
-- Criação da tabela temporária #fct
------------------------------------------------------------
SET NOCOUNT ON;
DECLARE @atributo VARCHAR(100) = '{atributo}'
DECLARE @id_indicador VARCHAR(100) = {id_indicador}
SELECT 
      [data],
      [atributo],
      [id],
      factibilidade,
      CASE 
          WHEN factibilidade = 'Meta AeC' THEN NULL
          ELSE
              CASE id.[tipo_medida_indicador]
                  WHEN 'admin_generic_percentage' THEN FORMAT(ff.[MetaSugerida], '0.00%')
                  WHEN 'admin_generic_hour'       THEN CONVERT(VARCHAR(8), DATEADD(SECOND, ff.[MetaSugerida], '00:00:00'), 108)
                  WHEN 'admin_generic_integer'    THEN FORMAT(ff.[MetaSugerida], '0')
                  WHEN 'admin_generic_float'      THEN FORMAT(ff.[MetaSugerida], '0.0')
                  WHEN 'admin_generic_coin'       THEN FORMAT(ff.[MetaSugerida], '0.0')
                  ELSE CAST(ff.[MetaSugerida] AS VARCHAR)
              END
      END AS [MetaSugerida],

      CASE 
          WHEN factibilidade = 'Meta AeC' THEN 'Meta AeC' 
          ELSE Meta_Escolhida 
      END AS Meta_Escolhida,

      CASE id.[tipo_medida_indicador]
          WHEN 'admin_generic_percentage' THEN FORMAT(ff.[MetaDesv], '0.00%')
          WHEN 'admin_generic_hour'       THEN CONVERT(VARCHAR(8), DATEADD(SECOND, ff.[MetaDesv], '00:00:00'), 108)
          WHEN 'admin_generic_integer'    THEN FORMAT(ff.[MetaDesv], '0')
          WHEN 'admin_generic_float'      THEN FORMAT(ff.[MetaDesv], '0.0')
          WHEN 'admin_generic_coin'       THEN FORMAT(ff.[MetaDesv], '0.0')
          ELSE CAST(ff.[MetaDesv] AS VARCHAR)
      END AS [MetaDesv],

      CASE id.[tipo_medida_indicador]
          WHEN 'admin_generic_percentage' THEN FORMAT(ff.[MetaG1G2], '0.00%')
          WHEN 'admin_generic_hour'       THEN CONVERT(VARCHAR(8), DATEADD(SECOND, ff.[MetaG1G2], '00:00:00'), 108)
          WHEN 'admin_generic_integer'    THEN FORMAT(ff.[MetaG1G2], '0')
          WHEN 'admin_generic_float'      THEN FORMAT(ff.[MetaG1G2], '0.0')
          WHEN 'admin_generic_coin'       THEN FORMAT(ff.[MetaG1G2], '0.0')
          ELSE CAST(ff.[MetaG1G2] AS VARCHAR)
      END AS [MetaG1G2],

      CASE id.[tipo_medida_indicador]
          WHEN 'admin_generic_percentage' THEN FORMAT(ff.[MetaG1G2G3], '0.00%')
          WHEN 'admin_generic_hour'       THEN CONVERT(VARCHAR(8), DATEADD(SECOND, ff.[MetaG1G2G3], '00:00:00'), 108)
          WHEN 'admin_generic_integer'    THEN FORMAT(ff.[MetaG1G2G3], '0')
          WHEN 'admin_generic_float'      THEN FORMAT(ff.[MetaG1G2G3], '0.0')
          WHEN 'admin_generic_coin'       THEN FORMAT(ff.[MetaG1G2G3], '0.0')
          ELSE CAST(ff.[MetaG1G2G3] AS VARCHAR)
      END AS [MetaG1G2G3],

      CASE id.[tipo_medida_indicador]
          WHEN 'admin_generic_percentage' THEN FORMAT(ff.[MetaG1G2_historico], '0.00%')
          WHEN 'admin_generic_hour'       THEN CONVERT(VARCHAR(8), DATEADD(SECOND, ff.[MetaG1G2_historico], '00:00:00'), 108)
          WHEN 'admin_generic_integer'    THEN FORMAT(ff.[MetaG1G2_historico], '0')
          WHEN 'admin_generic_float'      THEN FORMAT(ff.[MetaG1G2_historico], '0.0')
          WHEN 'admin_generic_coin'       THEN FORMAT(ff.[MetaG1G2_historico], '0.0')
          ELSE CAST(ff.[MetaG1G2_historico] AS VARCHAR)
      END AS [MetaG1G2_historico],

      CASE id.[tipo_medida_indicador]
          WHEN 'admin_generic_percentage' THEN FORMAT(ff.[MetaG1G2_historico], '0.00%')
          WHEN 'admin_generic_hour'       THEN CONVERT(VARCHAR(8), DATEADD(SECOND, ff.[MetaG1G2_historico], '00:00:00'), 108)
          WHEN 'admin_generic_integer'    THEN FORMAT(ff.[MetaG1G2_historico], '0')
          WHEN 'admin_generic_float'      THEN FORMAT(ff.[MetaG1G2_historico], '0.0')
          WHEN 'admin_generic_coin'       THEN FORMAT(ff.[MetaG1G2_historico], '0.0')
          ELSE CAST(ff.[MetaG1G2_historico] AS VARCHAR)
      END AS [MetaG1G2G3_historico],

      FORMAT([atingimento_projetado], '0%') AS atingimento_projetado

INTO #fct
FROM [Robbyson].[dbo].[factibilidadeEfaixas] AS ff WITH (NOLOCK)
LEFT JOIN [Robbyson].[rby].[indicador] AS id WITH (NOLOCK) 
       ON ff.id = id.id_indicador
WHERE data >= DATEADD(DD, 1, EOMONTH(DATEADD(MM, -1, GETDATE())))
-- AND factibilidade NOT IN ('Meta AeC')
AND atributo like @atributo
AND id_indicador = @id_indicador
-- AND id IN ('215')
ORDER BY data, atributo, id;


------------------------------------------------------------
-- Seleção final
------------------------------------------------------------
SELECT DISTINCT 
      mat.[id_indicador],
      id.[indicador_nome],
      mat.meta,
      mat.[Atributo],
      mat.ganho_g1,
      mat.escala,
      mat.acumulado,
      mat.data_inicio,
      mat.data_fim,
      fct.metasugerida,
      fct.Meta_Escolhida,
      fct.atingimento_projetado,

      CASE id.[tipo_medida_indicador]
          WHEN 'admin_generic_percentage' THEN FORMAT(rc.[resultado], '0.00%')
          WHEN 'admin_generic_hour'       THEN CONVERT(VARCHAR(8), DATEADD(SECOND, [resultado], '00:00:00'), 108)
          WHEN 'admin_generic_integer'    THEN FORMAT(rc.[resultado], '0')
          WHEN 'admin_generic_float'      THEN FORMAT(rc.[resultado], '0.0')
          WHEN 'admin_generic_coin'       THEN FORMAT(rc.[resultado], '0.0')
          ELSE CAST(rc.[resultado] AS VARCHAR)
      END AS [resultado_m0],

      FORMAT([atingimento], '0%') AS atingimento_m0,

      CASE id.[tipo_medida_indicador]
          WHEN 'admin_generic_percentage' THEN FORMAT(rc.[meta], '0.00%')
          WHEN 'admin_generic_hour'       THEN CONVERT(VARCHAR(8), DATEADD(SECOND, rc.[meta], '00:00:00'), 108)
          WHEN 'admin_generic_integer'    THEN FORMAT(rc.[meta], '0')
          WHEN 'admin_generic_float'      THEN FORMAT(rc.[meta], '0.0')
          WHEN 'admin_generic_coin'       THEN FORMAT(rc.[meta], '0.0')
          ELSE CAST(rc.[meta] AS VARCHAR)
      END AS [meta_formatada],

      CASE id.[tipo_medida_indicador]
          WHEN 'admin_generic_percentage' THEN FORMAT(rc.[resultado_anterior], '0.00%')
          WHEN 'admin_generic_hour'       THEN CONVERT(VARCHAR(8), DATEADD(SECOND, [resultado_anterior], '00:00:00'), 108)
          WHEN 'admin_generic_integer'    THEN FORMAT(rc.[resultado_anterior], '0')
          WHEN 'admin_generic_float'      THEN FORMAT(rc.[resultado_anterior], '0.0')
          WHEN 'admin_generic_coin'       THEN FORMAT(rc.[resultado_anterior], '0.0')
          ELSE CAST(rc.[resultado_anterior] AS VARCHAR)
      END AS [resultado_m1],

      FORMAT([atingimento_anterior], '0%') AS atingimento_m1,
      [MaxData],
      rc.mes, 
      rc.mes_anterior,
      CASE 
          WHEN mes IS NULL THEN MONTH(mat.Data_inicio) 
          ELSE MONTH(rc.mes) 
      END AS mesreal,
      id.[tipo_medida_indicador]

FROM [ROBBYSON].[RBY].[IMPORT_MATRIZ] AS mat WITH (NOLOCK)

LEFT JOIN (
    SELECT 
          rc.*, 
          MONTH(rc.mes) AS mesreal,
          rc_ant.atingimento AS atingimento_anterior,
          rc_ant.resultado   AS resultado_anterior,
          rc_ant.meta        AS meta_anterior,
          rc_ant.mes         AS mes_anterior
    FROM [Robbyson].[dbo].[resultado_consolidado] rc WITH (NOLOCK)
    LEFT JOIN [Robbyson].[dbo].[resultado_consolidado] rc_ant WITH (NOLOCK)
           ON rc_ant.id_indicador = rc.id_indicador
          AND rc_ant.atributo     = rc.atributo
          AND rc_ant.mes          = DATEADD(DAY, 1, EOMONTH(DATEADD(MONTH, -2, GETDATE())))
    WHERE rc.mes = DATEADD(DAY, 1, EOMONTH(DATEADD(MONTH, -1, GETDATE())))
) AS rc
       ON rc.id_indicador = mat.id_indicador 
      AND rc.atributo     = mat.atributo 
      AND MONTH(mat.Data_inicio) = rc.mesreal

LEFT JOIN (
    SELECT
          atributo,
          cliente,
          operacaohominum,
          segmento1,
          site,
          COUNT(DISTINCT data) AS idade
    FROM [Robbyson].[rlt].[hmn] (NOLOCK)
    WHERE data BETWEEN DATEADD(d,1,EOMONTH(GETDATE(),-1)) 
                   AND EOMONTH(DATEADD(MM, 0, GETDATE()))
      AND NivelHierarquico = 'operacional'
      AND TipoHierarquia   = 'operação'
      AND atributo IS NOT NULL
      AND SituacaoHominum = 'ativo'
      AND FuncaoRM NOT LIKE 'analista de aten%'
      AND FuncaoRM NOT LIKE 'auxiliar%'
      AND FuncaoRM NOT LIKE 'analista de op%'
      AND atributo NOT LIKE '%novo_tempo%'
      AND atributo NOT IN ('premium - banco fibra - banco_fibra - sp5')
    GROUP BY
          atributo,
          cliente,
          operacaohominum,
          segmento1,
          site
) AS hm 
       ON hm.atributo = mat.atributo

LEFT JOIN [Robbyson].[rby].[indicador] AS id WITH (NOLOCK) 
       ON mat.id_indicador = id.id_indicador

LEFT JOIN #fct fct 
       ON fct.data     = mat.data_inicio 
      AND fct.atributo = mat.atributo
      AND fct.id = mat.id_indicador

WHERE mat.data_inicio BETWEEN DATEADD(d,1,EOMONTH(GETDATE(),-1)) 
                          AND EOMONTH(DATEADD(MM, 0, GETDATE()))
  AND mat.ativo NOT IN (0, 6, 8) 
  AND mat.tipo_matriz = 'Operacional'
  AND mat.Atributo LIKE @atributo
  AND mat.id_indicador = @id_indicador

DROP TABLE #fct;


    """)

    resultados = cur.fetchall()
    cur.close()
    conn.close()

    set_cache(cache_key, resultados)
    return resultados