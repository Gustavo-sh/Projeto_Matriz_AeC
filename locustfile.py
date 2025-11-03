# locustfile.py
from locust import HttpUser, task, between, events
import random
import os

# ====== CONFIGURÁVEIS ======
USUARIOS = os.getenv("LOCUST_USERS", "280581,340154,295723,456369").split(",")
SENHA_PADRAO = os.getenv("LOCUST_PASSWORD", "123")   # ajuste se precisar
LOGIN_PATH = os.getenv("LOCUST_LOGIN_PATH", "/login")  # ajuste se a rota for outra

# Se quiser simular uma parte sem cache (só funciona se o backend respeitar X-Cache-Bypass)
PERCENTUAL_SEM_CACHE = 0.30  # 30% das requisições enviam header X-Cache-Bypass: 1

# Lista dos 40 atributos (você pode substituir/estender)
ATRIBUTOS_CLARO = [
    "CLARO - CLARO AQUISICAO - CLARO_AQUISICAO - MSS",
    "CLARO - CLARO CONTROLE - CLARO_CONTROLE _BKO_ANATEL - ARP1",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_BKO - CG",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_COBRANCA - ARP1",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_COBRANCA - ARP3",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_COBRANCA_POSPAGO - ARP3",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_COGNITIVO - ARP1",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_COGNITIVO - ARP3",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_COGNITIVO_POSPAGO - ARP1",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_COGNITIVO_POSPAGO - ARP3",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_FLEX - ARP1",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_FLEX - ARP3",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_NIVEL_I - ARP1",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_NIVEL_I - ARP3",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_RETENCAO - ARP1",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_SERVICE_TO_SALES - ARP1",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_SERVICE_TO_SALES - ARP3",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_SERVICE_TO_SALES_BKO - ARP3",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_TECNICO - ARP1",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_CONTROLE_TECNICO - ARP3",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_PRE_PAGO_VENDAS - ARP1",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_PRE_PAGO_VENDAS - ARP3",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_PRE_PAGO_VENDAS - PDI",
    "CLARO - CLARO CONTROLE - CLARO_MOVEL_SUPORTE_SUPERIOR - ARP3",
    "CLARO - CLARO CONTROLE PDI - CLARO_MOVEL_CONTROLE_NIVEL_I_PDI - ARP1",
    "CLARO - CLARO CONTROLE PDI - CLARO_MOVEL_CONTROLE_SERVICE_TO_SALES_PDI - ARP1",
    "CLARO - CLARO CONTROLE PDI - CLARO_MOVEL_PRE_PAGO_VENDAS_PDI - ARP1",
    "CLARO - CLARO MOVEL CRC - CLARO_MOVEL_CRC - ARP1",
    "CLARO - CLARO MOVEL CRC - CLARO_MOVEL_CRC_PILOTO_M90 - ARP1",
    "CLARO - CLARO MOVEL CRC - CLARO_MOVEL_CRC_SUPORTE_SUPERIOR - ARP1",
    "CLARO - CLARO MOVEL TELEVENDAS - CLARO_CROSS - MSS",
    "CLARO - CLARO RETENÇÃO ATIVO - CLARO_COMBO_RETENCAO_ATIVA - ARP1",
    "CLARO - CLARO RETENÇÃO ATIVO - CLARO_COMBO_RETENCAO_ATIVA - ARP3",
    "CLARO - CLARO RETENÇÃO ATIVO - CLARO_COMBO_RETENCAO_ATIVA_BACKLOG - ARP1",
    "CLARO - CLARO RETENÇÃO ATIVO - CLARO_COMBO_RETENCAO_ATIVA_BACKLOG - ARP3",
    "CLARO - CLARO RETENÇÃO ATIVO - CLARO_MOVEL_RETENCAO_ATIVA_BANDA_LARGA_2 - ARP1",
    "CLARO - CLARO RETENÇÃO ATIVO - CLARO_MOVEL_RETENCAO_ATIVA_BANDA_LARGA_2 - ARP3",
    "CLARO - CLARO RETENÇÃO ATIVO - CLARO_MOVEL_RETENCAO_ATIVA_BANDA_LARGA_PREDITIVO - ARP1",
    "CLARO - CLARO RETENÇÃO ATIVO - CLARO_MOVEL_RETENCAO_ATIVA_BANDA_LARGA_PREDITIVO - ARP3",
    "CLARO - CLARO RETENÇÃO ATIVO - CLARO_MOVEL_RETENCAO_ATIVA_CONTA_2 - ARP1",
]

# ====== LOCUST USER ======
class MatrizUser(HttpUser):
    host = "http://localhost:8000"
    # intervalos entre requisições por usuário (ajuste conforme realidade)
    wait_time = between(0.5, 2.0)

    def on_start(self):
        """
        Login real: faz POST para /login e captura cookies.
        Ajuste os nomes dos campos do formulário caso sejam diferentes.
        """
        self.username = random.choice(USUARIOS)
        # Ajuste os campos abaixo (username/password) conforme seu formulário real:
        data = {"username": self.username, "password": SENHA_PADRAO}
        # Se seu login usa HTMX, ainda funciona (cookies são setados na resposta/redirect).
        with self.client.post(LOGIN_PATH, data=data, allow_redirects=True, name="LOGIN", catch_response=True) as resp:
            # Aceitamos 200 ou 302 (redirect pós-login)
            if resp.status_code not in (200, 302):
                resp.failure(f"Falha no login ({resp.status_code}) para {self.username}")
            else:
                resp.success()

        # Opcional: setar manualmente o cookie 'username' caso seu backend dependa disso explicitamente
        # (se o login já seta, não precisa)
        self.client.cookies.set("username", self.username)

        # Cabeçalho base que simula navegador com HTMX
        self.base_headers = {
            "User-Agent": "LocustLoadTest/1.0",
            # header que o seu backend usa para detectar a página
            "hx-current-url": "http://localhost:8000/matriz/operacao",
        }

    # ====== Tarefas ======
    @task(6)  # peso maior para pesquisarm0
    def pesquisar_m0(self):
        atributo = random.choice(ATRIBUTOS_CLARO)
        headers = dict(self.base_headers)

        # Fração sem cache (se o backend suportar X-Cache-Bypass)
        if random.random() < PERCENTUAL_SEM_CACHE:
            headers["X-Cache-Bypass"] = "1"

        with self.client.post(
            "/pesquisarm0",
            data={"atributo": atributo},
            headers=headers,
            name="POST /pesquisarm0",
            catch_response=True
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"Status {resp.status_code} em pesquisarm0")
            else:
                resp.success()

    @task(4)  # peso menor para pesquisarm1 (ajuste conforme uso)
    def pesquisar_m1(self):
        atributo = random.choice(ATRIBUTOS_CLARO)
        headers = dict(self.base_headers)
        if random.random() < PERCENTUAL_SEM_CACHE:
            headers["X-Cache-Bypass"] = "1"

        with self.client.post(
            "/pesquisarm1",                    # <- ajuste se a rota real for outra
            data={"atributo": atributo},       # <- ajuste se os campos do form forem outros
            headers=headers,
            name="POST /pesquisarm1",
            catch_response=True
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"Status {resp.status_code} em pesquisarm1")
            else:
                resp.success()
