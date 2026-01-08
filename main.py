from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, JSONResponse
from datetime import datetime
from app.routes import router
from app.database import init_db_pool
from app.connections_db import get_resultados_indicadores_m3, get_excecoes_disponibilidade
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk import metrics

sentry_sdk.init(
    dsn="https://f793ba89c1855cace9ff1e3834468ded@o4510676437172224.ingest.us.sentry.io/4510676438679552",
    integrations=[FastApiIntegration()],
    send_default_pii=True,  # 🔥 ISSO habilita IP e usuário
    environment="producao",
    traces_sample_rate=0.2,
)

metrics.count("checkout.failed", 1)
metrics.gauge("queue.depth", 42)
metrics.distribution("cart.amount_usd", 187.5)
app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(router)
SESSION_TIMEOUT = 10 * 60

@app.on_event("startup")
async def startup_event():
    # 1. Inicializa o pool de conexões
    init_db_pool() 

    # 2. 🔑 PRÉ-CARREGA OS DADOS CRÍTICOS NO CACHE
    print("Iniciando pré-carregamento dos resultados dos indicadores M3 no cache...")
    print("Iniciando pré-carregamento dos atributos que são exceções em disponibilidade no cache...")
    try:
        # A função é assíncrona, então usamos await
        await get_resultados_indicadores_m3() 
        await get_excecoes_disponibilidade()
        print("Pré-carregamento de dados concluído.")
    except Exception as e:
        # É importante tratar exceções para não travar a inicialização do app
        print(f"ERRO durante o pré-carregamento do cache: {e}")
        # O app pode continuar, mas a primeira requisição que chamar a função será lenta.


@app.middleware("http")
async def session_middleware(request: Request, call_next):
    logged_in = request.cookies.get("logged_in")
    last_active = request.cookies.get("last_active")
    now = datetime.utcnow()
    if logged_in == "true" and last_active:
        try:
            last_active_dt = datetime.fromisoformat(last_active)
            delta = (now - last_active_dt).total_seconds()

            if delta > SESSION_TIMEOUT:
                if request.headers.get("HX-Request") == "true":
                    resp = JSONResponse({"detail": "Sessão expirada"}, status_code=401)
                    resp.delete_cookie("logged_in")
                    resp.delete_cookie("last_active")
                    return resp
                else:
                    resp = RedirectResponse("/login", status_code=303)
                    resp.delete_cookie("logged_in")
                    resp.delete_cookie("last_active")
                    return resp
        except Exception:
            pass
    response = await call_next(request)
    if logged_in == "true":
        response.set_cookie("last_active", now.isoformat(), httponly=True)
    return response
