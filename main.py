from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, JSONResponse
from datetime import datetime
from app.routes import router
from app.database import init_db_pool

app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(router)

SESSION_TIMEOUT = 10 * 60

@app.on_event("startup")
async def startup_event():
    init_db_pool() 

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
