from collections import OrderedDict
from datetime import datetime, timedelta
import fakeredis
from fastapi import Request
import json

CACHE = OrderedDict()
CACHE_TTL = timedelta(minutes=5)
CACHE_MAX_SIZE = 100
redis_client = fakeredis.FakeRedis()

SESSION_PREFIX = "session:"


def get_from_cache(key: str):
    if key in CACHE:
        valor, expira_em = CACHE[key]
        if datetime.utcnow() < expira_em:
            CACHE.move_to_end(key)
            return valor
        del CACHE[key]
    return None


def set_cache(key: str, valor):
    if key in CACHE:
        del CACHE[key]
    elif len(CACHE) >= CACHE_MAX_SIZE:
        CACHE.popitem(last=False)
    CACHE[key] = (valor, datetime.utcnow() + CACHE_TTL)


def get_user_key(request: Request):
    username = request.cookies.get("username", "anon")
    return f"registros:{username}"


def load_registros(request: Request):
    key = get_user_key(request)
    data = redis_client.get(key)
    return json.loads(data) if data else []


def save_registros(request: Request, registros):
    key = get_user_key(request)
    redis_client.set(key, json.dumps(registros))


# --- Novas funções para sessão de usuário com role ---
def set_session(token: str, data: dict):
    """
    Salva sessão de usuário no Redis.
    data deve conter pelo menos {"usuario": ..., "role": ...}
    """
    redis_client.set(f"{SESSION_PREFIX}{token}", json.dumps(data))


def get_session(token: str):
    data = redis_client.get(f"{SESSION_PREFIX}{token}")
    return json.loads(data) if data else None


def get_current_user(request: Request):
    """
    Recupera usuário atual a partir do cookie de sessão.
    """
    token = request.cookies.get("session_token")
    if not token:
        return None
    return get_session(token)
