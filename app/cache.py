from collections import OrderedDict
from datetime import datetime, timedelta
import fakeredis
from fastapi import Request
import json
from datetime import timedelta
import redis

CACHE = OrderedDict()
CACHE_TTL = timedelta(minutes=5)
CACHE_MAX_SIZE = 100
redis_client = fakeredis.FakeRedis()

# redis_client = redis.StrictRedis(
#     host='192.168.0.9',
#     port=6379, # Porta padrão, mas pode mudar
#     db=0,
#     decode_responses=True
# )

# CACHE_TTL_24H = timedelta(hours=24).total_seconds() # 86400 segundos
SESSION_PREFIX = "session:"

# def set_cache_24h(key: str, valor):
#     """Salva um valor no Redis com expiração de 24 horas."""
#     # O Redis armazena strings, então convertemos para JSON
#     data = json.dumps(valor)
#     redis_client.set(key, data, ex=int(CACHE_TTL_24H)) # 'ex' define o tempo de expiração em segundos

def get_from_cache(key: str):
    if key in CACHE:
        valor, expira_em = CACHE[key]
        if datetime.utcnow() < expira_em:
            CACHE.move_to_end(key)
            return valor
        del CACHE[key]
    return None

def set_cache(key: str, valor, ttl: timedelta = CACHE_TTL):
    if key in CACHE:
        del CACHE[key]
    elif len(CACHE) >= CACHE_MAX_SIZE:
        CACHE.popitem(last=False)
    CACHE[key] = (valor, datetime.utcnow() + ttl)

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

def set_session(token: str, data: dict):
    redis_client.set(f"{SESSION_PREFIX}{token}", json.dumps(data))

def get_session(token: str):
    data = redis_client.get(f"{SESSION_PREFIX}{token}")
    return json.loads(data) if data else None

def get_current_user(request: Request):
    token = request.cookies.get("session_token")
    if not token:
        return None
    return get_session(token)
