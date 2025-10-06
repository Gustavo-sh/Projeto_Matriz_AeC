import pyodbc
from queue import Queue, Empty
from contextlib import contextmanager

# Configurações do Pool
MAX_POOL_SIZE = 100 # Número máximo de conexões (ajuste conforme a carga)
CONNECTION_STRING = "Driver={SQL Server};Server=primno4;Database=Robbyson;Trusted_Connection=yes;"

# Fila para armazenar as conexões livres
_connection_pool = Queue(maxsize=MAX_POOL_SIZE)

def _create_connection():
    """Cria e retorna uma nova conexão pyodbc."""
    # Adicionar o atributo 'autocommit=True' aqui pode ser importante dependendo 
    # de como você gerencia suas transações.
    return pyodbc.connect(CONNECTION_STRING)

def _populate_pool():
    """Pré-enche o pool com algumas conexões iniciais."""
    print("Preenchendo Pool de Conexões...")
    for _ in range(5): # Comece com 5 conexões
        try:
            conn = _create_connection()
            _connection_pool.put(conn)
        except Exception as e:
            print(f"Erro ao criar conexão inicial: {e}")
            break
    print(f"Pool de Conexões preenchido: {_connection_pool.qsize()} conexões.")

# Preencher o pool ao iniciar a aplicação (opcional, mas recomendado)
#_populate_pool() 

@contextmanager
def get_db_connection():
    """
    Context Manager que fornece uma conexão do pool.
    Garante que a conexão seja devolvida ao pool.
    """
    conn = None
    try:
        # Tenta obter uma conexão do pool (timeout de 1 segundo)
        conn = _connection_pool.get(timeout=1)
    except Empty:
        # Se o pool estiver vazio, cria uma nova conexão (até MAX_POOL_SIZE)
        try:
            conn = _create_connection()
        except Exception as e:
            # Se não conseguir criar, levanta o erro
            print(f"Erro fatal ao criar nova conexão: {e}")
            raise

    # Fornece a conexão ao bloco 'with'
    try:
        yield conn
    finally:
        # DEVOLVE a conexão ao pool
        if conn is not None:
            # 1. Checa se a conexão está FECHADA (a correção anterior)
            if not conn.closed:
                try:
                    # 2. Tenta um 'ping' simples para verificar se a conexão está ativa no servidor
                    cur = conn.cursor()
                    cur.execute('SELECT 1') # Comando de ping leve para SQL Server
                    cur.close()
                    
                    # Se o ping funcionou, a conexão está viva, devolvemos ao pool.
                    _connection_pool.put(conn)
                    
                except pyodbc.Error as e:
                    # Se o ping falhar (erro de timeout, desconexão, etc.), descartamos a conexão.
                    print(f"Alerta: Conexão ping falhou ({e}). Descartando do pool.")
                    # A conexão é fechada (opcional, pois o erro de ping geralmente fecha)
                    try:
                        conn.close()
                    except:
                        pass
                    # O sistema irá criar uma nova conexão na próxima vez que precisar.
                
            else:
                # Conexão estava fechada antes de ser devolvida (o log que você viu)
                print("Conexão falhou e foi descartada do pool.")

# Função para inicializar o pool (opcional, mas bom para o FastAPI)
def init_db_pool():
    _populate_pool()