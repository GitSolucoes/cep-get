import psycopg2
import requests
import time
import os

from dotenv import load_dotenv
load_dotenv()

# Parâmetros banco
DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

WEBHOOKS = [
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.deal.list",
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/y5q6wd4evy5o57ze/crm.deal.list"
]

# Webhooks para pegar categorias e estágios (ajuste se necessário)
WEBHOOK_CATEGORIES = [
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.dealcategory.list",
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/y5q6wd4evy5o57ze/crm.dealcategory.list"
]

WEBHOOK_STAGES = [
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.dealstage.list",
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/y5q6wd4evy5o57ze/crm.dealstage.list"
]

PARAMS = {
    "select[]": ["ID", "TITLE", "STAGE_ID", "CATEGORY_ID", "UF_CRM_1700661314351", "UF_CRM_1698698407472", "DATE_CREATE"],
    "filter[>=DATE_CREATE]": "2023-11-01",
    "start": 0
}

MAX_RETRIES = 20
RETRY_DELAY = 30
REQUEST_DELAY = 2
PAGE_DELAY = 30
LIMITE_REGISTROS_TURBO = 20000

def get_conn():
    return psycopg2.connect(**DB_PARAMS)

def upsert_deal(conn, deal):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO deals ("id", "title", "stage_id", "category_id", "uf_crm_cep", "uf_crm_contato", "date_create")
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("id") DO UPDATE SET
                "title" = EXCLUDED."title",
                "stage_id" = EXCLUDED."stage_id",
                "category_id" = EXCLUDED."category_id",
                "uf_crm_cep" = EXCLUDED."uf_crm_cep",
                "uf_crm_contato" = EXCLUDED."uf_crm_contato",
                "date_create" = EXCLUDED."date_create";
        """, (
            deal.get("ID"),
            deal.get("TITLE"),
            deal.get("STAGE_ID"),
            deal.get("CATEGORY_ID"),
            deal.get("UF_CRM_1700661314351"),
            deal.get("UF_CRM_1698698407472"),
            deal.get("DATE_CREATE")
        ))

def fazer_requisicao(webhooks, params):
    for webhook in webhooks:
        try:
            resp = requests.get(webhook, params=params, timeout=30)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 1))
                print(f"⏳ Limite de requisições atingido: aguardando {retry_after}s...")
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            print(f"✅ Sucesso com {webhook}")
            return resp.json()
        except Exception as e:
            print(f"❌ Erro com {webhook}: {e}")
            continue
    print("🚫 Todos os webhooks falharam.")
    return None

def baixar_todos_dados():
    conn = get_conn()
    conn.autocommit = False
    todos = []
    local_params = PARAMS.copy()
    tentativas = 0

    while True:
        print(f"📡 Requisição start={local_params['start']} | Total acumulado: {len(todos)}")
        data = fazer_requisicao(WEBHOOKS, local_params)
        if data is None:
            tentativas += 1
            if tentativas >= MAX_RETRIES:
                print("🚫 Máximo de tentativas. Abortando.")
                break
            print(f"⏳ Retentativa {tentativas}/{MAX_RETRIES} em {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
            continue

        tentativas = 0
        deals = data.get("result", [])
        todos.extend(deals)
        for deal in deals:
            upsert_deal(conn, deal)

        conn.commit()
        print(f"💾 Processados {len(deals)} registros.")

        if 'next' in data and data['next']:
            local_params['start'] = data['next']
            time.sleep(PAGE_DELAY if len(todos) >= LIMITE_REGISTROS_TURBO else REQUEST_DELAY)
        else:
            print("🏁 Fim da paginação.")
            break

    conn.close()
    return todos

def get_categories():
    params = {"start": 0}
    categories = {}
    while True:
        data = fazer_requisicao(WEBHOOK_CATEGORIES, params)
        if data is None:
            break
        for cat in data.get("result", []):
            categories[cat["ID"]] = cat["NAME"]
        if 'next' in data and data['next']:
            params['start'] = data['next']
        else:
            break
    return categories

def get_stages(category_id):
    params = {
        "filter[CATEGORY_ID]": category_id,
        "start": 0
    }
    stages = {}
    while True:
        data = fazer_requisicao(WEBHOOK_STAGES, params)
        if data is None:
            break
        for stage in data.get("result", []):
            stages[stage["STATUS_ID"]] = stage["NAME"]
        if 'next' in data and data['next']:
            params['start'] = data['next']
        else:
            break
    return stages

if __name__ == "__main__":
    print("🚀 Iniciando atualização dos deals...")
    baixar_todos_dados()
    print("✅ Deals atualizados.")

    print("🚀 Buscando categorias...")
    categorias = get_categories()
    print(f"✅ {len(categorias)} categorias encontradas.")

    # Exemplo: buscar stages da primeira categoria
    if categorias:
        primeira_categoria = list(categorias.keys())[0]
        print(f"🚀 Buscando estágios da categoria {primeira_categoria}...")
        estagios = get_stages(primeira_categoria)
        print(f"✅ {len(estagios)} estágios encontrados.")
