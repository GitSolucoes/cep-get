import psycopg2
import requests
import time
import os
import json

# Parâmetros banco - ideal pegar do .env (use dotenv ou coloque direto aqui)
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

PARAMS = {
    "select[]": ["ID", "TITLE", "STAGE_ID", "UF_CRM_1700661314351", "UF_CRM_1698698407472", "DATE_CREATE"],
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
        # Inserir ou atualizar pelo ID
        cur.execute("""
            INSERT INTO deals (id, title, stage_id, uf_crm_cep, uf_crm_contato, date_create)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                stage_id = EXCLUDED.stage_id,
                uf_crm_cep = EXCLUDED.uf_crm_cep,
                uf_crm_contato = EXCLUDED.uf_crm_contato,
                date_create = EXCLUDED.date_create;
        """, (
            deal.get("ID"),
            deal.get("TITLE"),
            deal.get("STAGE_ID"),
            deal.get("UF_CRM_1700661314351"),
            deal.get("UF_CRM_1698698407472"),
            deal.get("DATE_CREATE")
        ))

def fazer_requisicao(local_params):
    for webhook in WEBHOOKS:
        try:
            resp = requests.get(webhook, params=local_params, timeout=30)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 1))
                print(f"⏳ Limite de requisições atingido para {webhook}. Aguardando {retry_after}s...")
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            print(f"✅ Requisição bem-sucedida com {webhook}")
            return resp.json()
        except Exception as e:
            print(f"❌ Erro com webhook {webhook}: {e}")
            continue
    print("🚫 Todos os webhooks falharam.")
    return None

def baixar_todos_dados():
    conn = get_conn()
    conn.autocommit = False

    todos = []
    local_params = PARAMS.copy()
    local_params["start"] = 0

    tentativas = 0

    while True:
        print(f"📡 Requisição com start={local_params['start']} (Registros acumulados: {len(todos)})")

        data = fazer_requisicao(local_params)

        if data is None:
            tentativas += 1
            if tentativas >= MAX_RETRIES:
                print("🚫 Máximo de tentativas com todos os webhooks. Abortando.")
                break
            print(f"⏳ Tentando novamente em {RETRY_DELAY}s (tentativa {tentativas}/{MAX_RETRIES})...")
            time.sleep(RETRY_DELAY)
            continue

        tentativas = 0

        deals = data.get("result", [])
        todos.extend(deals)

        for deal in deals:
            upsert_deal(conn, deal)

        conn.commit()
        print(f"💾 Inseridos/Atualizados {len(deals)} registros no banco.")

        if 'next' in data and data['next']:
            local_params['start'] = data['next']
            if len(todos) >= LIMITE_REGISTROS_TURBO:
                print(f"⏳ Modo cauteloso ativo. Aguardando {PAGE_DELAY}s...")
                time.sleep(PAGE_DELAY)
            else:
                print("🚀 Modo turbo ativo. Indo direto pra próxima página.")
        else:
            print("🏁 Fim da paginação.")
            break

        if len(todos) >= LIMITE_REGISTROS_TURBO:
            time.sleep(REQUEST_DELAY)

    conn.close()
    return todos

if __name__ == "__main__":
    print("🚀 Iniciando atualização no banco...")
    baixar_todos_dados()
    print("✅ Processo concluído com sucesso.")
