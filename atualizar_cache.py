import requests
import json
import time
import os
import psycopg2

# Lista de Webhooks
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

# Conexão com PostgreSQL
def get_conn():
    return psycopg2.connect(
        dbname="seubanco",
        user="seuusuario",
        password="suasenha",
        host="localhost",
        port="5432"
    )

def fazer_requisicao(local_params):
    for webhook in WEBHOOKS:
        try:
            resp = requests.get(webhook, params=local_params, timeout=30)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 1))
                print(f"⏳ Limite de requisições atingido. Aguardando {retry_after}s...")
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            print(f"✅ Requisição bem-sucedida.")
            return resp.json()
        except Exception as e:
            print(f"❌ Erro com webhook: {e}")
            continue
    return None

def inserir_no_banco(deals):
    conn = get_conn()
    cur = conn.cursor()

    for deal in deals:
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

    conn.commit()
    cur.close()
    conn.close()
    print(f"💾 {len(deals)} registros inseridos/atualizados no banco.")

def baixar_todos_dados():
    local_params = PARAMS.copy()
    tentativas = 0

    while True:
        print(f"📡 Requisição com start={local_params['start']}")
        data = fazer_requisicao(local_params)

        if data is None:
            tentativas += 1
            if tentativas >= MAX_RETRIES:
                print("🚫 Máximo de tentativas. Abortando.")
                break
            time.sleep(RETRY_DELAY)
            continue

        tentativas = 0

        deals = data.get("result", [])
        if not deals:
            print("🏁 Fim da paginação.")
            break

        inserir_no_banco(deals)

        if 'next' in data and data['next']:
            local_params['start'] = data['next']
            if len(deals) >= LIMITE_REGISTROS_TURBO:
                time.sleep(PAGE_DELAY)
            else:
                print("🚀 Modo turbo ativo.")
        else:
            print("🏁 Fim da paginação.")
            break

        if len(deals) >= LIMITE_REGISTROS_TURBO:
            time.sleep(REQUEST_DELAY)

if __name__ == "__main__":
    print("🚀 Iniciando atualização no banco...")
    baixar_todos_dados()
    print("✅ Processo concluído com sucesso.")
