import requests
import json
import time
import os

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

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

CACHE_FILE = os.path.join(CACHE_DIR, "cache.json")
CACHE_PARCIAL = os.path.join(CACHE_DIR, "cache_parcial.json")


MAX_RETRIES = 20
RETRY_DELAY = 30
REQUEST_DELAY = 2
PAGE_DELAY = 30
LIMITE_REGISTROS_TURBO = 20000

def carregar_parcial():
    if os.path.exists(CACHE_PARCIAL):
        try:
            with open(CACHE_PARCIAL, "r", encoding="utf-8") as f:
                dados = json.load(f)
            print(f"📁 Cache parcial carregado com {len(dados)} registros.")
            return dados
        except json.JSONDecodeError:
            print("⚠️ Cache parcial corrompido. Iniciando do zero.")
            return []
    print("📁 Nenhum cache parcial encontrado. Iniciando do zero.")
    return []

def salvar_parcial(dados):
    with open(CACHE_PARCIAL, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    print(f"💾 Cache parcial e final salvo com {len(dados)} registros.")

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
    todos = carregar_parcial()
    local_params = PARAMS.copy()
    local_params["start"] = len(todos)

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
        print(f"✅ Recebidos: {len(deals)} | Total acumulado: {len(todos)}")

        salvar_parcial(todos)

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

    return todos

if __name__ == "__main__":
    print("🚀 Iniciando atualização do cache...")
    dados = baixar_todos_dados()
    salvar_parcial(dados)
    print("✅ Processo concluído com sucesso.")
