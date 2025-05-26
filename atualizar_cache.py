import requests
import json
import time


BASE_URL = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.deal.list"
PARAMS = {
    "select[]": ["ID", "TITLE", "STAGE_ID", "UF_CRM_1700661314351", "DATE_CREATE"],
    "filter[>=DATE_CREATE]": "2023-12-13",
    "start": 0
}

MAX_RETRIES = 20
RETRY_DELAY = 30  # espera entre tentativas em caso de erro (ex: 429)
REQUEST_DELAY = 2  # espera entre requisições normais
PAGE_DELAY = 30    # espera entre páginas

LIMITE_REGISTROS_TURBO = 20000  # <<< só ativa os delays depois disso

def baixar_todos_dados():
    todos = []
    local_params = PARAMS.copy()
    tentativas = 0

    while True:
        print(f"📡 Requisição com start={local_params.get('start')} (Registros: {len(todos)})")

        try:
            resp = requests.get(BASE_URL, params=local_params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"❌ Erro: {e}")
            tentativas += 1

            if len(todos) >= LIMITE_REGISTROS_TURBO:
                if tentativas >= MAX_RETRIES:
                    print("🚫 Máximo de tentativas atingido. Abortando.")
                    break
                print(f"⏳ Tentando novamente em {RETRY_DELAY}s (modo cauteloso)...")
                time.sleep(RETRY_DELAY)
            else:
                print("❌ Erro durante modo turbo. Abortando por segurança.")
                break

            continue

        tentativas = 0  # reset das tentativas se a requisição der certo

        deals = data.get("result", [])
        todos.extend(deals)
        print(f"✅ Recebidos: {len(deals)} | Total acumulado: {len(todos)}")

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

def salvar_cache(dados):
    with open("cache.json", "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    dados = baixar_todos_dados()
    salvar_cache(dados)
    print(f"Cache salvo com {len(dados)} registros.")
