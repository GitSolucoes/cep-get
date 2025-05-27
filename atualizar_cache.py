import requests
import json
import time
import os

# Configurações
BASE_URL = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.deal.list"
PARAMS = {
    "select[]": ["ID", "TITLE", "STAGE_ID", "UF_CRM_1700661314351", "DATE_CREATE"],
    "filter[>=DATE_CREATE]": "2023-12-13",
    "start": 0
}

CACHE_FILE = "cache.json"
CACHE_PARCIAL = "cache_parcial.json"

MAX_RETRIES = 20
RETRY_DELAY = 30
REQUEST_DELAY = 2
PAGE_DELAY = 30
LIMITE_REGISTROS_TURBO = 20000


def carregar_parcial():
    if os.path.exists(CACHE_PARCIAL):
        with open(CACHE_PARCIAL, "r", encoding="utf-8") as f:
            dados = json.load(f)
        print(f"📁 Cache parcial carregado com {len(dados)} registros.")
        return dados
    return []

def salvar_parcial(dados):
    with open(CACHE_PARCIAL, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)

def salvar_cache_final(dados):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    print(f"✅ Cache final salvo com {len(dados)} registros.")

def baixar_todos_dados():
    todos = carregar_parcial()
    local_params = PARAMS.copy()
    local_params["start"] = len(todos)  # retoma da posição

    tentativas = 0

    while True:
        print(f"📡 Requisição com start={local_params['start']} (Registros: {len(todos)})")

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

        tentativas = 0  # reset em caso de sucesso

        deals = data.get("result", [])
        todos.extend(deals)
        print(f"✅ Recebidos: {len(deals)} | Total acumulado: {len(todos)}")

        # salva parcial
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
    dados = baixar_todos_dados()
    salvar_cache_final(dados)
