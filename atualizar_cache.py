import requests
import json

BASE_URL = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.deal.list"
PARAMS = {
    "select[]": ["ID", "TITLE", "STAGE_ID", "UF_CRM_1700661314351", "DATE_CREATE"],
    "filter[>=DATE_CREATE]": "2023-12-13",
    "start": 0
}

def baixar_todos_dados():
    todos = []
    local_params = PARAMS.copy()
    tentativas = 0

    while True:
        try:
            print(f"📡 Requisição com start={local_params.get('start')}")
            resp = requests.get(BASE_URL, params=local_params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"❌ Erro: {e}")
            tentativas += 1
            if tentativas >= 5:
                print("⚠️ Muitas falhas seguidas. Abortando.")
                break
            print("⏳ Tentando novamente em 5 segundos...")
            time.sleep(5)
            continue

        tentativas = 0  # Reset se a requisição der certo

        deals = data.get("result", [])
        todos.extend(deals)
        print(f"✅ Recebidos: {len(deals)} | Total acumulado: {len(todos)}")

        if 'next' in data and data['next']:
            local_params['start'] = data['next']
        else:
            print("🏁 Fim da paginação.")
            break

    return todos


def salvar_cache(dados):
    with open("cache.json", "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    dados = baixar_todos_dados()
    salvar_cache(dados)
    print(f"Cache salvo com {len(dados)} registros.")
