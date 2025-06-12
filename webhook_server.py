from flask import Flask, request
from atualizar_cache import get_conn, upsert_deal, format_date

app = Flask(__name__)

@app.route("/bitrix-webhook", methods=["POST"])
def bitrix_webhook():
    print("🔔 Webhook recebido")
    
    # Exibe headers para saber o tipo de conteúdo
    print("📩 Headers:", dict(request.headers))
    
    # Exibe corpo cru (caso não seja JSON)
    raw = request.get_data(as_text=True)
    print("📦 Corpo cru da requisição:", raw)

    return {"debug": "recebido"}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=1433)
