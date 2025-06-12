from flask import Flask, request
import threading
from atualizar_cache import baixar_todos_dados  # seu script já existente

app = Flask(__name__)

@app.route("/bitrix-webhook", methods=["POST"])
def bitrix_webhook():
    print("🔔 Webhook recebido do Bitrix24")
    threading.Thread(target=baixar_todos_dados).start()
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
