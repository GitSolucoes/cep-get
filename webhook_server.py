from flask import Flask, request, jsonify
from atualizar_cache import get_conn, upsert_deal, format_date

app = Flask(__name__)

@app.route("/bitrix-webhook", methods=["POST"])
def bitrix_webhook():
    try:
        data = request.get_json(force=True)
        print("📥 Payload recebido:", data)  # 👈 ESSENCIAL para debug
    except Exception as e:
        print("❌ Erro ao fazer parse do JSON:", e)
        return jsonify({"error": "Payload inválido", "detalhe": str(e)}), 400

    if not data:
        print("❌ JSON vazio ou malformado")
        return jsonify({"error": "Sem conteúdo no JSON"}), 400

    return jsonify({"debug": "recebido"}), 200  # Só para teste por enquanto

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=1433)
