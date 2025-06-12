from flask import Flask, request, jsonify
from atualizar_cache import get_conn, upsert_deal, format_date

app = Flask(__name__)

@app.route("/bitrix-webhook", methods=["POST"])
def bitrix_webhook():
    try:
        data = request.get_json(force=True)
        print("📥 Payload recebido:", data)  # 👈 log do conteúdo bruto
    except Exception as e:
        return jsonify({"error": "Payload inválido", "detalhe": str(e)}), 400


    deal = data["data"]["FIELDS"]

    # Formata datas
    if "DATE_CREATE" in deal:
        deal["DATE_CREATE"] = format_date(deal["DATE_CREATE"])
    if "UF_CRM_1698761151613" in deal:
        deal["UF_CRM_1698761151613"] = format_date(deal["UF_CRM_1698761151613"])

    try:
        conn = get_conn()
        upsert_deal(conn, deal)
        conn.commit()
        conn.close()
        print(f"✅ Deal {deal.get('ID')} salvo no banco")
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        print(f"❌ Erro ao salvar no banco: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=1433)
