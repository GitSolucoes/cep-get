from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import io
import pandas as pd
import psycopg2
import os
import tempfile
from dotenv import load_dotenv
import requests
import time

from atualizar_cache import format_date, get_operadora_map, upsert_deal

print("🔧 Iniciando main.py...")

load_dotenv()
print("✅ Variáveis de ambiente carregadas")

app = FastAPI()
print("✅ FastAPI iniciado")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
print("✅ Diretórios templates e static montados")

BITRIX_API_BASE = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5"
BITRIX_WEBHOOK = f"{BITRIX_API_BASE}/crm.deal.get"


def get_conn():
    print("🔌 Conectando ao banco de dados...")
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )
    print("✅ Conexão com banco estabelecida")
    return conn


def get_categories():
    print("🔍 Buscando categorias do Bitrix...")
    try:
        resp = requests.get(f"{BITRIX_API_BASE}/crm.category.list", params={"entityTypeId": 2})
        data = resp.json()
        print("✅ Categorias obtidas")
        return {cat["id"]: cat["name"] for cat in data.get("result", {}).get("categories", [])}
    except Exception as e:
        print(f"❌ Erro ao buscar categorias: {e}")
        return {}


def get_stages(category_id):
    print(f"🔍 Buscando estágios da categoria {category_id}...")
    try:
        resp = requests.get(f"{BITRIX_API_BASE}/crm.dealcategory.stage.list", params={"id": category_id})
        data = resp.json()
        return {stage["STATUS_ID"]: stage["NAME"] for stage in data.get("result", [])}
    except Exception as e:
        print(f"❌ Erro ao buscar estágios: {e}")
        return {}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    print("📥 Acessando página inicial")
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/bitrix-webhook")
async def bitrix_webhook(request: Request):
    print("🔔 Webhook recebido")
    try:
        form_data = await request.form()
        deal_id = form_data.get("data[FIELDS][ID]")
        print(f"🆔 Deal ID recebido: {deal_id}")

        if not deal_id:
            print("⚠️ ID do negócio não encontrado")
            return JSONResponse({"error": "ID do negócio não encontrado"}, status_code=400)

        resp = requests.get(BITRIX_WEBHOOK, params={"id": deal_id}, timeout=20)
        data = resp.json()
        print(f"📦 Resposta do Bitrix: {data}")

        if "result" not in data:
            print("❌ Resposta inválida do Bitrix")
            return JSONResponse({"error": "Resposta inválida do Bitrix"}, status_code=502)

        deal = data["result"]
        print(f"📄 Deal bruto: {deal}")

        # Formata campos
        deal["DATE_CREATE"] = format_date(deal.get("DATE_CREATE"))
        deal["UF_CRM_1698761151613"] = format_date(deal.get("UF_CRM_1698761151613"))

        categorias = get_categories()
        estagios_por_categoria = {cat_id: get_stages(cat_id) for cat_id in categorias}
        operadora_map = get_operadora_map()

        cat_id = deal.get("CATEGORY_ID")
        stage_id = deal.get("STAGE_ID")
        if cat_id in categorias:
            deal["CATEGORY_ID"] = categorias[cat_id]
        if cat_id in estagios_por_categoria and stage_id in estagios_por_categoria[cat_id]:
            deal["STAGE_ID"] = estagios_por_categoria[cat_id][stage_id]

        ids = deal.get("UF_CRM_1699452141037", [])
        if not isinstance(ids, list):
            ids = []
        nomes = [operadora_map.get(str(i)) for i in ids if str(i) in operadora_map]
        deal["UF_CRM_1699452141037"] = ", ".join([n for n in nomes if n]) or ""

        conn = get_conn()
        upsert_deal(conn, deal)
        conn.commit()
        conn.close()

        print(f"✅ Deal {deal_id} atualizado com sucesso")
        return JSONResponse({"status": "ok", "deal_id": deal_id}, status_code=200)

    except Exception as e:
        print(f"❌ Erro no webhook: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=1433, reload=True)
