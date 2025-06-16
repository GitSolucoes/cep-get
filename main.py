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

# ⬇️ Funções auxiliares vindas do atualizar_cache.py
from atualizar_cache import format_date, get_operadora_map, upsert_deal

load_dotenv()

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

BITRIX_API_BASE = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5"
BITRIX_WEBHOOK = f"{BITRIX_API_BASE}/crm.deal.get"

# ============ Conexão com banco ============

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )

# ============ Utilitários de Bitrix ============

def get_categories():
    try:
        resp = requests.get(f"{BITRIX_API_BASE}/crm.category.list", params={"entityTypeId": 2})
        data = resp.json()
        return {cat["id"]: cat["name"] for cat in data.get("result", {}).get("categories", [])}
    except:
        return {}

def get_stages(category_id):
    try:
        resp = requests.get(f"{BITRIX_API_BASE}/crm.dealcategory.stage.list", params={"id": category_id})
        data = resp.json()
        return {stage["STATUS_ID"]: stage["NAME"] for stage in data.get("result", [])}
    except:
        return {}

# ============ Busca de dados ============

def buscar_por_cep(cep):
    cep_limpo = cep.replace("-", "").strip()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, title, stage_id, category_id, uf_crm_cep, uf_crm_contato, date_create,
                       contato01, contato02, ordem_de_servico, nome_do_cliente, nome_da_mae,
                       data_de_vencimento, email, cpf, rg, referencia, rua, data_de_instalacao,
                       quais_operadoras_tem_viabilidade
                FROM deals
                WHERE replace(uf_crm_cep, '-', '') = %s;
            """, (cep_limpo,))
            rows = cur.fetchall()

    categorias = get_categories()
    stages_cache = {}

    resultados = []
    for r in rows:
        cat_id = r[3]
        categoria_nome = categorias.get(cat_id, str(cat_id))
        if cat_id not in stages_cache:
            stages_cache[cat_id] = get_stages(cat_id)
        fase_nome = stages_cache[cat_id].get(r[2], r[2])

        resultados.append({
            "id": r[0], "cliente": r[1], "fase": fase_nome, "categoria": categoria_nome,
            "cep": r[4], "contato": r[5], "criado_em": r[6].isoformat() if hasattr(r[6], "isoformat") else str(r[6]),
            "contato01": r[7], "contato02": r[8], "ordem_de_servico": r[9],
            "nome_do_cliente": r[10], "nome_da_mae": r[11], "data_de_vencimento": r[12],
            "email": r[13], "cpf": r[14], "rg": r[15], "referencia": r[16], "rua": r[17],
            "data_de_instalacao": r[18], "quais_operadoras_tem_viabilidade": r[19]
        })
    return resultados

def buscar_varios_ceps(lista_ceps):
    ceps_limpos = [c.replace("-", "").strip() for c in lista_ceps if c.strip()]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, title, stage_id, category_id, uf_crm_cep, uf_crm_contato, date_create,
                       contato01, contato02, ordem_de_servico, nome_do_cliente, nome_da_mae,
                       data_de_vencimento, email, cpf, rg, referencia, rua, data_de_instalacao,
                       quais_operadoras_tem_viabilidade
                FROM deals
                WHERE replace(uf_crm_cep, '-', '') = ANY(%s);
            """, (ceps_limpos,))
            rows = cur.fetchall()

    categorias = get_categories()
    stages_cache = {}
    resultados = []
    for r in rows:
        cat_id = r[3]
        categoria_nome = categorias.get(cat_id, str(cat_id))
        if cat_id not in stages_cache:
            stages_cache[cat_id] = get_stages(cat_id)
        fase_nome = stages_cache[cat_id].get(r[2], r[2])

        resultados.append({
            "id": r[0], "cliente": r[1], "fase": fase_nome, "categoria": categoria_nome,
            "uf_crm_cep": r[4], "contato": r[5], "criado_em": r[6].isoformat() if hasattr(r[6], "isoformat") else str(r[6]),
            "contato01": r[7], "contato02": r[8], "ordem_de_servico": r[9],
            "nome_do_cliente": r[10], "nome_da_mae": r[11], "data_de_vencimento": r[12],
            "email": r[13], "cpf": r[14], "rg": r[15], "referencia": r[16], "rua": r[17],
            "data_de_instalacao": r[18], "quais_operadoras_tem_viabilidade": r[19]
        })
    return resultados

async def extrair_ceps_arquivo(arquivo: UploadFile):
    nome = arquivo.filename.lower()
    conteudo = await arquivo.read()
    if nome.endswith(".txt"):
        return conteudo.decode().splitlines()
    elif nome.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(conteudo))
    elif nome.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(conteudo))
    else:
        return []

    for col in df.columns:
        if "cep" in col.lower():
            return df[col].astype(str).tolist()
    return []

# ============ ROTAS ============

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/buscar")
async def buscar(cep: str = Form(None), arquivo: UploadFile = File(None), formato: str = Form("txt")):
    if cep and arquivo and arquivo.filename != "":
        return JSONResponse({"error": "Envie apenas um CEP ou um arquivo, não ambos."}, status_code=400)

    if arquivo and arquivo.filename != "":
        ceps = await extrair_ceps_arquivo(arquivo)
        if not ceps:
            return JSONResponse({"error": "Nenhum CEP encontrado no arquivo."}, status_code=400)
        resultados = buscar_varios_ceps(ceps)
        if formato == "xlsx":
            df = pd.DataFrame(resultados)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                df.to_excel(tmp.name, index=False)
                tmp.seek(0)
                return FileResponse(tmp.name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename="resultado.xlsx")
        else:
            output = io.StringIO()
            for res in resultados:
                output.write(f"{res}\n")
            output.seek(0)
            return StreamingResponse(output, media_type="text/plain", headers={"Content-Disposition": 'attachment; filename="resultado.txt"'})

    elif cep:
        resultados = buscar_por_cep(cep)
        return JSONResponse({"total": len(resultados), "resultados": resultados})

    return JSONResponse({"error": "Nenhum CEP ou arquivo enviado."}, status_code=400)

@app.post("/bitrix-webhook")
async def bitrix_webhook(request: Request):
    form_data = await request.form()
    deal_id = form_data.get("data[FIELDS][ID]")
    if not deal_id:
        return JSONResponse({"error": "ID do negócio não encontrado"}, status_code=400)

    try:
        resp = requests.get(BITRIX_WEBHOOK, params={"id": deal_id}, timeout=20)
        data = resp.json()
        if "result" not in data:
            return JSONResponse({"error": "Resposta inválida do Bitrix"}, status_code=502)

        deal = data["result"]
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

        return JSONResponse({"status": "ok", "deal_id": deal_id}, status_code=200)

    except Exception as e:
        print(f"Erro ao processar webhook: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
