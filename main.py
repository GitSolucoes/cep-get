from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import io
import pandas as pd
import psycopg2
import os
import tempfile
import re
from dotenv import load_dotenv
import requests
import logging

load_dotenv()

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

BITRIX_API_BASE = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5"

# Configura o log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )

def get_categories():
    try:
        resp = requests.get(f"{BITRIX_API_BASE}/crm.category.list", params={"entityTypeId": 2})
        resp.raise_for_status()
        data = resp.json()
        return {cat["id"]: cat["name"] for cat in data.get("result", {}).get("categories", [])}
    except Exception as e:
        logger.error(f"Erro ao buscar categorias do Bitrix: {e}")
        return {}

def get_stages(category_id):
    try:
        resp = requests.get(f"{BITRIX_API_BASE}/crm.dealcategory.stage.list", params={"id": category_id})
        resp.raise_for_status()
        data = resp.json()
        return {stage["STATUS_ID"]: stage["NAME"] for stage in data.get("result", [])}
    except Exception as e:
        logger.error(f"Erro ao buscar stages do Bitrix (categoria {category_id}): {e}")
        return {}

def formatar_dado(dado):
    if dado is None:
        return "N/A"
    if isinstance(dado, str):
        return dado.strip() or "N/A"
    return str(dado)

def buscar_por_cep(cep):
    cep_limpo = re.sub(r'\D', '', cep)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    "id", "title", "stage_id", "category_id", 
                    TRIM("uf_crm_cep") as uf_crm_cep, 
                    "uf_crm_contato", "date_create", "contato01", "contato02", 
                    "ordem_de_servico", 
                    TRIM("nome_do_cliente") as nome_do_cliente,
                    "nome_da_mae", 
                    "data_de_vencimento", "email", "cpf", "rg", "referencia", 
                    TRIM("rua") as rua,
                    "data_de_instalacao", "quais_operadoras_tem_viabilidade",
                    TRIM("uf_crm_bairro") as uf_crm_bairro,
                    TRIM("uf_crm_cidade") as uf_crm_cidade,
                    "uf_crm_numero", "uf_crm_uf"
                FROM deals
                WHERE regexp_replace("uf_crm_cep", '[^0-9]', '', 'g') = %s;
                """,
                (cep_limpo,),
            )
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
            "id": r[0],
            "cliente": formatar_dado(r[1]),
            "fase": formatar_dado(fase_nome),
            "categoria": formatar_dado(categoria_nome),
            "uf_crm_cep": formatar_dado(r[4]),
            "contato": formatar_dado(r[5]),
            "criado_em": r[6].isoformat() if hasattr(r[6], "isoformat") else formatar_dado(r[6]),
            "contato01": formatar_dado(r[7]),
            "contato02": formatar_dado(r[8]),
            "ordem_de_servico": formatar_dado(r[9]),
            "nome_do_cliente": formatar_dado(r[10]),
            "nome_da_mae": formatar_dado(r[11]),
            "data_de_vencimento": formatar_dado(r[12]),
            "email": formatar_dado(r[13]),
            "cpf": formatar_dado(r[14]),
            "rg": formatar_dado(r[15]),
            "referencia": formatar_dado(r[16]),
            "rua": formatar_dado(r[17]),
            "data_de_instalacao": formatar_dado(r[18]),
            "quais_operadoras_tem_viabilidade": formatar_dado(r[19]),
            "uf_crm_bairro": formatar_dado(r[20]),
            "uf_crm_cidade": formatar_dado(r[21]),
            "uf_crm_numero": formatar_dado(r[22]),
            "uf_crm_uf": formatar_dado(r[23]),
        })
    return resultados

def buscar_varios_ceps(lista_ceps):
    ceps_limpos = [c.replace("-", "").strip() for c in lista_ceps if c.strip()]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    "id", "title", "stage_id", "category_id", 
                    TRIM("uf_crm_cep") as uf_crm_cep, 
                    "uf_crm_contato", "date_create", "contato01", "contato02", 
                    "ordem_de_servico", 
                    TRIM("nome_do_cliente") as nome_do_cliente,
                    "nome_da_mae", 
                    "data_de_vencimento", "email", "cpf", "rg", "referencia", 
                    TRIM("rua") as rua,
                    "data_de_instalacao", "quais_operadoras_tem_viabilidade",
                    TRIM("uf_crm_bairro") as uf_crm_bairro,
                    TRIM("uf_crm_cidade") as uf_crm_cidade,
                    "uf_crm_numero", "uf_crm_uf"
                FROM deals
                WHERE replace("uf_crm_cep", '-', '') = ANY(%s);
                """,
                (ceps_limpos,),
            )
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
            "id": r[0],
            "cliente": formatar_dado(r[1]),
            "fase": formatar_dado(fase_nome),
            "categoria": formatar_dado(categoria_nome),
            "uf_crm_cep": formatar_dado(r[4]),
            "contato": formatar_dado(r[5]),
            "criado_em": r[6].isoformat() if hasattr(r[6], "isoformat") else formatar_dado(r[6]),
            "contato01": formatar_dado(r[7]),
            "contato02": formatar_dado(r[8]),
            "ordem_de_servico": formatar_dado(r[9]),
            "nome_do_cliente": formatar_dado(r[10]),
            "nome_da_mae": formatar_dado(r[11]),
            "data_de_vencimento": formatar_dado(r[12]),
            "email": formatar_dado(r[13]),
            "cpf": formatar_dado(r[14]),
            "rg": formatar_dado(r[15]),
            "referencia": formatar_dado(r[16]),
            "rua": formatar_dado(r[17]),
            "data_de_instalacao": formatar_dado(r[18]),
            "quais_operadoras_tem_viabilidade": formatar_dado(r[19]),
            "uf_crm_bairro": formatar_dado(r[20]),
            "uf_crm_cidade": formatar_dado(r[21]),
            "uf_crm_numero": formatar_dado(r[22]),
            "uf_crm_uf": formatar_dado(r[23]),
        })
    return resultados

async def extrair_ceps_arquivo(arquivo: UploadFile):
    nome = arquivo.filename.lower()
    conteudo = await arquivo.read()
    ceps = []

    if nome.endswith(".txt"):
        ceps = conteudo.decode().splitlines()
    elif nome.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(conteudo))
        for col in df.columns:
            if "cep" in col.lower():
                ceps = df[col].astype(str).tolist()
                break
    elif nome.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(conteudo))
        for col in df.columns:
            if "cep" in col.lower():
                ceps = df[col].astype(str).tolist()
                break
    return ceps

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/buscar")
async def buscar(
    cep: str = Form(None),
    arquivo: UploadFile = File(None),
    formato: str = Form("txt"),
):
    if cep and (arquivo and arquivo.filename != ""):
        return JSONResponse(
            content={"error": "Envie apenas um CEP ou um arquivo, não ambos."},
            status_code=400,
        )

    if arquivo and arquivo.filename != "":
        ceps = await extrair_ceps_arquivo(arquivo)
        if not ceps:
            return JSONResponse(
                content={"error": "Nenhum CEP encontrado no arquivo."},
                status_code=400,
            )

        resultados = buscar_varios_ceps(ceps)
        if not resultados:
            resultados = []

        if formato == "xlsx":
            df = pd.DataFrame(resultados)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                df.to_excel(tmp.name, index=False)
                tmp.seek(0)
                return FileResponse(
                    tmp.name,
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    filename="resultado.xlsx",
                )
        else:
            output = io.StringIO()
            for res in resultados:
                output.write(str(res) + "\n")
            output.seek(0)
            headers = {"Content-Disposition": 'attachment; filename="resultado.txt"'}
            return StreamingResponse(output, media_type="text/plain", headers=headers)

    elif cep:
        resultados = buscar_por_cep(cep)
        return JSONResponse(
            content={"total": len(resultados), "resultados": resultados}
        )

    else:
        return JSONResponse(
            content={"error": "Nenhum CEP ou arquivo enviado."},
            status_code=400,
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=1433, reload=True)
