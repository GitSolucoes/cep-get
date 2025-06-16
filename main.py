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

# Configuração do logger
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.DEBUG  # Aumentei para DEBUG para capturar tudo
)

load_dotenv()

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

BITRIX_API_BASE = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5"

def get_conn():
    try:
        logging.debug("Estabelecendo conexão com o banco de dados")
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        return conn
    except Exception as e:
        logging.error(f"Erro ao conectar no banco de dados: {e}")
        raise

def get_categories():
    try:
        logging.debug("Buscando categorias no Bitrix")
        resp = requests.get(f"{BITRIX_API_BASE}/crm.category.list", params={"entityTypeId": 2})
        data = resp.json()
        logging.debug(f"Categorias obtidas: {data}")
        return {cat["id"]: cat["name"] for cat in data.get("result", {}).get("categories", [])}
    except Exception as e:
        logging.error(f"Erro ao buscar categorias: {e}")
        return {}

def get_stages(category_id):
    try:
        logging.debug(f"Buscando fases para categoria {category_id}")
        resp = requests.get(f"{BITRIX_API_BASE}/crm.dealcategory.stage.list", params={"id": category_id})
        data = resp.json()
        logging.debug(f"Fases obtidas: {data}")
        return {stage["STATUS_ID"]: stage["NAME"] for stage in data.get("result", [])}
    except Exception as e:
        logging.error(f"Erro ao buscar fases: {e}")
        return {}

def buscar_por_cep(cep):
    cep_limpo = re.sub(r'\D', '', cep)
    logging.info(f"Buscando por CEP: {cep} (limpo: {cep_limpo})")
    with get_conn() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT 
                    "id", "title", "stage_id", "category_id", "uf_crm_cep", 
                    "uf_crm_contato", "date_create", "contato01", "contato02", 
                    "ordem_de_servico", "nome_do_cliente", "nome_da_mae", 
                    "data_de_vencimento", "email", "cpf", "rg", "referencia", 
                    "rua", "data_de_instalacao", "quais_operadoras_tem_viabilidade",
                    "uf_crm_bairro", "uf_crm_cidade", "uf_crm_numero", "uf_crm_uf"
                FROM deals
                WHERE regexp_replace("uf_crm_cep", '[^0-9]', '', 'g') = %s;
            """
            logging.debug(f"Query executada: {query} | Parâmetros: {cep_limpo}")
            cur.execute(query, (cep_limpo,))
            rows = cur.fetchall()
            logging.info(f"{len(rows)} registros encontrados para o CEP {cep_limpo}")

    categorias = get_categories()
    stages_cache = {}
    resultados = []

    for r in rows:
        cat_id = r[3]
        categoria_nome = categorias.get(cat_id, str(cat_id))
        if cat_id not in stages_cache:
            stages_cache[cat_id] = get_stages(cat_id)
            logging.debug(f"Fases carregadas para categoria {cat_id}: {stages_cache[cat_id]}")
        fase_nome = stages_cache[cat_id].get(r[2], r[2])

        resultado = {
            "id": r[0],
            "cliente": r[1],
            "fase": fase_nome,
            "categoria": categoria_nome,
            "uf_crm_cep": r[4],
            "contato": r[5],
            "criado_em": r[6].isoformat() if hasattr(r[6], "isoformat") else str(r[6]),
            "contato01": r[7],
            "contato02": r[8],
            "ordem_de_servico": r[9],
            "nome_do_cliente": r[10],
            "nome_da_mae": r[11],
            "data_de_vencimento": r[12],
            "email": r[13],
            "cpf": r[14],
            "rg": r[15],
            "referencia": r[16],
            "rua": r[17],
            "data_de_instalacao": r[18],
            "quais_operadoras_tem_viabilidade": r[19],
            "uf_crm_bairro": r[20],
            "uf_crm_cidade": r[21],
            "uf_crm_numero": r[22],
            "uf_crm_uf": r[23],
        }
        resultados.append(resultado)

    logging.debug(f"Resultado final do CEP: {resultados}")
    return resultados

def buscar_varios_ceps(lista_ceps):
    ceps_limpos = [re.sub(r'\D', '', c) for c in lista_ceps if c.strip()]
    logging.info(f"Buscando por vários CEPs: {ceps_limpos}")
    with get_conn() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT 
                    "id", "title", "stage_id", "category_id", "uf_crm_cep", 
                    "uf_crm_contato", "date_create", "contato01", "contato02", 
                    "ordem_de_servico", "nome_do_cliente", "nome_da_mae", 
                    "data_de_vencimento", "email", "cpf", "rg", "referencia", 
                    "rua", "data_de_instalacao", "quais_operadoras_tem_viabilidade",
                    "uf_crm_bairro", "uf_crm_cidade", "uf_crm_numero", "uf_crm_uf"
                FROM deals
                WHERE regexp_replace("uf_crm_cep", '[^0-9]', '', 'g') = ANY(%s);
            """
            logging.debug(f"Query executada: {query} | Parâmetros: {ceps_limpos}")
            cur.execute(query, (ceps_limpos,))
            rows = cur.fetchall()
            logging.info(f"{len(rows)} registros encontrados para os CEPs enviados")

    categorias = get_categories()
    stages_cache = {}
    resultados = []

    for r in rows:
        cat_id = r[3]
        categoria_nome = categorias.get(cat_id, str(cat_id))
        if cat_id not in stages_cache:
            stages_cache[cat_id] = get_stages(cat_id)
            logging.debug(f"Fases carregadas para categoria {cat_id}: {stages_cache[cat_id]}")
        fase_nome = stages_cache[cat_id].get(r[2], r[2])

        resultado = {
            "id": r[0],
            "cliente": r[1],
            "fase": fase_nome,
            "categoria": categoria_nome,
            "uf_crm_cep": r[4],
            "contato": r[5],
            "criado_em": r[6].isoformat() if hasattr(r[6], "isoformat") else str(r[6]),
            "contato01": r[7],
            "contato02": r[8],
            "ordem_de_servico": r[9],
            "nome_do_cliente": r[10],
            "nome_da_mae": r[11],
            "data_de_vencimento": r[12],
            "email": r[13],
            "cpf": r[14],
            "rg": r[15],
            "referencia": r[16],
            "rua": r[17],
            "data_de_instalacao": r[18],
            "quais_operadoras_tem_viabilidade": r[19],
            "uf_crm_bairro": r[20],
            "uf_crm_cidade": r[21],
            "uf_crm_numero": r[22],
            "uf_crm_uf": r[23],
        }
        resultados.append(resultado)

    logging.debug(f"Resultado final dos vários CEPs: {resultados}")
    return resultados

async def extrair_ceps_arquivo(arquivo: UploadFile):
    logging.info(f"Processando arquivo: {arquivo.filename}")
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

    logging.info(f"CEPs extraídos do arquivo: {ceps}")
    return ceps

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    logging.info("Página inicial acessada")
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/buscar")
async def buscar(cep: str = Form(None), arquivo: UploadFile = File(None), formato: str = Form("txt")):
    try:
        logging.info(f"Requisição recebida - CEP: {cep}, Arquivo: {arquivo.filename if arquivo else 'Nenhum'}")
        # [restante do código do buscar igual ao seu, já estava bom]
        # (mantém os logs de geração dos arquivos e retorno como no seu)
    except Exception as e:
        logging.error(f"Erro inesperado na rota /buscar: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=1433, reload=True)
