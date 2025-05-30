from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import io
import pandas as pd
import psycopg2
import os
import tempfile
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

def buscar_por_cep(cep):
    cep_limpo = cep.replace("-", "").strip()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT "id", "title", "stage_id", "category_id", "uf_crm_cep", "uf_crm_contato", "date_create"
                FROM deals
                WHERE replace("uf_crm_cep", '-', '') = %s;
            """, (cep_limpo,))
            rows = cur.fetchall()

    resultados = []
    for r in rows:
        resultados.append({
            "id": r[0],
            "cliente": r[1],
            "fase": r[2],
            "categoria": r[3],
            "cep": r[4],
            "contato": r[5],
            "criado_em": r[6].isoformat() if hasattr(r[6], 'isoformat') else str(r[6])
        })
    return resultados

def buscar_varios_ceps(lista_ceps):
    ceps_limpos = [c.replace("-", "").strip() for c in lista_ceps if c.strip()]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT "id", "title", "stage_id", "category_id", "uf_crm_cep", "uf_crm_contato", "date_create"
                FROM deals
                WHERE replace("uf_crm_cep", '-', '') = ANY(%s);
            """, (ceps_limpos,))
            rows = cur.fetchall()

    resultados = []
    for r in rows:
        resultados.append({
            "id": r[0],
            "cliente": r[1],
            "fase": r[2],
            "categoria": r[3],
            "cep": r[4],
            "contato": r[5],
            "criado_em": r[6].isoformat() if hasattr(r[6], 'isoformat') else str(r[6])
        })
    return resultados

async def extrair_ceps_arquivo(arquivo: UploadFile):
    nome = arquivo.filename.lower()
    conteudo = await arquivo.read()
    ceps = []

    if nome.endswith('.txt'):
        ceps = conteudo.decode().splitlines()
    elif nome.endswith('.csv'):
        df = pd.read_csv(io.BytesIO(conteudo))
        for col in df.columns:
            if 'cep' in col.lower():
                ceps = df[col].astype(str).tolist()
                break
    elif nome.endswith('.xlsx'):
        df = pd.read_excel(io.BytesIO(conteudo))
        for col in df.columns:
            if 'cep' in col.lower():
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
    formato: str = Form("txt")
):
    if cep and (arquivo and arquivo.filename != ""):
        return JSONResponse(content={"error": "Envie apenas um CEP ou um arquivo, não ambos."}, status_code=400)

    if arquivo and arquivo.filename != "":
        ceps = await extrair_ceps_arquivo(arquivo)
        if not ceps:
            return JSONResponse(content={"error": "Nenhum CEP encontrado no arquivo. Certifique-se de que o arquivo contém uma coluna ou linhas com CEPs."}, status_code=400)

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
                    media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    filename="resultado.xlsx"
                )
        else:
            output = io.StringIO()
            for res in resultados:
                output.write(
                    f"ID: {res['id']} | Cliente: {res['cliente']} | Fase: {res['fase']} | Categoria: {res['categoria']} | CEP: {res['cep']} | Contato: {res['contato']} | Criado em: {res['criado_em']}\n"
                )
            output.seek(0)
            return PlainTextResponse(content=output.read(), media_type='text/plain')

    elif cep:
        resultados = buscar_por_cep(cep)
        if not resultados:
            resultados = []
        return JSONResponse(content={"total": len(resultados), "resultados": resultados})

    else:
        return JSONResponse(content={"error": "Nenhum CEP ou arquivo enviado."}, status_code=400)
