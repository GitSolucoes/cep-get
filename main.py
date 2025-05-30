from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import io
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

# Carrega .env
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
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT "title", "stage_id", "uf_crm_cep", "uf_crm_contato", "date_create"
        FROM deals
        WHERE "uf_crm_cep" = %s;
    """, (cep.replace("-", "").strip(),))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    resultados = []
    for r in rows:
        resultados.append({
            "cliente": r[0],
            "fase": r[1],
            "cep": r[2],
            "contato": r[3],
            "criado_em": r[4].isoformat() if hasattr(r[4], 'isoformat') else str(r[4])
        })
    return resultados


def buscar_varios_ceps(lista_ceps):
    ceps_limpos = [c.replace("-", "").strip() for c in lista_ceps if c.strip()]
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT "title", "stage_id", "uf_crm_cep", "uf_crm_contato", "date_create"
        FROM deals
        WHERE "uf_crm_cep" = ANY(%s);
    """, (ceps_limpos,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    resultados = []
    for r in rows:
        resultados.append({
            "cliente": r[0],
            "fase": r[1],
            "cep": r[2],
            "contato": r[3],
            "criado_em": r[4].isoformat() if hasattr(r[4], 'isoformat') else str(r[4])
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
    if arquivo and arquivo.filename != "":
        ceps = await extrair_ceps_arquivo(arquivo)
        if not ceps:
            return JSONResponse(content={"error": "Nenhum CEP encontrado no arquivo."}, status_code=400)

        resultados = buscar_varios_ceps(ceps)
        if resultados is None:
            resultados = []

        if formato == "xlsx":
            df = pd.DataFrame(resultados)
            caminho_saida = "resultado.xlsx"
            df.to_excel(caminho_saida, index=False)
            return FileResponse(
                caminho_saida,
                media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                filename="resultado.xlsx"
            )
        else:
            output = io.StringIO()
            for res in resultados:
                output.write(
                    f"Cliente: {res['cliente']} | Fase: {res['fase']} | CEP: {res['cep']} | Contato: {res['contato']} | Criado em: {res['criado_em']}\n"
                )
            output.seek(0)
            return PlainTextResponse(content=output.read(), media_type='text/plain')

    elif cep:
        resultados = buscar_por_cep(cep)
        if resultados is None:
            resultados = []
        return JSONResponse(content={"total": len(resultados), "resultados": resultados})

    else:
        return JSONResponse(content={"error": "Nenhum CEP ou arquivo enviado."}, status_code=400)
