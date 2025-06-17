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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ... todas as funções anteriores mantidas (omitidas aqui para foco nos novos endpoints) ...

async def extrair_valores_arquivo(arquivo: UploadFile):
    nome = arquivo.filename.lower()
    conteudo = await arquivo.read()
    valores = []

    if nome.endswith(".txt"):
        valores = conteudo.decode().splitlines()
    elif nome.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(conteudo))
        for col in df.columns:
            valores = df[col].astype(str).tolist()
            break
    elif nome.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(conteudo))
        for col in df.columns:
            valores = df[col].astype(str).tolist()
            break
    return valores

def gerar_arquivo_resultado(resultados, formato):
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
        return JSONResponse({"total": len(resultados), "resultados": resultados})

@app.post("/buscar-rua")
async def buscar_rua(rua: str = Form(None), arquivo: UploadFile = File(None), formato: str = Form("txt")):
    if rua and (arquivo and arquivo.filename):
        return JSONResponse({"error": "Envie apenas uma rua ou um arquivo."}, status_code=400)

    if arquivo and arquivo.filename:
        valores = await extrair_valores_arquivo(arquivo)
        resultados = []
        for v in valores:
            resultados.extend(buscar_por_rua(v))
    elif rua:
        resultados = buscar_por_rua(rua)
    else:
        return JSONResponse({"error": "Nenhuma rua ou arquivo enviado."}, status_code=400)

    return gerar_arquivo_resultado(resultados, formato)

@app.post("/buscar-bairro")
async def buscar_bairro(bairro: str = Form(None), arquivo: UploadFile = File(None), formato: str = Form("txt")):
    if bairro and (arquivo and arquivo.filename):
        return JSONResponse({"error": "Envie apenas um bairro ou um arquivo."}, status_code=400)

    if arquivo and arquivo.filename:
        valores = await extrair_valores_arquivo(arquivo)
        resultados = []
        for v in valores:
            resultados.extend(buscar_por_bairro(v))
    elif bairro:
        resultados = buscar_por_bairro(bairro)
    else:
        return JSONResponse({"error": "Nenhum bairro ou arquivo enviado."}, status_code=400)

    return gerar_arquivo_resultado(resultados, formato)

@app.post("/buscar-cidade")
async def buscar_cidade(cidade: str = Form(None), arquivo: UploadFile = File(None), formato: str = Form("txt")):
    if cidade and (arquivo and arquivo.filename):
        return JSONResponse({"error": "Envie apenas uma cidade ou um arquivo."}, status_code=400)

    if arquivo and arquivo.filename:
        valores = await extrair_valores_arquivo(arquivo)
        resultados = []
        for v in valores:
            resultados.extend(buscar_por_cidade(v))
    elif cidade:
        resultados = buscar_por_cidade(cidade)
    else:
        return JSONResponse({"error": "Nenhuma cidade ou arquivo enviado."}, status_code=400)

    return gerar_arquivo_resultado(resultados, formato)

@app.post("/buscar-estado")
async def buscar_estado(estado: str = Form(None), arquivo: UploadFile = File(None), formato: str = Form("txt")):
    if estado and (arquivo and arquivo.filename):
        return JSONResponse({"error": "Envie apenas um estado ou um arquivo."}, status_code=400)

    if arquivo and arquivo.filename:
        valores = await extrair_valores_arquivo(arquivo)
        resultados = []
        for v in valores:
            resultados.extend(buscar_por_estado(v))
    elif estado:
        resultados = buscar_por_estado(estado)
    else:
        return JSONResponse({"error": "Nenhum estado ou arquivo enviado."}, status_code=400)

    return gerar_arquivo_resultado(resultados, formato)
