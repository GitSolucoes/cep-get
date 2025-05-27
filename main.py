from fastapi import FastAPI, Request, Form, UploadFile, File, Query
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import json
import io
import pandas as pd
import os
import requests

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

CACHE_PATH = "cache.json"

BITRIX_URL = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/"

# Cache dinâmico
CACHE_DINAMICO = {
    "pipelines": {},
    "etapas": {},
    "campos": {}
}

# ---------------------- Bitrix API ----------------------

def bitrix_get(method, params=None):
    url = f"{BITRIX_URL}{method}"
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def get_pipelines():
    params = {"entityTypeId": 2}  # Para deals
    data = bitrix_get("crm.category.list", params)
    return {str(item["ID"]): item["NAME"] for item in data.get("result", [])}

def get_etapas():
    etapas = {}
    params = {"entityTypeId": 2}
    categorias = bitrix_get("crm.category.list", params).get("result", [])
    
    for cat in categorias:
        categoria_id = cat["ID"]
        entity_id = f"DEAL_STAGE_{categoria_id}"
        status_params = {"filter[ENTITY_ID]": entity_id}
        data = bitrix_get("crm.status.list", status_params)
        
        for stage in data.get("result", []):
            etapas[stage["STATUS_ID"]] = stage["NAME"]
    
    return etapas
    

def get_campos_personalizados():
    data = bitrix_get("crm.deal.fields")
    result = data.get("result", {})
    return {k: v.get("title", k) for k, v in result.items() if k.startswith("UF_CRM")}

def atualizar_cache_dinamico():
    print("🔄 Atualizando cache dinâmico do Bitrix...")
    CACHE_DINAMICO["pipelines"] = get_pipelines()
    CACHE_DINAMICO["etapas"] = get_etapas()
    CACHE_DINAMICO["campos"] = get_campos_personalizados()
    print("✅ Cache dinâmico atualizado.")

# ---------------------- Utilitários ----------------------

def get_pipeline_nome(stage_id):
    pipeline_code = stage_id.split(":")[0] if ":" in stage_id else stage_id
    return CACHE_DINAMICO["pipelines"].get(pipeline_code, pipeline_code)

def get_etapa_nome(stage_id):
    return CACHE_DINAMICO["etapas"].get(stage_id, stage_id)

def get_campo_legivel(nome_campo):
    return CACHE_DINAMICO["campos"].get(nome_campo, nome_campo)

def carregar_cache():
    if not os.path.exists(CACHE_PATH):
        return []
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        print("⚠️ ERRO: cache.json inválido ou corrompido.")
        return []

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

def formatar_card(deal):
    stage_id = deal.get("STAGE_ID", "")
    pipeline_nome = get_pipeline_nome(stage_id)
    etapa_nome = get_etapa_nome(stage_id)

    card_formatado = {
        "id_card": deal.get("ID"),
        "cliente": deal.get("TITLE"),
        "pipeline": pipeline_nome,
        "etapa": etapa_nome,
        "campos_preenchidos": {}
    }

    for campo, valor in deal.items():
        if campo.startswith("UF_CRM") and valor:
            nome_legivel = get_campo_legivel(campo)
            card_formatado["campos_preenchidos"][nome_legivel] = valor

    return card_formatado

def buscar_varios_ceps(lista_ceps):
    ceps_set = set(c.strip().replace("-", "") for c in lista_ceps if c.strip())
    dados = carregar_cache()
    resultados = []

    for deal in dados:
        c = (deal.get("UF_CRM_1700661314351") or "").replace("-", "").strip()
        if c in ceps_set:
            resultados.append(formatar_card(deal))

    return resultados

def buscar_cep_unico(cep):
    cep = cep.replace("-", "").strip()
    dados = carregar_cache()
    resultados = []

    for deal in dados:
        c = (deal.get("UF_CRM_1700661314351") or "").replace("-", "").strip()
        if c == cep:
            resultados.append(formatar_card(deal))

    return resultados

# ---------------------- Rotas ----------------------

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
        resultados = buscar_varios_ceps(ceps)

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
                output.write(f"ID: {res['id_card']} | Cliente: {res['cliente']} | Pipeline: {res['pipeline']} | Etapa: {res['etapa']} | Campos: {json.dumps(res['campos_preenchidos'])}\n")
            output.seek(0)
            return PlainTextResponse(content=output.read(), media_type='text/plain')

    elif cep:
        resultados = buscar_cep_unico(cep)
        return JSONResponse(content={"total": len(resultados), "resultados": resultados})

    else:
        return JSONResponse(content={"error": "Nenhum CEP ou arquivo enviado."})

@app.get("/consultar-etapa")
async def consultar_etapa(pipeline: str = Query(...), etapa: str = Query(...)):
    dados = carregar_cache()
    resultados = []

    for deal in dados:
        stage_id = deal.get("STAGE_ID", "")
        if get_pipeline_nome(stage_id).lower() == pipeline.lower() and get_etapa_nome(stage_id).lower() == etapa.lower():
            resultados.append(formatar_card(deal))

    return JSONResponse(content={"total": len(resultados), "resultados": resultados})

@app.get("/relatorio-campos")
async def relatorio_campos():
    dados = carregar_cache()
    relatorio = []

    for deal in dados:
        card = formatar_card(deal)
        if card["campos_preenchidos"]:
            relatorio.append(card)

    return JSONResponse(content={"total": len(relatorio), "relatorio": relatorio})

@app.post("/atualizar-cache-dinamico")
async def atualizar_cache_endpoint():
    atualizar_cache_dinamico()
    return JSONResponse(content={"status": "Cache dinâmico atualizado."})

# ---------------------- Inicialização ----------------------

atualizar_cache_dinamico()
