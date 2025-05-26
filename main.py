from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import json
import io
import os

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

CACHE_PATH = "cache.json"

# Função protegida contra erros de JSON corrompido
def carregar_cache():
    if not os.path.exists(CACHE_PATH):
        return []
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        print("⚠️ ERRO: cache.json inválido ou corrompido.")
        return []

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Busca por CEP único (retorna todos os cards com o mesmo CEP)
def buscar_cep_unico(cep):
    cep = cep.replace("-", "").strip()
    dados = carregar_cache()
    resultados = []

    for deal in dados:
        c = (deal.get("UF_CRM_1700661314351") or "").replace("-", "").strip()
        if c == cep:
            resultados.append({
                "id_card": deal.get("ID"),
                "cliente": deal.get("TITLE"),
                "fase": deal.get("STAGE_ID"),
                "cep": c,
                "criado_em": deal.get("DATE_CREATE")
            })

    return resultados

# Busca vários CEPs de uma vez (para upload de arquivo)
def buscar_varios_ceps(lista_ceps):
    ceps_set = set(c.strip().replace("-", "") for c in lista_ceps if c.strip())
    dados = carregar_cache()
    resultados = []

    for deal in dados:
        c = (deal.get("UF_CRM_1700661314351") or "").replace("-", "").strip()
        if c in ceps_set:
            resultados.append({
                "id_card": deal.get("ID"),
                "cliente": deal.get("TITLE"),
                "fase": deal.get("STAGE_ID"),
                "cep": c,
                "criado_em": deal.get("DATE_CREATE")
            })

    return resultados

# Rota principal de busca
@app.post("/buscar")
async def buscar(cep: str = Form(None), arquivo: UploadFile = File(None)):
    if arquivo and arquivo.filename != "":
        conteudo = await arquivo.read()
        ceps = conteudo.decode().splitlines()

        resultados = buscar_varios_ceps(ceps)

        output = io.StringIO()
        for res in resultados:
            if "error" in res:
                output.write(f"Erro: {res['error']}\n")
            else:
                output.write(f"ID: {res['id_card']} | Cliente: {res['cliente']} | Fase: {res['fase']} | CEP: {res['cep']} | Criado em: {res['criado_em']}\n")
        output.seek(0)
        return PlainTextResponse(content=output.read(), media_type='text/plain')

    elif cep:
        resultados = buscar_cep_unico(cep)
        return JSONResponse(content={"total": len(resultados), "resultados": resultados})

    else:
        return JSONResponse(content={"error": "Nenhum CEP ou arquivo enviado."})
