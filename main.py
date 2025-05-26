from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import requests

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")

BASE_URL = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.deal.list"

PARAMS = {
    "select[]": ["ID", "TITLE", "STAGE_ID", "UF_CRM_1700661314351", "DATE_CREATE"],
    "filter[>=DATE_CREATE]": "2023-11-01",
    "start": 0
}

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/buscar", response_class=JSONResponse)
async def buscar(cep: str = Form(...)):
    cep = cep.replace("-", "").strip()
    resultados = []
    total_processado = 0
    local_params = PARAMS.copy()

    while True:
        try:
            response = requests.get(BASE_URL, params=local_params, timeout=10)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            return {"error": f"Erro na requisição: {e}"}

        if 'result' not in data:
            break

        deals = data['result']
        if not deals:
            break

        for deal in deals:
            c = deal.get("UF_CRM_1700661314351", "").replace("-", "").strip()
            if c == cep:
                resultado = {
                    "id_card": deal['ID'],
                    "cliente": deal['TITLE'],
                    "fase": deal['STAGE_ID'],
                    "cep": c,
                    "criado_em": deal.get("DATE_CREATE")
                }
                resultados.append(resultado)

        total_processado += len(deals)

        if 'next' in data and data['next']:
            local_params['start'] = data['next']
        else:
            break

    return {"total": len(resultados), "resultados": resultados}
