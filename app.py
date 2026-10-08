import os
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional, List
from analytics_core import (
    generate_report, 
    train_recommendation_model, 
    predict_next_monument,
    train_profile_model,
    predict_tourist_profile,
    train_budget_model,
    predict_tourist_budget
)

# el root_path se usa para Nginx en produccion
app = FastAPI(root_path="/apiiaturistica")

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Data API is running"}

API_KEY = os.getenv("ANALYTICS_API_KEY", "dev-key")
REPORTS_DIR = os.path.abspath(os.getenv("REPORTS_DIR", "./reports"))

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB", "mydb")
MYSQL_USER = os.getenv("MYSQL_USER", "jorge")
MYSQL_PASS = os.getenv("MYSQL_PASS", "ChamaPel0n")

conn_params = dict(
    host=MYSQL_HOST, port=MYSQL_PORT,
    user=MYSQL_USER, password=MYSQL_PASS,
    database=MYSQL_DB, charset="utf8mb4"
)

class ReportRequest(BaseModel):
    userIds: Optional[List[int]] = None
    start: Optional[str] = None
    end: Optional[str] = None
    epsM: int = 120
    minSamples: int = 15
    poiRadiusM: int = 120
    visitaLookbackMin: int = 30

def _auth(x_api_key: str):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.post("/reports/generate")
def generate(req: ReportRequest, x_api_key: str = Header(default="")):
    _auth(x_api_key)

    summary, report_id, xlsx_path, maps = generate_report(
        conn_params=conn_params,
        reports_dir=REPORTS_DIR,
        user_ids=req.userIds,
        start=req.start,
        end=req.end,
        eps_m=req.epsM,
        min_samples=req.minSamples,
        poi_radius_m=req.poiRadiusM,
        visita_lookback_min=req.visitaLookbackMin
    )

    # Mantengo tu campo "download" original para Excel (compatibilidad)
    return {
        "status": "DONE",
        "summary": summary,
        "download": f"/reports/{report_id}/excel",
        "downloads": {
            "excel": f"/reports/{report_id}/excel",
            "heatmap": f"/reports/{report_id}/heatmap",
            "clusters": f"/reports/{report_id}/clusters"
        }
    }

@app.get("/reports/{report_id}/excel")
def download_excel(report_id: str, x_api_key: str = Header(default="")):
    _auth(x_api_key)
    path = os.path.join(REPORTS_DIR, report_id, "reporte.xlsx")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(path, filename="reporte.xlsx")

@app.get("/reports/{report_id}/heatmap")
def download_heatmap(report_id: str, x_api_key: str = Header(default="")):
    _auth(x_api_key)
    path = os.path.join(REPORTS_DIR, report_id, "mapa_heatmap.html")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(path, filename="mapa_heatmap.html", media_type="text/html")

@app.get("/reports/{report_id}/clusters")
def download_clusters(report_id: str, x_api_key: str = Header(default="")):
    _auth(x_api_key)
    path = os.path.join(REPORTS_DIR, report_id, "mapa_clusters_global.html")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(path, filename="mapa_clusters_global.html", media_type="text/html")

@app.post("/recommend/train")
def train_model(x_api_key: str = Header(default="")):
    _auth(x_api_key)
    res = train_recommendation_model(conn_params)
    return {"status": "SUCCESS", "detail": res}

class PredictRequest(BaseModel):
    userId: int
    currentPoiId: int

@app.post("/recommend/next")
def predict_next(req: PredictRequest, x_api_key: str = Header(default="")):
    _auth(x_api_key)
    res = predict_next_monument(conn_params, req.userId, req.currentPoiId)
    return res

class ProfileRequest(BaseModel):
    userId: int

@app.post("/recommend/profile/train")
def train_profile(x_api_key: str = Header(default="")):
    _auth(x_api_key)
    res = train_profile_model(conn_params)
    return {"status": "SUCCESS", "detail": res}

@app.post("/recommend/profile")
def predict_profile(req: ProfileRequest, x_api_key: str = Header(default="")):
    _auth(x_api_key)
    res = predict_tourist_profile(conn_params, req.userId)
    return res

class BudgetRequest(BaseModel):
    userId: int

@app.post("/recommend/budget/train")
def train_budget(x_api_key: str = Header(default="")):
    _auth(x_api_key)
    res = train_budget_model(conn_params)
    return {"status": "SUCCESS", "detail": res}

@app.post("/recommend/budget")
def predict_budget(req: BudgetRequest, x_api_key: str = Header(default="")):
    _auth(x_api_key)
    res = predict_tourist_budget(conn_params, req.userId)
    return res