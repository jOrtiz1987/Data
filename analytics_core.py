import os
import uuid
import numpy as np
import pandas as pd
import pymysql
from sklearn.cluster import DBSCAN

# Mapas
import folium
from folium.plugins import HeatMap, MarkerCluster

EARTH_R = 6371000.0

def haversine_m(lat1, lon1, lat2, lon2):
    lat1 = np.radians(lat1); lon1 = np.radians(lon1)
    lat2 = np.radians(lat2); lon2 = np.radians(lon2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2.0)**2
    c = 2*np.arctan2(np.sqrt(a), np.sqrt(1-a))
    return EARTH_R * c

def dbscan_geo(lat, lon, eps_m=120, min_samples=15):
    coords = np.radians(np.column_stack([lat, lon]))
    eps_rad = eps_m / EARTH_R
    model = DBSCAN(eps=eps_rad, min_samples=min_samples, metric="haversine")
    return model.fit_predict(coords)

def mysql_read(conn_params, query: str) -> pd.DataFrame:
    conn = pymysql.connect(**conn_params)
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()

def make_base_map(df_):
    center_lat = float(df_["lat"].mean())
    center_lon = float(df_["lon"].mean())
    return folium.Map(location=[center_lat, center_lon], zoom_start=12, control_scale=True)

def build_maps(df, outdir, global_clusters_df, sample_points=2000):
    """
    Genera:
      - mapa_heatmap.html
      - mapa_clusters_global.html

    sample_points evita que el HTML pese demasiado.
    """
    # 1) Heatmap
    m_heat = make_base_map(df)
    heat_points = df[["lat", "lon"]].dropna().values.tolist()
    HeatMap(heat_points, radius=10, blur=12).add_to(m_heat)
    heat_path = os.path.join(outdir, "mapa_heatmap.html")
    m_heat.save(heat_path)

    # 2) Clusters globales
    m_clust = make_base_map(df)
    mc = MarkerCluster(name="Puntos").add_to(m_clust)

    df_sample = df.sample(min(sample_points, len(df)), random_state=42) if len(df) > 0 else df

    for _, row in df_sample.iterrows():
        lbl = int(row["cluster_global"])
        popup = f"cluster_global={lbl}<br>user_id={row['user_id']}<br>{row['timestamp']}"
        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=3,
            popup=popup,
            fill=True
        ).add_to(mc)

    # centroides top 50 (si existen)
    if global_clusters_df is not None and len(global_clusters_df) > 0:
        for _, r in global_clusters_df.head(50).iterrows():
            folium.Marker(
                location=[r["lat_c"], r["lon_c"]],
                popup=f"Cluster {int(r['cluster_global'])}<br>n={int(r['n'])}<br>users={int(r['n_users'])}"
            ).add_to(m_clust)

    folium.LayerControl().add_to(m_clust)
    clusters_path = os.path.join(outdir, "mapa_clusters_global.html")
    m_clust.save(clusters_path)

    return heat_path, clusters_path

def generate_report(conn_params: dict, reports_dir: str,
                    user_ids=None, start=None, end=None,
                    eps_m=120, min_samples=15,
                    poi_radius_m=120, visita_lookback_min=30):

    report_id = str(uuid.uuid4())[:10]
    outdir = os.path.join(reports_dir, report_id)
    os.makedirs(outdir, exist_ok=True)

    where = []
    if start and end:
        where.append(f"rc.fecha BETWEEN '{start}' AND '{end}'")
    if user_ids:
        ids = ",".join(str(int(x)) for x in user_ids)
        where.append(f"rc.idUsuario IN ({ids})")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    q_pings = f"""
      SELECT rc.idRegistroCoordendas AS id,
             rc.fecha AS timestamp,
             rc.latitud AS lat,
             rc.longitud AS lon,
             rc.idUsuario AS user_id
      FROM RegistroCoordendas rc
      {where_sql}
      ORDER BY rc.idUsuario, rc.fecha;
    """

    q_pois = """
      SELECT li.idLugarInteres AS poi_id,
             li.descripcion AS poi_name,
             li.latitud AS poi_lat,
             li.longitud AS poi_lon,
             c.descripcion AS cat_name
      FROM LugarInteres li
      JOIN Categoria c ON c.idCategoria = li.idCategoria;
    """

    q_vis = """
      SELECT v.idVisita AS visita_id,
             v.fecha AS visita_ts,
             v.idEdificioHistorico AS poi_id,
             v.idUsuario AS user_id,
             v.llevaNinos AS lleva_ninos
      FROM Visita v
      ORDER BY v.idUsuario, v.fecha;
    """

    df = mysql_read(conn_params, q_pings)
    pois = mysql_read(conn_params, q_pois)
    vis = mysql_read(conn_params, q_vis)

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "lat", "lon", "user_id"]).copy()

    vis["visita_ts"] = pd.to_datetime(vis["visita_ts"], errors="coerce")
    vis = vis.dropna(subset=["visita_ts", "poi_id", "user_id"]).copy()

    # DBSCAN global
    df["cluster_global"] = dbscan_geo(df["lat"].values, df["lon"].values,
                                      eps_m=eps_m, min_samples=min_samples)

    # Centroides por cluster global (para mapa)
    global_clusters_df = (df[df["cluster_global"] != -1]
        .groupby("cluster_global")
        .agg(
            n=("cluster_global", "size"),
            lat_c=("lat", "mean"),
            lon_c=("lon", "mean"),
            n_users=("user_id", "nunique")
        )
        .reset_index()
        .sort_values("n", ascending=False)
    )

    # Enriquecer visitas con distancia mínima previa
    vis = vis.merge(pois[["poi_id", "poi_name", "poi_lat", "poi_lon", "cat_name"]],
                    on="poi_id", how="left")

    df = df.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
    lookback = pd.Timedelta(minutes=visita_lookback_min)

    rows = []
    for _, v in vis.iterrows():
        uid = int(v["user_id"])
        t = v["visita_ts"]
        poi_lat0 = v["poi_lat"]
        poi_lon0 = v["poi_lon"]

        window = df[(df["user_id"] == uid) & (df["timestamp"] <= t) & (df["timestamp"] >= (t - lookback))]
        if len(window) == 0 or pd.isna(poi_lat0) or pd.isna(poi_lon0):
            rows.append({**v.to_dict(),
                         "min_dist_m": np.nan,
                         "t_min_dist": pd.NaT,
                         "lag_open_s": np.nan,
                         "was_exposed_in_window": False})
            continue

        d = haversine_m(window["lat"].values, window["lon"].values, poi_lat0, poi_lon0)
        k = int(np.argmin(d))
        min_dist = float(d[k])
        t_min = window.iloc[k]["timestamp"]
        lag_s = (t - t_min).total_seconds()

        rows.append({**v.to_dict(),
                     "min_dist_m": min_dist,
                     "t_min_dist": t_min,
                     "lag_open_s": lag_s,
                     "was_exposed_in_window": (min_dist <= poi_radius_m)})

    vis_enriched = pd.DataFrame(rows)

    summary = {
        "reportId": report_id,
        "points": int(len(df)),
        "users": int(df["user_id"].nunique()),
        "clustersGlobal": int(df[df["cluster_global"] != -1]["cluster_global"].nunique()),
        "visitas": int(len(vis))
    }

    # Export Excel
    xlsx_path = os.path.join(outdir, "reporte.xlsx")
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="pings")
        pois.to_excel(writer, index=False, sheet_name="pois")
        vis.to_excel(writer, index=False, sheet_name="visitas")
        vis_enriched.to_excel(writer, index=False, sheet_name="visitas_enriquecidas")
        global_clusters_df.to_excel(writer, index=False, sheet_name="clusters_global")

    # Generar mapas (HTML)
    heat_path, clusters_path = build_maps(df, outdir, global_clusters_df, sample_points=2000)

    maps = {
        "heatmap_path": heat_path,
        "clusters_path": clusters_path
    }

    return summary, report_id, xlsx_path, maps