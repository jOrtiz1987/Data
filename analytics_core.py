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

def build_maps(df, outdir, global_clusters_df, sample_points=500):
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

    lats = df_sample["lat"].values
    lons = df_sample["lon"].values
    lbls = df_sample["cluster_global"].values.astype(int)
    uids = df_sample["user_id"].values
    tss = df_sample["timestamp"].astype(str).values

    for i in range(len(df_sample)):
        popup = f"cluster_global={lbls[i]}<br>user_id={uids[i]}<br>{tss[i]}"
        folium.CircleMarker(
            location=[lats[i], lons[i]],
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

    # Agrupar pings por usuario en memoria para búsquedas binarias ultra-rápidas
    df_by_user = {}
    for uid, group in df.groupby("user_id"):
        df_by_user[uid] = {
            "timestamps": group["timestamp"].values,
            "lats": group["lat"].values,
            "lons": group["lon"].values
        }

    rows = []
    for _, v in vis.iterrows():
        uid = int(v["user_id"])
        t = v["visita_ts"]
        poi_lat0 = v["poi_lat"]
        poi_lon0 = v["poi_lon"]

        user_pings = df_by_user.get(uid)
        if not user_pings or pd.isna(poi_lat0) or pd.isna(poi_lon0):
            rows.append({**v.to_dict(),
                         "min_dist_m": np.nan,
                         "t_min_dist": pd.NaT,
                         "lag_open_s": np.nan,
                         "was_exposed_in_window": False})
            continue

        times = user_pings["timestamps"]
        t_end = pd.Timestamp(t)
        t_start = t_end - lookback

        idx_start = np.searchsorted(times, t_start)
        idx_end = np.searchsorted(times, t_end, side='right')

        if idx_start >= len(times) or idx_start == idx_end:
            rows.append({**v.to_dict(),
                         "min_dist_m": np.nan,
                         "t_min_dist": pd.NaT,
                         "lag_open_s": np.nan,
                         "was_exposed_in_window": False})
            continue

        win_lats = user_pings["lats"][idx_start:idx_end]
        win_lons = user_pings["lons"][idx_start:idx_end]
        win_times = user_pings["timestamps"][idx_start:idx_end]

        d = haversine_m(win_lats, win_lons, poi_lat0, poi_lon0)
        k = int(np.argmin(d))
        min_dist = float(d[k])
        t_min = pd.Timestamp(win_times[k])
        lag_s = (t_end - t_min).total_seconds()

        rows.append({**v.to_dict(),
                     "min_dist_m": min_dist,
                     "t_min_dist": t_min,
                     "lag_open_s": lag_s,
                     "was_exposed_in_window": (min_dist <= poi_radius_m)})

    vis_enriched = pd.DataFrame(rows)

    # ==========================================
    # 1. CÁLCULO DE TIEMPOS DE ESTANCIA
    # ==========================================
    stay_durations = []
    max_gap = pd.Timedelta(minutes=30)
    for _, v in vis_enriched.iterrows():
        uid = int(v["user_id"])
        t = v["visita_ts"]
        poi_lat0 = v["poi_lat"]
        poi_lon0 = v["poi_lon"]
        
        if pd.isna(poi_lat0) or pd.isna(poi_lon0):
            stay_durations.append(0.0)
            continue
            
        user_pings = df_by_user.get(uid)
        if not user_pings:
            stay_durations.append(0.0)
            continue
            
        times = user_pings["timestamps"]
        t_start = pd.Timestamp(t)
        t_end = t_start + pd.Timedelta(hours=3)
        
        idx_start = np.searchsorted(times, t_start)
        idx_end = np.searchsorted(times, t_end)
        
        if idx_start >= len(times) or idx_start == idx_end:
            stay_durations.append(0.0)
            continue
            
        future_times = times[idx_start:idx_end]
        future_lats = user_pings["lats"][idx_start:idx_end]
        future_lons = user_pings["lons"][idx_start:idx_end]
        
        dists = haversine_m(future_lats, future_lons, poi_lat0, poi_lon0)
        
        stay_pings = []
        last_t = t_start
        for i in range(len(dists)):
            if dists[i] <= poi_radius_m:
                curr_t = pd.Timestamp(future_times[i])
                if (curr_t - last_t) <= max_gap:
                    stay_pings.append(curr_t)
                    last_t = curr_t
                else:
                    break
            else:
                break
                
        if len(stay_pings) > 0:
            duration_min = (stay_pings[-1] - t_start).total_seconds() / 60.0
            stay_durations.append(round(duration_min, 1))
        else:
            stay_durations.append(0.0)
            
    vis_enriched["duracion_estancia_min"] = stay_durations
    
    # Resumen promedio de estancia por POI
    stay_summary = (vis_enriched.groupby(["poi_id", "poi_name"])
                    .agg(
                        total_visitas=("visita_id", "size"),
                        duracion_promedio_min=("duracion_estancia_min", "mean"),
                        duracion_maxima_min=("duracion_estancia_min", "max")
                    )
                    .reset_index())
    stay_summary["duracion_promedio_min"] = stay_summary["duracion_promedio_min"].round(1)

    # ==========================================
    # 2. ANÁLISIS DE SECUENCIAS DE RUTA Y MATRIZ
    # ==========================================
    user_routes = []
    transitions = []
    
    vis_sorted = vis_enriched.sort_values(["user_id", "visita_ts"])
    for uid, group in vis_sorted.groupby("user_id"):
        route_list = group["poi_name"].tolist()
        route_str = " -> ".join(route_list)
        user_routes.append({
            "user_id": uid,
            "cant_paradas": len(route_list),
            "ruta_completa": route_str
        })
        
        for idx in range(len(route_list) - 1):
            transitions.append({
                "origen": route_list[idx],
                "destino": route_list[idx + 1]
            })
            
    df_user_routes = pd.DataFrame(user_routes)
    df_transitions = pd.DataFrame(transitions)
    
    # A) Rutas completas más comunes
    top_routes = pd.DataFrame(columns=["ruta_completa", "cantidad"])
    if len(df_user_routes) > 0:
        top_routes = (df_user_routes.groupby("ruta_completa")
                      .size()
                      .reset_index(name="cantidad")
                      .sort_values("cantidad", ascending=False)
                      .head(15))
                  
    # B) Bigramas de transición más comunes
    top_transitions = pd.DataFrame(columns=["origen", "destino", "cantidad"])
    if len(df_transitions) > 0:
        top_transitions = (df_transitions.groupby(["origen", "destino"])
                           .size()
                           .reset_index(name="cantidad")
                           .sort_values("cantidad", ascending=False)
                           .head(15))
        
    # C) Matriz de Transición Origen-Destino
    poi_names = sorted(pois["poi_name"].unique())
    transition_matrix = pd.DataFrame(0, index=poi_names, columns=poi_names)
    
    if len(df_transitions) > 0:
        for _, row in df_transitions.iterrows():
            orig = row["origen"]
            dest = row["destino"]
            if orig in transition_matrix.index and dest in transition_matrix.columns:
                transition_matrix.at[orig, dest] += 1

    summary = {
        "reportId": report_id,
        "points": int(len(df)),
        "users": int(df["user_id"].nunique()),
        "clustersGlobal": int(df[df["cluster_global"] != -1]["cluster_global"].nunique()),
        "visitas": int(len(vis)),
        "resumenEstancia": stay_summary[["poi_name", "total_visitas", "duracion_promedio_min"]].sort_values("duracion_promedio_min", ascending=False).to_dict(orient="records"),
        "rutasPopulares": top_routes.to_dict(orient="records"),
        "transicionesPopulares": top_transitions.to_dict(orient="records")
    }

    # Export Excel
    xlsx_path = os.path.join(outdir, "reporte.xlsx")
    # Para optimizar el tiempo de generación y evitar bloqueos,
    # si hay más de 3000 pings se exporta 1 de cada 5 (downsampling).
    df_pings_excel = df.iloc[::5] if len(df) > 3000 else df
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df_pings_excel.to_excel(writer, index=False, sheet_name="pings")
        pois.to_excel(writer, index=False, sheet_name="pois")
        vis.to_excel(writer, index=False, sheet_name="visitas")
        vis_enriched.to_excel(writer, index=False, sheet_name="visitas_enriquecidas")
        global_clusters_df.to_excel(writer, index=False, sheet_name="clusters_global")
        
        # Nuevas hojas
        # Tiempos de Estancia
        stay_summary.to_excel(writer, index=False, sheet_name="resumen_estancia")
        
        # Secuencias de Ruta
        top_routes.to_excel(writer, index=False, sheet_name="rutas_populares")
        top_transitions.to_excel(writer, index=False, sheet_name="transiciones_populares")
        transition_matrix.to_excel(writer, index=True, sheet_name="matriz_transicion")

    # Generar mapas (HTML)
    heat_path, clusters_path = build_maps(df, outdir, global_clusters_df, sample_points=2000)

    maps = {
        "heatmap_path": heat_path,
        "clusters_path": clusters_path
    }

    return summary, report_id, xlsx_path, maps

def train_recommendation_model(conn_params: dict, model_path: str = "recommender_model.pkl"):
    """
    Entrena una Red Neuronal (MLPClassifier) para predecir la siguiente visita
    basándose en el perfil del turista (edad, género, presupuesto) y el monumento actual.
    """
    import pickle
    from sklearn.neural_network import MLPClassifier
    
    # 1. Leer datos de visitas, usuarios y presupuestos
    q_data = """
        SELECT v.idUsuario, v.idEdificioHistorico, v.fecha,
               u.genero, u.fechaDeNacimiento,
               pv.presupuesto
        FROM Visita v
        JOIN Usuario u ON u.idUsuario = v.idUsuario
        LEFT JOIN PeriodoVacacional pv ON pv.idUsuario = v.idUsuario
        ORDER BY v.idUsuario, v.fecha;
    """
    df = mysql_read(conn_params, q_data)
    if df.empty or len(df) < 10:
        return "No hay suficientes datos para entrenar el modelo."
    
    # Calcular edad aproximada (año actual 2026)
    df["fechaDeNacimiento"] = pd.to_datetime(df["fechaDeNacimiento"], errors="coerce")
    df["edad"] = 2026 - df["fechaDeNacimiento"].dt.year
    df["edad"] = df["edad"].fillna(35) # fallback edad promedio
    
    # Convertir género a numérico (Femenino/F -> 1, Masculino/M -> 0)
    df["genero_num"] = df["genero"].apply(lambda x: 1 if str(x).lower().startswith('f') else 0)
    
    # Rellenar presupuesto promedio
    mean_budget = df["presupuesto"].mean()
    if pd.isna(mean_budget):
        mean_budget = 5000.0
    df["presupuesto"] = df["presupuesto"].fillna(mean_budget)
    
    # 2. Generar transiciones de visitas (Monumento A -> Monumento B)
    X = []
    y = []
    
    for uid, group in df.groupby("idUsuario"):
        group = group.sort_values("fecha")
        visit_list = group.to_dict(orient="records")
        for i in range(len(visit_list) - 1):
            curr_visit = visit_list[i]
            next_visit = visit_list[i + 1]
            
            # Feature vector: [edad, genero, presupuesto, id_monumento_actual]
            features = [
                float(curr_visit["edad"]),
                float(curr_visit["genero_num"]),
                float(curr_visit["presupuesto"]),
                float(curr_visit["idEdificioHistorico"])
            ]
            X.append(features)
            y.append(int(next_visit["idEdificioHistorico"]))
            
    if not X:
        return "No hay transiciones de visitas para entrenar."
        
    # 3. Entrenar la Red Neuronal (Multi-Layer Perceptron)
    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    clf = MLPClassifier(hidden_layer_sizes=(64, 32), max_iter=1000, random_state=42)
    clf.fit(X_scaled, y)
    
    # Guardar modelo y metadatos
    model_data = {
        "model": clf,
        "scaler": scaler,
        "mean_budget": mean_budget,
        "classes": clf.classes_.tolist()
    }
    
    with open(model_path, "wb") as f:
        pickle.dump(model_data, f)
        
    return f"Modelo entrenado exitosamente con {len(X)} transiciones de visitas."

def predict_next_monument(conn_params: dict, user_id: int, current_poi_id: int, model_path: str = "recommender_model.pkl"):
    """
    Predice el siguiente monumento recomendado para un usuario específico.
    """
    import pickle
    import os
    
    # Si el modelo no existe, entrenarlo primero
    if not os.path.exists(model_path):
        train_recommendation_model(conn_params, model_path)
        
    try:
        with open(model_path, "rb") as f:
            model_data = pickle.load(f)
    except Exception:
        # Fallback si falla cargar
        return {"recommended_poi_id": 1, "status": "fallback"}
        
    clf = model_data["model"]
    scaler = model_data.get("scaler")
    mean_budget = model_data["mean_budget"]
    
    # Obtener perfil del usuario
    q_user = f"""
        SELECT u.genero, u.fechaDeNacimiento, pv.presupuesto
        FROM Usuario u
        LEFT JOIN PeriodoVacacional pv ON pv.idUsuario = u.idUsuario
        WHERE u.idUsuario = {user_id}
        LIMIT 1;
    """
    df_user = mysql_read(conn_params, q_user)
    
    if df_user.empty:
        # Si el usuario es nuevo, usar promedio de edad y presupuesto por defecto
        edad = 35.0
        genero_num = 0
        presupuesto = mean_budget
    else:
        user_row = df_user.iloc[0]
        birth_year = pd.to_datetime(user_row["fechaDeNacimiento"], errors="coerce").year
        edad = 2026 - birth_year if not pd.isna(birth_year) else 35.0
        genero_num = 1 if str(user_row["genero"]).lower().startswith('f') else 0
        presupuesto = user_row["presupuesto"] if not pd.isna(user_row["presupuesto"]) else mean_budget
        
    # Construir feature vector
    features = [[
        float(edad),
        float(genero_num),
        float(presupuesto),
        float(current_poi_id)
    ]]
    
    if scaler:
        features = scaler.transform(features)
        
    # Realizar predicción con la red neuronal
    pred_class = int(clf.predict(features)[0])
    
    # Obtener probabilidades
    probs = clf.predict_proba(features)[0]
    classes = model_data["classes"]
    
    # Crear un diccionario de probabilidades para las 3 mejores opciones
    top_3_indices = probs.argsort()[-3:][::-1]
    recommendations = []
    for idx in top_3_indices:
        recommendations.append({
            "poi_id": int(classes[idx]),
            "probability": float(probs[idx])
        })
        
    return {
        "user_id": user_id,
        "current_poi_id": current_poi_id,
        "recommended_poi_id": pred_class,
        "top_recommendations": recommendations,
        "status": "success"
    }

def train_profile_model(conn_params: dict, model_path: str = "profile_model.pkl"):
    """
    Entrena una Red Neuronal (MLPClassifier) para clasificar el perfil de turista
    basándose en edad, género, presupuesto, duración del viaje, visitas y ratio de niños.
    """
    import pickle
    from sklearn.neural_network import MLPClassifier
    from sklearn.preprocessing import LabelEncoder, StandardScaler
    
    q_data = """
        SELECT u.idUsuario, u.genero, u.fechaDeNacimiento,
               pv.presupuesto, pv.fechaInicioReal, pv.fechaFinReal,
               COUNT(v.idVisita) as total_visitas,
               SUM(CASE WHEN v.llevaNinos = 1 THEN 1 ELSE 0 END) as total_ninos
        FROM Usuario u
        LEFT JOIN PeriodoVacacional pv ON pv.idUsuario = u.idUsuario
        LEFT JOIN Visita v ON v.idUsuario = u.idUsuario
        GROUP BY u.idUsuario, u.genero, u.fechaDeNacimiento, pv.presupuesto, pv.fechaInicioReal, pv.fechaFinReal;
    """
    df = mysql_read(conn_params, q_data)
    if df.empty or len(df) < 5:
        return "No hay suficientes datos para entrenar el modelo de perfiles."
        
    df["fechaDeNacimiento"] = pd.to_datetime(df["fechaDeNacimiento"], errors="coerce")
    df["edad"] = 2026 - df["fechaDeNacimiento"].dt.year
    df["edad"] = df["edad"].fillna(35)
    df["genero_num"] = df["genero"].apply(lambda x: 1 if str(x).lower().startswith('f') else 0)
    df["presupuesto"] = df["presupuesto"].fillna(5000.0)
    
    df["fechaInicioReal"] = pd.to_datetime(df["fechaInicioReal"], errors="coerce")
    df["fechaFinReal"] = pd.to_datetime(df["fechaFinReal"], errors="coerce")
    df["duracion_minutos"] = (df["fechaFinReal"] - df["fechaInicioReal"]).dt.total_seconds() / 60.0
    df["duracion_minutos"] = df["duracion_minutos"].fillna(120.0)
    
    df["total_visitas"] = df["total_visitas"].fillna(0)
    df["lleva_ninos_ratio"] = df["total_ninos"] / df["total_visitas"]
    df["lleva_ninos_ratio"] = df["lleva_ninos_ratio"].fillna(0.0)
    
    y_labels = []
    for idx, row in df.iterrows():
        if row["lleva_ninos_ratio"] > 0.4:
            y_labels.append("Familiar")
        elif row["presupuesto"] > 6500:
            y_labels.append("Cultural")
        elif row["presupuesto"] < 3000:
            y_labels.append("Religioso")
        elif row["total_visitas"] <= 2:
            y_labels.append("Fotográfico")
        else:
            y_labels.append("Aventura")
            
    df["perfil"] = y_labels
    le = LabelEncoder()
    df["perfil_num"] = le.fit_transform(df["perfil"])
    
    X = df[["edad", "genero_num", "presupuesto", "duracion_minutos", "total_visitas", "lleva_ninos_ratio"]].values.tolist()
    y = df["perfil_num"].values.tolist()
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    clf = MLPClassifier(hidden_layer_sizes=(32, 16), max_iter=1000, random_state=42)
    clf.fit(X_scaled, y)
    
    model_data = {
        "model": clf,
        "scaler": scaler,
        "classes": le.classes_.tolist(),
        "mean_duracion": df["duracion_minutos"].mean(),
        "mean_visitas": df["total_visitas"].mean(),
        "mean_ninos": df["lleva_ninos_ratio"].mean()
    }
    
    with open(model_path, "wb") as f:
        pickle.dump(model_data, f)
        
    return f"Modelo de perfiles entrenado con {len(X)} perfiles."

def predict_tourist_profile(conn_params: dict, user_id: int, model_path: str = "profile_model.pkl"):
    """
    Predice el perfil del turista.
    """
    import pickle
    import os
    
    if not os.path.exists(model_path):
        train_profile_model(conn_params, model_path)
        
    try:
        with open(model_path, "rb") as f:
            model_data = pickle.load(f)
    except Exception:
        return {"recommended_profile": "Cultural", "status": "fallback"}
        
    clf = model_data["model"]
    scaler = model_data.get("scaler")
    classes = model_data["classes"]
    
    q_data = f"""
        SELECT u.genero, u.fechaDeNacimiento,
               pv.presupuesto, pv.fechaInicioReal, pv.fechaFinReal,
               COUNT(v.idVisita) as total_visitas,
               SUM(CASE WHEN v.llevaNinos = 1 THEN 1 ELSE 0 END) as total_ninos
        FROM Usuario u
        LEFT JOIN PeriodoVacacional pv ON pv.idUsuario = u.idUsuario
        LEFT JOIN Visita v ON v.idUsuario = u.idUsuario
        WHERE u.idUsuario = {user_id}
        GROUP BY u.idUsuario, u.genero, u.fechaDeNacimiento, pv.presupuesto, pv.fechaInicioReal, pv.fechaFinReal;
    """
    df = mysql_read(conn_params, q_data)
    
    if df.empty:
        edad = 35.0
        genero_num = 0
        presupuesto = 5000.0
        duracion = model_data["mean_duracion"]
        total_visitas = model_data["mean_visitas"]
        ratio_ninos = model_data["mean_ninos"]
    else:
        row = df.iloc[0]
        birth_year = pd.to_datetime(row["fechaDeNacimiento"], errors="coerce").year
        edad = 2026 - birth_year if not pd.isna(birth_year) else 35.0
        genero_num = 1 if str(row["genero"]).lower().startswith('f') else 0
        presupuesto = row["presupuesto"] if not pd.isna(row["presupuesto"]) else 5000.0
        
        start = pd.to_datetime(row["fechaInicioReal"], errors="coerce")
        end = pd.to_datetime(row["fechaFinReal"], errors="coerce")
        duracion = (end - start).total_seconds() / 60.0 if (not pd.isna(start) and not pd.isna(end)) else model_data["mean_duracion"]
        
        total_visitas = row["total_visitas"] if not pd.isna(row["total_visitas"]) else 0
        ratio_ninos = (row["total_ninos"] / total_visitas) if (total_visitas > 0 and not pd.isna(row["total_ninos"])) else 0.0

    features = [[
        float(edad),
        float(genero_num),
        float(presupuesto),
        float(duracion),
        float(total_visitas),
        float(ratio_ninos)
    ]]
    
    if scaler:
        features = scaler.transform(features)
        
    pred_num = int(clf.predict(features)[0])
    profile_name = classes[pred_num]
    
    probs = clf.predict_proba(features)[0]
    top_recommendations = []
    for idx, prob in enumerate(probs):
        top_recommendations.append({
            "profile": classes[idx],
            "probability": float(prob)
        })
    top_recommendations.sort(key=lambda x: x["probability"], reverse=True)
    
    return {
        "user_id": user_id,
        "recommended_profile": profile_name,
        "top_profiles": top_recommendations,
        "status": "success"
    }

def train_budget_model(conn_params: dict, model_path: str = "budget_model.pkl"):
    """
    Entrena una Red Neuronal de Regresión (MLPRegressor) para predecir el presupuesto.
    """
    import pickle
    from sklearn.neural_network import MLPRegressor
    from sklearn.preprocessing import StandardScaler
    
    q_data = """
        SELECT u.idUsuario, u.genero, u.fechaDeNacimiento,
               pv.presupuesto, pv.fechaInicioReal, pv.fechaFinReal,
               COUNT(v.idVisita) as total_visitas,
               SUM(CASE WHEN v.llevaNinos = 1 THEN 1 ELSE 0 END) as total_ninos
        FROM Usuario u
        JOIN PeriodoVacacional pv ON pv.idUsuario = u.idUsuario
        LEFT JOIN Visita v ON v.idUsuario = u.idUsuario
        GROUP BY u.idUsuario, u.genero, u.fechaDeNacimiento, pv.presupuesto, pv.fechaInicioReal, pv.fechaFinReal;
    """
    df = mysql_read(conn_params, q_data)
    if df.empty or len(df) < 5:
        return "No hay suficientes datos para entrenar el modelo de presupuesto."
        
    df["fechaDeNacimiento"] = pd.to_datetime(df["fechaDeNacimiento"], errors="coerce")
    df["edad"] = 2026 - df["fechaDeNacimiento"].dt.year
    df["edad"] = df["edad"].fillna(35)
    df["genero_num"] = df["genero"].apply(lambda x: 1 if str(x).lower().startswith('f') else 0)
    
    df["fechaInicioReal"] = pd.to_datetime(df["fechaInicioReal"], errors="coerce")
    df["fechaFinReal"] = pd.to_datetime(df["fechaFinReal"], errors="coerce")
    df["duracion_minutos"] = (df["fechaFinReal"] - df["fechaInicioReal"]).dt.total_seconds() / 60.0
    df["duracion_minutos"] = df["duracion_minutos"].fillna(120.0)
    
    df["total_visitas"] = df["total_visitas"].fillna(0)
    df["lleva_ninos_ratio"] = df["total_ninos"] / df["total_visitas"]
    df["lleva_ninos_ratio"] = df["lleva_ninos_ratio"].fillna(0.0)
    df["presupuesto"] = df["presupuesto"].fillna(5000.0)
    
    X = df[["edad", "genero_num", "duracion_minutos", "total_visitas", "lleva_ninos_ratio"]].values.tolist()
    y = df["presupuesto"].values.tolist()
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    reg = MLPRegressor(hidden_layer_sizes=(32, 16), max_iter=1500, random_state=42)
    reg.fit(X_scaled, y)
    
    model_data = {
        "model": reg,
        "scaler": scaler,
        "mean_duracion": df["duracion_minutos"].mean(),
        "mean_visitas": df["total_visitas"].mean(),
        "mean_ninos": df["lleva_ninos_ratio"].mean(),
        "mean_budget": df["presupuesto"].mean()
    }
    
    with open(model_path, "wb") as f:
        pickle.dump(model_data, f)
        
    return f"Modelo de presupuesto entrenado con {len(X)} muestras."

def predict_tourist_budget(conn_params: dict, user_id: int, model_path: str = "budget_model.pkl"):
    """
    Predice el presupuesto estimado del turista.
    """
    import pickle
    import os
    
    if not os.path.exists(model_path):
        train_budget_model(conn_params, model_path)
        
    try:
        with open(model_path, "rb") as f:
            model_data = pickle.load(f)
    except Exception:
        return {"estimated_budget": 5000.0, "status": "fallback"}
        
    reg = model_data["model"]
    scaler = model_data.get("scaler")
    
    q_data = f"""
        SELECT u.genero, u.fechaDeNacimiento,
               pv.fechaInicioReal, pv.fechaFinReal,
               COUNT(v.idVisita) as total_visitas,
               SUM(CASE WHEN v.llevaNinos = 1 THEN 1 ELSE 0 END) as total_ninos
        FROM Usuario u
        LEFT JOIN PeriodoVacacional pv ON pv.idUsuario = u.idUsuario
        LEFT JOIN Visita v ON v.idUsuario = u.idUsuario
        WHERE u.idUsuario = {user_id}
        GROUP BY u.idUsuario, u.genero, u.fechaDeNacimiento, pv.fechaInicioReal, pv.fechaFinReal;
    """
    df = mysql_read(conn_params, q_data)
    
    if df.empty:
        edad = 35.0
        genero_num = 0
        duracion = model_data["mean_duracion"]
        total_visitas = model_data["mean_visitas"]
        ratio_ninos = model_data["mean_ninos"]
    else:
        row = df.iloc[0]
        birth_year = pd.to_datetime(row["fechaDeNacimiento"], errors="coerce").year
        edad = 2026 - birth_year if not pd.isna(birth_year) else 35.0
        genero_num = 1 if str(row["genero"]).lower().startswith('f') else 0
        
        start = pd.to_datetime(row["fechaInicioReal"], errors="coerce")
        end = pd.to_datetime(row["fechaFinReal"], errors="coerce")
        duracion = (end - start).total_seconds() / 60.0 if (not pd.isna(start) and not pd.isna(end)) else model_data["mean_duracion"]
        
        total_visitas = row["total_visitas"] if not pd.isna(row["total_visitas"]) else 0
        ratio_ninos = (row["total_ninos"] / total_visitas) if (total_visitas > 0 and not pd.isna(row["total_ninos"])) else 0.0

    features = [[
        float(edad),
        float(genero_num),
        float(duracion),
        float(total_visitas),
        float(ratio_ninos)
    ]]
    
    if scaler:
        features = scaler.transform(features)
        
    pred_budget = float(reg.predict(features)[0])
    pred_budget = max(500.0, pred_budget)
    
    return {
        "user_id": user_id,
        "estimated_budget": pred_budget,
        "status": "success"
    }