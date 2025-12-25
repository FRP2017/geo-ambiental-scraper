import streamlit as st
import folium
from streamlit_folium import st_folium
from google.cloud import bigquery
import pandas as pd
from scraper import ejecutar_scrapping 

# --- 1. CONFIGURACIÓN Y ESTÉTICA (De app1.py) ---
st.set_page_config(page_title="GeoAmbiental Pro", layout="wide", page_icon="🌍")

st.markdown("""
    <style>
    ::-webkit-scrollbar { width: 20px !important; height: 20px !important; }
    ::-webkit-scrollbar-track { background: #f1f1f1 !important; }
    ::-webkit-scrollbar-thumb { background: #1E40AF !important; border-radius: 10px; border: 3px solid #f1f1f1; }
    [data-testid="stSidebar"] { background-color: #0F172A; color: white; }
    div.stButton > button { border-radius: 8px; background-color: #1E3A8A; color: white; font-weight: bold; width: 100%; }
    </style>
""", unsafe_allow_html=True)

PROJECT_ID = "geo-ambiental-481814"
bq_client = bigquery.Client(project=PROJECT_ID)
BUCKET_NAME = "almacen_antecedentes"
DATASET_ID = "dataset_ambiental"
TABLE_ID = "raw_seia_final"

if 'punto_seleccionado' not in st.session_state:
    st.session_state.punto_seleccionado = {"lat": -33.4489, "lon": -70.6693}
if 'df_resultados' not in st.session_state:
    st.session_state.df_resultados = None

# --- 2. SIDEBAR (Búsqueda Geo) ---
with st.sidebar:
    st.title("🛰️ Control Geo")
    in_lat = st.number_input("Latitud", value=st.session_state.punto_seleccionado['lat'], format="%.6f")
    in_lon = st.number_input("Longitud", value=st.session_state.punto_seleccionado['lon'], format="%.6f")

    if (in_lat != st.session_state.punto_seleccionado['lat'] or in_lon != st.session_state.punto_seleccionado['lon']):
        st.session_state.punto_seleccionado = {"lat": in_lat, "lon": in_lon}
        st.rerun()

    radio_km = st.slider("Radio (Km)", 1, 50, 10)
    
    if st.button("🔍 BUSCAR PROYECTOS"):
        query = f"""
            SELECT 
                nombre_proyecto as nombre_original,
                titular, 
                fecha_presentacion,
                latitud, longitud, region, provincia, comuna, tipo_proyecto, estado_proyecto, inversion_mmu,
                ST_DISTANCE(SAFE.ST_GEOGPOINT(longitud, latitud), SAFE.ST_GEOGPOINT({st.session_state.punto_seleccionado['lon']}, {st.session_state.punto_seleccionado['lat']})) / 1000 as distancia_km
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE SAFE.ST_GEOGPOINT(longitud, latitud) IS NOT NULL
              AND ST_DWITHIN(SAFE.ST_GEOGPOINT(longitud, latitud), SAFE.ST_GEOGPOINT({st.session_state.punto_seleccionado['lon']}, {st.session_state.punto_seleccionado['lat']}), {radio_km * 1000})
            ORDER BY distancia_km ASC LIMIT 1000
        """
        raw_df = bq_client.query(query).to_dataframe()
        # Eliminamos duplicados por metadatos únicos antes de guardar en sesión
        st.session_state.df_resultados = raw_df.drop_duplicates(subset=['nombre_original', 'titular', 'fecha_presentacion'])
        st.rerun()

# --- 3. MAPA ---
st.title("🌍 Inteligencia Territorial + Scraper")
m = folium.Map(location=[st.session_state.punto_seleccionado['lat'], st.session_state.punto_seleccionado['lon']], zoom_start=11, tiles="cartodbpositron")
folium.Marker([st.session_state.punto_seleccionado['lat'], st.session_state.punto_seleccionado['lon']], icon=folium.Icon(color='red')).add_to(m)

if st.session_state.df_resultados is not None:
    for _, r in st.session_state.df_resultados.iterrows():
        folium.CircleMarker([r['latitud'], r['longitud']], radius=4, color='blue', fill=True).add_to(m)

st_folium(m, width="100%", height=300, key="mapa_final")

# --- 4. FILTROS (Restaurados de app1.py) ---
if st.session_state.df_resultados is not None:
    df_f = st.session_state.df_resultados.copy()
    
    with st.expander("🛠️ PANEL DE FILTROS", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            f_region = st.multiselect("Región", options=sorted(df_f['region'].dropna().unique()))
            f_comuna = st.multiselect("Comuna", options=sorted(df_f['comuna'].dropna().unique()))
        with c2:
            f_prov = st.multiselect("Provincia", options=sorted(df_f['provincia'].dropna().unique()))
            f_tipo = st.multiselect("Tipo Proyecto", options=sorted(df_f['tipo_proyecto'].dropna().unique()))
        with c3:
            f_titu = st.multiselect("Titular", options=sorted(df_f['titular'].dropna().unique()))
            f_esta = st.multiselect("Estado", options=sorted(df_f['estado_proyecto'].dropna().unique()))
        with c4:
            f_inv = st.slider("Inversión MMU", float(df_f['inversion_mmu'].min()), float(df_f['inversion_mmu'].max()), (float(df_f['inversion_mmu'].min()), float(df_f['inversion_mmu'].max())))
            f_dist = st.slider("Distancia Km", float(df_f['distancia_km'].min()), float(df_f['distancia_km'].max()), (float(df_f['distancia_km'].min()), float(df_f['distancia_km'].max())))

    # Aplicar Filtros
    if f_region: df_f = df_f[df_f['region'].isin(f_region)]
    if f_comuna: df_f = df_f[df_f['comuna'].isin(f_comuna)]
    if f_prov:   df_f = df_f[df_f['provincia'].isin(f_prov)]
    if f_tipo:   df_f = df_f[df_f['tipo_proyecto'].isin(f_tipo)]
    if f_titu:   df_f = df_f[df_f['titular'].isin(f_titu)]
    if f_esta:   df_f = df_f[df_f['estado_proyecto'].isin(f_esta)]
    df_f = df_f[(df_f['inversion_mmu'] >= f_inv[0]) & (df_f['inversion_mmu'] <= f_inv[1])]
    df_f = df_f[(df_f['distancia_km'] >= f_dist[0]) & (df_f['distancia_km'] <= f_dist[1])]

    # --- 5. RESULTADOS Y LÓGICA DE SCRAPER (MODIFICADO) ---
    st.subheader("📋 Selección de Proyectos")
    
    evento_seleccion = st.dataframe(
        df_f, 
        use_container_width=True, 
        height=400, 
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row" 
    )

    indices = evento_seleccion.selection.rows
    df_final = df_f.iloc[indices]

    if len(df_final) > 0:
        st.success(f"✅ {len(df_final)} proyectos seleccionados.")
        if st.button(f"🚀 INICIAR SCRAPPING ({len(df_final)})"):
            for i, row in df_final.iterrows():
                # --- AQUÍ SE REALIZA LA LIMPIEZA SOLICITADA ---
                nombre_limpio_busqueda = row['nombre_original'].replace("(e-seia)", "").strip()
                
                with st.status(f"Procesando: {nombre_limpio_busqueda}", expanded=True) as status:
                    # Se envía el nombre ya limpio al scraper
                    resultado, logs = ejecutar_scrapping(
                        nombre_limpio_busqueda, 
                        row['titular'], 
                        row['fecha_presentacion'], 
                        BUCKET_NAME
                    )
                    
                    if "✅ EXITOSO" in resultado:
                        status.update(label=f"✅ Finalizado: {nombre_limpio_busqueda}", state="complete")
                    else:
                        status.update(label=f"❌ Falló: {nombre_limpio_busqueda}", state="error")
                        st.error(resultado)
                    
                    with st.expander(f"Logs de {nombre_limpio_busqueda}"):
                        st.code(logs)
            st.balloons()
    else:
        st.warning("Selecciona proyectos en la tabla.")
else:
    st.info("Realice una búsqueda geo para comenzar.")