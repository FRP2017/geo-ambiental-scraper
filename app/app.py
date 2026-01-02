import streamlit as st
import os
from google.cloud import bigquery
from scraper import ejecutar_scrapping 
# Importamos nuestros nuevos módulos
import data_manager as dm
import ui_components as ui

# --- CONFIGURACIÓN ---
PROJECT_ID = os.getenv("PROJECT_ID", "geo-ambiental-482615") 
BUCKET_NAME = os.getenv("BUCKET_NAME", "almacen_antecedentes_482615")
BQ_TABLE_PATH = os.getenv("BQ_TABLE_PATH", "geo-ambiental-482615.dataset_ambiental.seia_limpio")

st.set_page_config(page_title="GeoAmbiental Pro", layout="wide", page_icon="🌍")
ui.inyectar_estilos()
bq_client = bigquery.Client(project=PROJECT_ID)

# --- ESTADO DE SESIÓN ---
if 'punto_seleccionado' not in st.session_state:
    st.session_state.punto_seleccionado = {"lat": -33.4489, "lon": -70.6693}
if 'df_resultados' not in st.session_state:
    st.session_state.df_resultados = None

# --- SIDEBAR ---
with st.sidebar:
    st.title("🛰️ Control Geo")
    in_lat = st.number_input("Latitud", value=st.session_state.punto_seleccionado['lat'], format="%.6f")
    in_lon = st.number_input("Longitud", value=st.session_state.punto_seleccionado['lon'], format="%.6f")

    if (in_lat != st.session_state.punto_seleccionado['lat'] or in_lon != st.session_state.punto_seleccionado['lon']):
        st.session_state.punto_seleccionado = {"lat": in_lat, "lon": in_lon}
        st.rerun()

    radio_km = st.slider("Radio (Km)", 1, 50, 10)
    
    if st.button("🔍 BUSCAR PROYECTOS"):
        st.session_state.df_resultados = dm.consultar_proyectos_bq(
            bq_client, BQ_TABLE_PATH, 
            st.session_state.punto_seleccionado['lat'], 
            st.session_state.punto_seleccionado['lon'], 
            radio_km
        )
        st.rerun()

# --- CUERPO PRINCIPAL ---
st.title("🌍 Inteligencia Territorial + Scraper")
ui.renderizar_mapa(st.session_state.punto_seleccionado['lat'], st.session_state.punto_seleccionado['lon'], st.session_state.df_resultados)

if st.session_state.df_resultados is not None:
    # Bloque de Filtros
    with st.expander("🛠️ PANEL DE FILTROS", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            f_reg = st.multiselect("Región", sorted(st.session_state.df_resultados['region'].dropna().unique()))
            f_com = st.multiselect("Comuna", sorted(st.session_state.df_resultados['comuna'].dropna().unique()))
        
        filtros = {
            'region': f_reg, 'comuna': f_com, 
            'provincia': st.multiselect("Provincia", sorted(st.session_state.df_resultados['provincia'].dropna().unique())),
            'tipo': st.multiselect("Tipo Proyecto", sorted(st.session_state.df_resultados['tipo_proyecto'].dropna().unique())),
            'titular': st.multiselect("Titular", sorted(st.session_state.df_resultados['titular'].dropna().unique())),
            'estado': st.multiselect("Estado", sorted(st.session_state.df_resultados['estado_proyecto'].dropna().unique())),
            'inversion': st.slider("Inversión MMU", float(st.session_state.df_resultados['inversion_mmu'].min()), float(st.session_state.df_resultados['inversion_mmu'].max()), (float(st.session_state.df_resultados['inversion_mmu'].min()), float(st.session_state.df_resultados['inversion_mmu'].max()))),
            'distancia': st.slider("Distancia Km", float(st.session_state.df_resultados['distancia_km'].min()), float(st.session_state.df_resultados['distancia_km'].max()), (float(st.session_state.df_resultados['distancia_km'].min()), float(st.session_state.df_resultados['distancia_km'].max())))
        }

    df_f = dm.filtrar_dataframe(st.session_state.df_resultados, filtros)

    # --- TABLA Y SCRAPER ---
    st.subheader("📋 Selección de Proyectos")
    evento_seleccion = st.dataframe(df_f, use_container_width=True, height=400, hide_index=True, on_select="rerun", selection_mode="multi-row")
    
    indices = evento_seleccion.selection.rows
    df_final = df_f.iloc[indices]

    if len(df_final) > 0:
        if st.button(f"🚀 INICIAR SCRAPPING ({len(df_final)})"):
            for _, row in df_final.iterrows():
                nombre_limpio = row['nombre_original'].replace("(e-seia)", "").strip()
                with st.status(f"Procesando: {nombre_limpio}") as status:
                    # AQUÍ ESTÁ EL CONTRATO QUE NO DEBO ROMPER:
                    res, logs = ejecutar_scrapping(row['id'], nombre_limpio, row['titular'], row['fecha_presentacion'], row['fecha_calificacion'], BUCKET_NAME)
                    
                    # --- NUEVO BLOQUE DE LÓGICA DE RESULTADOS ---
                    if "✅ EXITOSO" in res:
                        status.update(label=f"✅ Finalizado: {nombre_limpio}", state="complete")
                    
                    elif "⚠️ SIN RESULTADOS" in res:
                        # Si el scraper devuelve que la tabla está vacía
                        partes = res.split("|")
                        # Aquí capturamos la cadena larga con los 4 parámetros
                        parametros = partes[1] if len(partes) > 1 else "No se pudieron recuperar los parámetros."
                        
                        status.update(label=f"⚠️ Sin resultados: {nombre_limpio}", state="error")
                        
                        # Mostramos el aviso amarillo
                        st.warning(f"La búsqueda no arrojó registros en el SEIA para el proyecto **{nombre_limpio}**.")
                        
                        # Mostramos el cuadro azul con los 4 parámetros detallados
                        st.info(f"**Datos utilizados en la búsqueda fallida:**\n\n{parametros}")
                    
                    else:
                        # Error crítico o fallo de Selenium
                        status.update(label=f"❌ Falló: {nombre_limpio}", state="error")
                        st.error(res)
                    # --------------------------------------------

                    with st.expander(f"Logs de {nombre_limpio}"): 
                        st.code(logs)
            st.balloons()