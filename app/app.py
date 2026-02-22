import streamlit as st
import os
from google.cloud import bigquery
from scraper import ejecutar_scrapping 
# Importamos nuestros nuevos módulos
import data_manager as dm
import ui_components as ui
import time
import gc 
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
# === AGREGAR ESTO ===
if 'ejecutando_scraping' not in st.session_state:
    st.session_state.ejecutando_scraping = False

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
        
        # --- CÁLCULO SEGURO PARA SLIDERS (EVITAR ERROR min==max) ---
        min_inv = float(st.session_state.df_resultados['inversion_mmu'].min())
        max_inv = float(st.session_state.df_resultados['inversion_mmu'].max())
        if min_inv == max_inv:
            min_slider_inv = max(0.0, min_inv - 1.0)
            max_slider_inv = max_inv + 1.0
        else:
            min_slider_inv = min_inv
            max_slider_inv = max_inv

        min_dist = float(st.session_state.df_resultados['distancia_km'].min())
        max_dist = float(st.session_state.df_resultados['distancia_km'].max())
        if min_dist == max_dist:
            min_slider_dist = max(0.0, min_dist - 1.0)
            max_slider_dist = max_dist + 1.0
        else:
            min_slider_dist = min_dist
            max_slider_dist = max_dist
        # -----------------------------------------------------------

        filtros = {
            'region': f_reg, 'comuna': f_com, 
            'provincia': st.multiselect("Provincia", sorted(st.session_state.df_resultados['provincia'].dropna().unique())),
            'tipo': st.multiselect("Tipo Proyecto", sorted(st.session_state.df_resultados['tipo_proyecto'].dropna().unique())),
            'titular': st.multiselect("Titular", sorted(st.session_state.df_resultados['titular'].dropna().unique())),
            'estado': st.multiselect("Estado", sorted(st.session_state.df_resultados['estado_proyecto'].dropna().unique())),
            'inversion': st.slider("Inversión MMU", min_value=min_slider_inv, max_value=max_slider_inv, value=(min_inv, max_inv)),
            'distancia': st.slider("Distancia Km", min_value=min_slider_dist, max_value=max_slider_dist, value=(min_dist, max_dist))
        }

        df_f = dm.filtrar_dataframe(st.session_state.df_resultados, filtros)

        # --- TABLA Y SCRAPER ---
        st.subheader("📋 Selección de Proyectos")
        evento_seleccion = st.dataframe(df_f, use_container_width=True, height=400, hide_index=True, on_select="rerun", selection_mode="multi-row")

        # Capturamos la selección
        indices = evento_seleccion.selection.rows
        df_final = df_f.iloc[indices]


# Lógica de Ejecución
        if len(df_final) > 0:
            
            # 1. LIMITADOR DE SEGURIDAD
            cantidad_sel = len(df_final)
            if cantidad_sel > 10:
                st.warning(f"⚠️ Has seleccionado {cantidad_sel}. Se procesarán los primeros 10.")
                df_final = df_final.iloc[:10]
                cantidad_sel = 10

            # 2. BOTÓN QUE ACTIVA LA BANDERA
            # Deshabilitamos el botón si ya está corriendo
            boton_disabled = st.session_state.ejecutando_scraping
            
            if st.button(f"🚀 INICIAR SCRAPPING (Lote: {cantidad_sel})", disabled=boton_disabled):
                st.session_state.ejecutando_scraping = True
                st.rerun() # Recarga para bloquear la UI

            # 3. PROCESO PERSISTENTE (Se mantiene vivo tras el rerun)
            if st.session_state.ejecutando_scraping:
                
                import traceback # Para ver errores detallados si falla
                
                contenedor = st.container()
                with contenedor:
                    st.info("⚠️ Procesando... Por favor no cierres esta pestaña.")
                    barra_progreso = st.progress(0, text="Iniciando secuencia...")
                    console_log = st.empty() 

                    try:
                        # --- INICIO DEL BUCLE FOR ---
                        for i, (_, row) in enumerate(df_final.iterrows()):
                            
                            nombre_limpio = row['nombre_original'].replace("(e-seia)", "").strip()
                            
                            # Actualizar barra
                            pct = (i) / cantidad_sel
                            barra_progreso.progress(pct, text=f"⏳ [{i+1}/{cantidad_sel}] Procesando: {nombre_limpio}")
                            
                            with st.status(f"Analizando: {nombre_limpio}", expanded=False) as status:
                                st.write("⚙️ Iniciando navegador...")
                                
                                # LLAMADA AL SCRAPER
                                res, logs, excel_path = ejecutar_scrapping(
                                    row['id'], nombre_limpio, row['titular'], 
                                    row['fecha_presentacion'], BUCKET_NAME,
                                    region=row['region'], comuna=row['comuna']
                                )

                                # PROCESAMIENTO DE RESPUESTA
                                if "✅ EXITOSO" in res:
                                    st.write("✅ Descarga completada. Actualizando BD...")
                                    if excel_path:
                                        ok_upd, msg_upd = dm.actualizar_desde_excel(bq_client, BQ_TABLE_PATH, row['id'], excel_path)
                                        if os.path.exists(excel_path): os.remove(excel_path)
                                        
                                        if ok_upd:
                                            status.update(label=f"✅ Finalizado: {nombre_limpio}", state="complete")
                                        else:
                                            status.update(label=f"⚠️ Error BQ: {nombre_limpio}", state="error")
                                            st.error(msg_upd)
                                    else:
                                        status.update(label=f"✅ Finalizado (Sin Excel): {nombre_limpio}", state="complete")

                                elif "⚠️ SIN RESULTADOS" in res:
                                    status.update(label=f"⚠️ Sin datos: {nombre_limpio}", state="error")
                                    st.warning("No se encontraron documentos.")
                                else:
                                    status.update(label=f"❌ Falló: {nombre_limpio}", state="error")
                                    st.error(res)

                                with st.expander("Ver Logs Técnicos"):
                                    st.code(logs)

                            # LIMPIEZA DE MEMORIA
                            console_log.text(f"♻️ Liberando memoria tras proyecto {i+1}...")
                            gc.collect()
                            time.sleep(2)

                        # --- FIN DEL PROCESO ---
                        barra_progreso.progress(1.0, text="✅ ¡Lote completado!")
                        st.success(f"Proceso finalizado. {cantidad_sel} proyectos procesados.")
                        st.balloons()
                    
                    except Exception as e:
                        st.error(f"💥 Error crítico: {e}")
                        print(traceback.format_exc()) # Imprime error real en consola negra
                    
                    finally:
                        # APAGAMOS LA BANDERA AL TERMINAR (SEA ÉXITO O ERROR)
                        st.session_state.ejecutando_scraping = False
                        if st.button("🔄 Reiniciar Vista"):
                            st.rerun()