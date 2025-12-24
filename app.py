import streamlit as st
from google.cloud import bigquery
from scraper import ejecutar_scrapping

st.set_page_config(page_title="SEIA Document Crawler", layout="wide")

@st.cache_data(ttl=3600)
def obtener_proyectos_bq():
    try:
        client = bigquery.Client()
        # Seleccionamos los 3 campos necesarios para identificar de forma única
        query = """
                SELECT DISTINCT 
                TRIM(REPLACE(nombre_proyecto, '(e-seia)', '')) AS nombre_proyecto, 
                titular, 
                fecha_presentacion
            FROM `geo-ambiental-481814.dataset_ambiental.raw_seia_final` 
            WHERE nombre_proyecto IS NOT NULL
            ORDER BY nombre_proyecto ASC
        """
        query_job = client.query(query)
        df = query_job.to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error al conectar con BigQuery: {e}")
        return None

st.title("🕷️ SEIA: Crawler de Documentos")
st.markdown("Extrae automáticamente el Excel, la ficha y el contenido íntegro (HTML/PDF) usando búsqueda por metadatos únicos.")

with st.spinner("Cargando listado de proyectos únicos..."):
    df_proyectos = obtener_proyectos_bq()

if df_proyectos is not None:
    # Creamos una lista de opciones que muestra los 3 metadatos
    opciones = []
    for _, row in df_proyectos.iterrows():
        fecha_fmt = row['fecha_presentacion'].strftime('%d/%m/%Y')
        label = f"{row['nombre_proyecto']} | {row['titular']} | {fecha_fmt}"
        opciones.append({
            "label": label,
            "nombre": row['nombre_proyecto'],
            "titular": row['titular'],
            "fecha": row['fecha_presentacion']
        })

    seleccion = st.selectbox(
        "Selecciona el proyecto exacto para procesar:",
        options=opciones,
        format_func=lambda x: x["label"] if x else "Seleccione un proyecto..."
    )

    st.divider()

    if st.button("🚀 Iniciar Crawling Profundo", use_container_width=True):
        if seleccion:
            with st.status(f"Procesando: {seleccion['nombre']}...", expanded=True) as status:
                st.write(f"🔍 Buscando por Nombre, Titular y Fecha: {seleccion['fecha']}")
                
                # Pasamos los 3 parámetros al scraper
                resultado, logs = ejecutar_scrapping(
                    seleccion['nombre'], 
                    seleccion['titular'], 
                    seleccion['fecha']
                )
                
                if "✅ EXITOSO" in resultado:
                    partes = resultado.split("|")
                    status.update(label=f"✅ ¡Completado! {partes[4]} documentos procesados.", state="complete")
                    
                    st.success(f"**Proceso finalizado.** Se ha evitado la duplicidad mediante búsqueda por metadatos.")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Documentos", partes[4])
                    with col2:
                        st.metric("Criterio Selección", "Metadatos Únicos")

                    st.info(f"📍 **Bucket:** `{partes[2]}`")
                    st.link_button("📂 Explorar Archivos", partes[3])
                else:
                    status.update(label="❌ Falló el proceso", state="error")
                    st.error(resultado)
                
                with st.expander("Ver bitácora de ejecución (Logs)"):
                    st.code(logs)
        else:
            st.warning("Selecciona un proyecto.")
else:
    st.error("No se pudo cargar la base de datos de proyectos.")