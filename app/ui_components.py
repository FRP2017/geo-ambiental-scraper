import streamlit as st
import folium
from streamlit_folium import st_folium

def inyectar_estilos():
    st.markdown("""
        <style>
        ::-webkit-scrollbar { width: 20px !important; height: 20px !important; }
        ::-webkit-scrollbar-track { background: #f1f1f1 !important; }
        ::-webkit-scrollbar-thumb { background: #1E40AF !important; border-radius: 10px; border: 3px solid #f1f1f1; }
        [data-testid="stSidebar"] { background-color: #0F172A; color: white; }
        div.stButton > button { border-radius: 8px; background-color: #1E3A8A; color: white; font-weight: bold; width: 100%; }
        </style>
    """, unsafe_allow_html=True)

def renderizar_mapa(lat, lon, df_resultados=None):
    m = folium.Map(location=[lat, lon], zoom_start=11, tiles="cartodbpositron")
    folium.Marker([lat, lon], icon=folium.Icon(color='red')).add_to(m)
    
    if df_resultados is not None:
        for _, r in df_resultados.iterrows():
            folium.CircleMarker([r['latitud'], r['longitud']], radius=4, color='blue', fill=True).add_to(m)
    
    return st_folium(m, width="100%", height=300, key="mapa_final")

# ui_components.py (Añadir esta función)

def mostrar_panel_filtros(df):
    """Renderiza el expander de filtros y retorna el diccionario de filtros seleccionados."""
    with st.expander("🛠️ PANEL DE FILTROS", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            f_reg = st.multiselect("Región", sorted(df['region'].dropna().unique()))
            f_com = st.multiselect("Comuna", sorted(df['comuna'].dropna().unique()))
        with c2:
            f_prov = st.multiselect("Provincia", sorted(df['provincia'].dropna().unique()))
            f_tipo = st.multiselect("Tipo Proyecto", sorted(df['tipo_proyecto'].dropna().unique()))
        with c3:
            f_titu = st.multiselect("Titular", sorted(df['titular'].dropna().unique()))
            f_esta = st.multiselect("Estado", sorted(df['estado_proyecto'].dropna().unique()))
        with c4:
            f_inv = st.slider("Inversión MMU", float(df['inversion_mmu'].min()), float(df['inversion_mmu'].max()), 
                              (float(df['inversion_mmu'].min()), float(df['inversion_mmu'].max())))
            f_dist = st.slider("Distancia Km", float(df['distancia_km'].min()), float(df['distancia_km'].max()), 
                               (float(df['distancia_km'].min()), float(df['distancia_km'].max())))
    
    return {
        'region': f_reg, 'comuna': f_com, 'provincia': f_prov, 'tipo': f_tipo,
        'titular': f_titu, 'estado': f_esta, 'inversion': f_inv, 'distancia': f_dist
    }