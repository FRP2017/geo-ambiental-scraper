import pandas as pd
from google.cloud import bigquery

def consultar_proyectos_bq(client, table_path, lat, lon, radio_km):
    """Ejecuta la query geoespacial en BigQuery."""
    query = f"""
        SELECT 
            id,
            nombre_proyecto as nombre_original,
            titular, 
            fecha_presentacion,
            fecha_calificacion,
            latitud, longitud, region, provincia, comuna, tipo_proyecto, estado_proyecto, inversion_mmu,
            ST_DISTANCE(SAFE.ST_GEOGPOINT(longitud, latitud), SAFE.ST_GEOGPOINT({lon}, {lat})) / 1000 as distancia_km
        FROM `{table_path}`
        WHERE SAFE.ST_GEOGPOINT(longitud, latitud) IS NOT NULL
          AND ST_DWITHIN(SAFE.ST_GEOGPOINT(longitud, latitud), SAFE.ST_GEOGPOINT({lon}, {lat}), {radio_km * 1000})
        ORDER BY distancia_km ASC LIMIT 1000
    """
    raw_df = client.query(query).to_dataframe()
    # Limpieza de duplicados
    return raw_df.drop_duplicates(subset=['id', 'nombre_original', 'titular', 'fecha_presentacion'])

def filtrar_dataframe(df, filtros):
    """Aplica la lógica de filtrado de pandas de forma aislada."""
    df_f = df.copy()
    if filtros['region']: df_f = df_f[df_f['region'].isin(filtros['region'])]
    if filtros['comuna']: df_f = df_f[df_f['comuna'].isin(filtros['comuna'])]
    if filtros['provincia']: df_f = df_f[df_f['provincia'].isin(filtros['provincia'])]
    if filtros['tipo']: df_f = df_f[df_f['tipo_proyecto'].isin(filtros['tipo'])]
    if filtros['titular']: df_f = df_f[df_f['titular'].isin(filtros['titular'])]
    if filtros['estado']: df_f = df_f[df_f['estado_proyecto'].isin(filtros['estado'])]
    
    df_f = df_f[(df_f['inversion_mmu'] >= filtros['inversion'][0]) & 
                (df_f['inversion_mmu'] <= filtros['inversion'][1])]
    df_f = df_f[(df_f['distancia_km'] >= filtros['distancia'][0]) & 
                (df_f['distancia_km'] <= filtros['distancia'][1])]
    return df_f