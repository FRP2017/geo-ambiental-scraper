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

def actualizar_desde_excel(client, table_path, id_interno, excel_path):
    # Mapeo de columnas (Excel -> BigQuery)
    mapping = {
        'Nombre del Proyecto': 'nombre_proyecto',
        'Tipo de Presentación': 'tipo_presentacion',
        'Región': 'region',
        'Comuna': 'comuna',
        'Provincia': 'provincia',
        'Tipo de Proyecto': 'tipo_proyecto',
        'Razón de Ingreso': 'razon_ingreso',
        'Titular': 'titular',
        'Inversión (MMU$)': 'inversion_mmu',
        'Fecha Presentación': 'fecha_presentacion',
        'Estado del Proyecto': 'estado_proyecto',
        'Fecha Calificación': 'fecha_calificacion',
        'Sector Productivo': 'sector_productivo',
        'Latitud Punto Representativo': 'latitud',
        'Longitud Punto Representativo': 'longitud'
    }

    try:
        # 1. Leer Excel
        df = pd.read_excel(excel_path)
        if df.empty: return False, "Excel vacío"
        
        row = df.iloc[0]
        set_clauses = []

        for excel_col, bq_col in mapping.items():
            valor = row.get(excel_col)
            
            # --- TRATAMIENTO ESPECIAL DE FECHAS ---
            if "Fecha" in excel_col:
                if pd.isna(valor) or str(valor).strip() == "":
                    set_clauses.append(f"{bq_col} = NULL")
                else:
                    try:
                        # Convertimos a datetime y luego a string formato ISO BQ
                        dt_obj = pd.to_datetime(valor)
                        fecha_iso = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
                        set_clauses.append(f"{bq_col} = '{fecha_iso}'")
                    except:
                        set_clauses.append(f"{bq_col} = NULL")
                continue

            # --- TRATAMIENTO DE NÚMEROS (Inversión, Lat, Lon) ---
            if bq_col in ['inversion_mmu', 'latitud', 'longitud']:
                if pd.isna(valor):
                    set_clauses.append(f"{bq_col} = NULL")
                else:
                    set_clauses.append(f"{bq_col} = {valor}")
                continue

            # --- TRATAMIENTO DE STRINGS ---
            if pd.isna(valor):
                set_clauses.append(f"{bq_col} = NULL")
            else:
                val_str = str(valor).replace("'", "''") # Escapar comillas
                set_clauses.append(f"{bq_col} = '{val_str}'")

        # Agregar timestamp de actualización
        set_clauses.append("fecha_actualizacion = TIMESTAMP(CURRENT_DATETIME('America/Santiago'))")

        # 2. Ejecutar Update
        query = f"UPDATE `{table_path}` SET {', '.join(set_clauses)} WHERE id = '{id_interno}'"
        client.query(query).result()
        
        return True, "Actualización exitosa"

    except Exception as e:
        return False, f"Error en procesamiento de datos: {str(e)}"