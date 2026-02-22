import os
from google.cloud import storage

# --- CONFIGURACIÓN ---
PROJECT_ID = os.getenv("PROJECT_ID", "geo-ambiental-482615") 
BUCKET_NAME = os.getenv("BUCKET_NAME", "almacen_antecedentes_482615")
OUTPUT_FILE = "metadata_maestra.jsonl"
OUTPUT_PATH = f"config_search/{OUTPUT_FILE}"

def consolidar_archivos():
    print(f"🔌 Conectando al bucket: {BUCKET_NAME}...")
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    
    # 1. Listar todos los archivos del bucket
    blobs = client.list_blobs(BUCKET_NAME)
    
    contenido_total = []
    archivos_procesados = 0
    
    print("🔍 Buscando archivos .jsonl dispersos...")
    
    for blob in blobs:
        # Filtramos solo los .jsonl, evitando el archivo de salida para no duplicar
        if blob.name.endswith(".jsonl") and blob.name != OUTPUT_PATH:
            try:
                print(f"   - Leyendo: {blob.name}")
                # Descargamos el texto y lo limpiamos de espacios extra
                texto = blob.download_as_text().strip()
                if texto:
                    contenido_total.append(texto)
                    archivos_procesados += 1
            except Exception as e:
                print(f"   ⚠️ Error leyendo {blob.name}: {e}")

    if contenido_total:
        print(f"\n🧩 Unificando {archivos_procesados} archivos...")
        
        # Unimos todo con saltos de línea (formato JSONL estricto)
        contenido_final = "\n".join(contenido_total)
        
        # 2. Subir el archivo maestro
        blob_salida = bucket.blob(OUTPUT_PATH)
        blob_salida.upload_from_string(contenido_final, content_type='application/jsonl')
        
        print(f"✅ ¡Éxito! Archivo maestro creado en:")
        print(f"   gs://{BUCKET_NAME}/{OUTPUT_PATH}")
        print("\n--- INSTRUCCIONES PARA VERTEX AI ---")
        print("1. Ve a 'Data Stores' > 'Import Data'.")
        print("2. Selecciona 'Cloud Storage'.")
        print(f"3. Elige la opción 'JSONL for unstructured data with metadata'.")
        print(f"4. En la ruta, selecciona ESTE archivo específico: gs://{BUCKET_NAME}/{OUTPUT_PATH}")
        
    else:
        print("⚠️ No se encontraron archivos .jsonl en el bucket.")

if __name__ == "__main__":
    consolidar_archivos()