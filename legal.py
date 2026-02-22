import os
import json
import re
from google.cloud import storage

# --- CONFIGURACIÓN ---
BUCKET_NAME = "almacen_antecedentes_482615"
PREFIX = "biblioteca_legal/"  # Carpeta a escanear
OUTPUT_FILE = "metadata_biblioteca_legal.jsonl"

def limpiar_id(nombre_archivo):
    """Genera un ID seguro para Vertex (solo alfanumérico y guiones bajos)."""
    # Quitamos extensión y caracteres raros
    nombre_base = os.path.splitext(nombre_archivo)[0]
    return re.sub(r'[^a-zA-Z0-9]', '_', nombre_base)

def generar_metadata():
    print(f"🔌 Conectando al bucket: {BUCKET_NAME}...")
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    # Listamos los archivos en la carpeta específica
    blobs = bucket.list_blobs(prefix=PREFIX)
    
    registros = []
    
    print(f"📂 Escaneando carpeta '{PREFIX}'...")
    
    count = 0
    for blob in blobs:
        # Ignoramos si es la carpeta misma (tamaño 0 y termina en /)
        if blob.name.endswith("/"):
            continue
            
        nombre_archivo = os.path.basename(blob.name)
        
        # 1. ID Único
        id_doc = limpiar_id(nombre_archivo)
        
        # 2. URI de Google Cloud Storage
        uri = f"gs://{BUCKET_NAME}/{blob.name}"
        
        # 3. Metadatos (jsonData)
        # Aquí definimos qué filtros queremos tener disponibles en el buscador
        metadata = {
            "titulo": nombre_archivo,
            "tipo_documento": "Normativa Legal",
            "fuente": "Biblioteca Legal Interna",
            "fecha_carga": blob.updated.strftime('%Y-%m-%d') if blob.updated else None,
            "extension": os.path.splitext(nombre_archivo)[1].replace(".", "")
        }
        
        # 4. Estructura final para Vertex AI Search
        registro = {
            "id": id_doc,
            "jsonData": json.dumps(metadata, ensure_ascii=False),
            "content": {
                "mimeType": blob.content_type or "application/pdf", # Asumimos PDF si no detecta
                "uri": uri
            }
        }
        
        registros.append(registro)
        print(f" - Detectado: {nombre_archivo}")
        count += 1

    # Guardar archivo JSONL localmente
    if registros:
        print(f"\n💾 Guardando {count} registros en '{OUTPUT_FILE}'...")
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            for reg in registros:
                f.write(json.dumps(reg, ensure_ascii=False) + '\n')
        
        print("✅ Archivo JSONL generado exitosamente.")
        
        # Opcional: Subir el JSONL al mismo bucket (raíz o carpeta config)
        respuesta = input("¿Quieres subir este archivo JSONL al bucket ahora? (s/n): ")
        if respuesta.lower() == 's':
            blob_upload = bucket.blob(f"config_search/{OUTPUT_FILE}")
            blob_upload.upload_from_filename(OUTPUT_FILE)
            print(f"🚀 Subido a: gs://{BUCKET_NAME}/config_search/{OUTPUT_FILE}")
    else:
        print("⚠️ No se encontraron archivos en esa carpeta.")

if __name__ == "__main__":
    # Asegúrate de tener credenciales (gcloud auth application-default login)
    try:
        generar_metadata()
    except Exception as e:
        print(f"❌ Error: {e}")