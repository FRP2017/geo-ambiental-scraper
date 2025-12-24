import os
import time
import logging
import traceback
import io
import re
import requests  # <--- Modificación: Importación necesaria
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.cloud import storage

# --- CONFIGURACIÓN DE LOGS ---
def obtener_logger():
    log_stream = io.StringIO()
    logger = logging.getLogger("scraper")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(log_stream)
        logger.addHandler(handler)
    return logger, log_stream

# Función para limpiar nombres de archivos
def limpiar_nombre_archivo(nombre):
    nombre = re.sub(r'[\\/*?:"<>|]', "", nombre)
    return nombre.replace(" ", "_").strip()

def ejecutar_scrapping(nombre_proyecto, titular, fecha_presentacion, bucket_name="almacen_antecedentes"):
    logger, log_stream = obtener_logger()
    
    fecha_str = fecha_presentacion.strftime('%d/%m/%Y') if hasattr(fecha_presentacion, 'strftime') else str(fecha_presentacion)

    if os.environ.get("K_SERVICE"): 
        download_dir = "/tmp"
    else:
        download_dir = os.path.join(os.getcwd(), "downloads")
        if not os.path.exists(download_dir): os.makedirs(download_dir)

    # --- CONFIGURACIÓN PARA CLOUD RUN (HEADLESS) ---
    options = Options()
    options.add_argument('--headless=new') 
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    prefs = {
        "download.default_directory": download_dir, 
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True 
    }
    options.add_experimental_option("prefs", prefs)

    driver = None
    try:
        storage_client = storage.Client() 
        project_id = storage_client.project 
        bucket = storage_client.bucket(bucket_name)
        
        driver = webdriver.Chrome(options=options)
        
        # Habilitar descargas incluso en modo Headless
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow", 
            "downloadPath": download_dir
        })
        
        wait = WebDriverWait(driver, 20)

        # 1. BÚSQUEDA
        driver.get("https://seia.sea.gob.cl/busqueda/buscarProyecto.php")
        ventana_busqueda = driver.current_window_handle
        
        input_proyecto = wait.until(EC.presence_of_element_located((By.ID, "projectName")))
        input_proyecto.send_keys(nombre_proyecto)
        
        input_titular = driver.find_element(By.ID, "nombreTitular")
        input_titular.send_keys(titular)
        
        input_fecha_desde = driver.find_element(By.ID, "startDateFechaP")
        input_fecha_desde.send_keys(fecha_str)
        
        input_fecha_hasta = driver.find_element(By.ID, "endDateFechaP")
        input_fecha_hasta.send_keys(fecha_str)
        
        driver.execute_script("arguments[0].click();", driver.find_element(By.CSS_SELECTOR, "button.sg-btnForm"))
        
        time.sleep(5) # PAUSA MANTENIDA
        
        # 2. DESCARGA EXCEL
        link_excel = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Descargar en formato Excel")))
        driver.execute_script("arguments[0].click();", link_excel)
        time.sleep(2) # PAUSA MANTENIDA

        # 3. NAVEGACIÓN A FICHA
        link_expediente = wait.until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, nombre_proyecto)))
        driver.execute_script("arguments[0].click();", link_expediente)
        
        wait.until(EC.number_of_windows_to_be(2))
        for handle in driver.window_handles:
            if handle != ventana_busqueda:
                driver.switch_to.window(handle)
                break
        
        ventana_ficha = driver.current_window_handle
        time.sleep(5) # PAUSA MANTENIDA
        
        html_ficha = driver.page_source
        nombre_ficha = "ficha_principal.html"
        blob_ficha = bucket.blob(f"{nombre_proyecto}/{nombre_ficha}")
        blob_ficha.content_disposition = f'attachment; filename="{nombre_ficha}"'
        blob_ficha.upload_from_string(html_ficha, content_type='text/html')

        #######################################################################
        # 🚀 LOOP DE EXTRACCIÓN MODIFICADO CON REQUESTS PARA PDF
        #######################################################################
        enlaces_en_tabla = driver.find_elements(By.CSS_SELECTOR, "td.td-primary a")
        num_docs = len(enlaces_en_tabla)
        
        for index in range(num_docs):
            try:
                enlaces_act = driver.find_elements(By.CSS_SELECTOR, "td.td-primary a")
                link_elem = enlaces_act[index]
                url_documento = link_elem.get_attribute("href")
                nombre_doc_original = link_elem.text.strip()
                nombre_limpio = limpiar_nombre_archivo(nombre_doc_original)
                
                # --- MODIFICACIÓN: PDF vía Requests Robusto ---
                if "firma.sea.gob.cl" in url_documento:
                    nombre_final = f"DOC_{index+1}_{nombre_limpio}.pdf"
                    logger.info(f"Descargando PDF robusto: {nombre_final}")
                    
                    # Preparar sesión clonando a Selenium
                    session = requests.Session()
                    user_agent = driver.execute_script("return navigator.userAgent;")
                    session.headers.update({
                        "User-Agent": user_agent,
                        "Referer": driver.current_url
                    })
                    for cookie in driver.get_cookies():
                        session.cookies.set(cookie['name'], cookie['value'])
                    
                    # Ejecutar descarga
                    res = session.get(url_documento, timeout=30)
                    content_type = res.headers.get('Content-Type', '').lower()
                    
                    # Verificación de integridad (No descargar si es HTML/Error)
                    if res.status_code == 200 and 'application/pdf' in content_type:
                        blob = bucket.blob(f"{nombre_proyecto}/documentos_detalle/{nombre_final}")
                        blob.content_disposition = f'attachment; filename="{nombre_final}"'
                        blob.upload_from_string(res.content, content_type='application/pdf')
                        logger.info(f"✅ PDF guardado: {nombre_final}")
                    else:
                        logger.error(f"❌ PDF Corrupto o Error: {nombre_final} (Tipo: {content_type})")
                
                # HTML (Se mantiene lógica original)
                else:
                    driver.execute_script("arguments[0].click();", link_elem)
                    wait.until(EC.number_of_windows_to_be(3))
                    for handle in driver.window_handles:
                        if handle != ventana_busqueda and handle != ventana_ficha:
                            driver.switch_to.window(handle)
                            break
                    time.sleep(3) # PAUSA MANTENIDA
                    html_doc = driver.page_source
                    nombre_archivo = f"DOC_{index+1}_{nombre_limpio}.html"
                    blob = bucket.blob(f"{nombre_proyecto}/documentos_detalle/{nombre_archivo}")
                    blob.content_disposition = f'attachment; filename="{nombre_archivo}"'
                    blob.upload_from_string(html_doc, content_type='text/html')
                    driver.close()
                    driver.switch_to.window(ventana_ficha)
                
                time.sleep(3) # PAUSA MANTENIDA

            except Exception as e:
                if len(driver.window_handles) > 2: driver.close()
                driver.switch_to.window(ventana_ficha)
                time.sleep(2)

        # Finalización Excel (Se mantiene lógica original)
        for f in os.listdir(download_dir):
            if f.endswith(".xlsx"):
                ruta_local = os.path.join(download_dir, f)
                blob_xlsx = bucket.blob(f"{nombre_proyecto}/{f}")
                blob_xlsx.content_disposition = f'attachment; filename="{f}"'
                blob_xlsx.upload_from_filename(ruta_local)
                os.remove(ruta_local)
                break

        ruta_gcs = f"gs://{bucket_name}/{nombre_proyecto}/"
        console_url = f"https://console.cloud.google.com/storage/browser/{bucket_name}/{nombre_proyecto}?project={project_id}"
        
        return f"✅ EXITOSO|Excel Procesado|{ruta_gcs}|{console_url}|{num_docs}", log_stream.getvalue()

    except Exception:
        return f"❌ FALLO|{traceback.format_exc()}", log_stream.getvalue()
    finally:
        if driver: driver.quit()