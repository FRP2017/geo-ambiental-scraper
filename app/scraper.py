import os
import time
import logging
import traceback
import io
import re
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.cloud import storage
from bs4 import BeautifulSoup
import pandas as pd
import sys
# ==========================================
# 1. UTILIDADES Y CONFIGURACIÓN
# ==========================================

def obtener_logger():
    log_stream = io.StringIO()
    logger = logging.getLogger("scraper")
    logger.setLevel(logging.INFO)
    
    # Limpiamos handlers anteriores para evitar duplicados si se reusa
    if logger.handlers:
        logger.handlers = []

    # 1. ESTO YA LO TENÍAS (Guarda el log en memoria para tu reporte final)
    handler_memoria = logging.StreamHandler(log_stream)
    logger.addHandler(handler_memoria)

    # 2. AGREGA ESTO (Manda una COPIA del log a la consola de Cloud Run)
    handler_consola = logging.StreamHandler(sys.stdout) # <--- Mágica línea 1
    logger.addHandler(handler_consola)                  # <--- Mágica línea 2

    return logger, log_stream




def limpiar_nombre_archivo(nombre):
    nombre = re.sub(r'[\\/*?:"<>|]', "", nombre)
    return nombre.replace(" ", "_").strip()

def configurar_driver(download_dir):
    options = Options()
    options.add_argument('--headless=new') # Descomentar para Cloud Run
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
    
    driver = webdriver.Chrome(options=options)
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow", 
        "downloadPath": download_dir
    })
    return driver

# ==========================================
# 2. ACCIONES DE NAVEGACIÓN (SEIA)
# ==========================================

def realizar_busqueda(driver, wait, nombre, titular, f_pres):
    driver.get("https://seia.sea.gob.cl/busqueda/buscarProyecto.php")
    
    wait.until(EC.presence_of_element_located((By.ID, "projectName"))).send_keys(nombre)
    driver.find_element(By.ID, "nombreTitular").send_keys(titular)
    driver.find_element(By.ID, "startDateFechaP").send_keys(f_pres)
    driver.find_element(By.ID, "endDateFechaP").send_keys(f_pres)

    
    time.sleep(2)
    boton = driver.find_element(By.CSS_SELECTOR, "button.sg-btnForm")
    driver.execute_script("arguments[0].click();", boton)
    time.sleep(5)




def descargar_excel(driver, wait):
    try:
        link_excel = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Descargar en formato Excel")))
        driver.execute_script("arguments[0].click();", link_excel)
        time.sleep(2)
    except: pass

# ==========================================
# 3. EXTRACCIÓN Y CARGA (GCS)
# ==========================================

def procesar_documentos_detalle(driver, wait, bucket, id_proyecto, v_busqueda, v_ficha):
    enlaces = driver.find_elements(By.CSS_SELECTOR, "td.td-primary a")
    num_docs = len(enlaces)
    
    for index in range(num_docs):
        try:
            # Re-localizar elementos para evitar StaleElement
            link_elem = driver.find_elements(By.CSS_SELECTOR, "td.td-primary a")[index]
            url_doc = link_elem.get_attribute("href")
            nombre_limpio = limpiar_nombre_archivo(link_elem.text.strip())
            
            es_pdf = "firma.sea.gob.cl" in url_doc or url_doc.lower().endswith(".pdf")

            if es_pdf:
                nombre_f = f"DOC_{index+1}_{nombre_limpio}.pdf"
                session = requests.Session()
                session.headers.update({"User-Agent": driver.execute_script("return navigator.userAgent;"), "Referer": driver.current_url})
                for c in driver.get_cookies(): session.cookies.set(c['name'], c['value'])
                
                res = session.get(url_doc, timeout=30)
                if res.status_code == 200 and 'application/pdf' in res.headers.get('Content-Type', '').lower():
                    blob = bucket.blob(f"{id_proyecto}/documentos_detalle/{nombre_f}")
                    blob.content_disposition = f'attachment; filename="{nombre_f}"'
                    blob.upload_from_string(res.content, content_type='application/pdf')
            else:
                driver.execute_script("arguments[0].click();", link_elem)
                wait.until(EC.number_of_windows_to_be(3))
                for handle in driver.window_handles:
                    if handle not in [v_busqueda, v_ficha]:
                        driver.switch_to.window(handle); break
                
                time.sleep(3)
                html_doc = driver.page_source
                nombre_h = f"DOC_{index+1}_{nombre_limpio}.html"
                blob = bucket.blob(f"{id_proyecto}/documentos_detalle/{nombre_h}")
                blob.content_disposition = f'attachment; filename="{nombre_h}"'
                blob.upload_from_string(html_doc, content_type='text/html')
                driver.close()
                driver.switch_to.window(v_ficha)
            
            time.sleep(2)
        except:
            if len(driver.window_handles) > 2: driver.close()
            driver.switch_to.window(v_ficha)
    return num_docs


from bs4 import BeautifulSoup
import pandas as pd

def procesar_expediente_evaluacion(driver, wait, bucket, id_proyecto, v_busqueda, v_ficha):
    # flush=True OBLIGA a que el log aparezca INMEDIATAMENTE en Cloud Run
    print(f"🚀 [INICIO] Ejecutando scraper para ID: {id_proyecto}", flush=True)
    logger = logging.getLogger("scraper")
    
    try:
        # 1. Navegar a la pestaña Expediente
        print("📍 [PASO 1] Buscando pestaña 'Expediente'...", flush=True)
        tab_xpath = "//a[contains(@href, 'listadoExpediente') or contains(text(), 'Expediente')]"
        
        try:
            # Esperamos el botón
            tab_boton = wait.until(EC.element_to_be_clickable((By.XPATH, tab_xpath)))
            print("📍 [PASO 1] Botón encontrado. Click...", flush=True)
            driver.execute_script("arguments[0].click();", tab_boton)
        except Exception as e_click:
            print(f"❌ [ERROR FATAL] Falló click en pestaña Expediente: {e_click}", flush=True)
            # Intento de subir evidencia
            try:
                bucket.blob(f"{id_proyecto}/debug/ERROR_CLICK_PESTAÑA.txt").upload_from_string(str(e_click))
            except: pass
            return 0, None

        print("⏳ [ESPERA] 10 segundos para carga...", flush=True)
        time.sleep(10) 

        # ====================================================================
        # LOG VISUAL (CON FLUSH)
        # ====================================================================
        print(f"📸 [LOG] Generando LOG_1_VISTA_EXPEDIENTE.txt...", flush=True)
        try:
            debug_html = driver.page_source
            ruta_blob = f"{id_proyecto}/debug/LOG_1_VISTA_EXPEDIENTE.txt"
            
            blob = bucket.blob(ruta_blob)
            blob.upload_from_string(debug_html, content_type='text/plain')
            
            print(f"✅ [LOG] ¡ÉXITO! HTML guardado en bucket.", flush=True)
        except Exception as e_log:
            print(f"❌ [LOG] Error subiendo log al bucket: {e_log}", flush=True)
        # ====================================================================

        # 2. ENTRAR AL IFRAME (Si existe)
        # Tu caso 'no_funciono.txt' no tiene iframe, así que esto fallará rápido y seguirá.
        print("📍 [PASO 2] Buscando iframe...", flush=True)
        try:
            iframe = driver.find_element(By.XPATH, "//iframe[contains(@src, 'xhr_expediente')]")
            driver.switch_to.frame(iframe)
            print("✅ [PASO 2] Entramos al Iframe.", flush=True)
        except:
            print("ℹ️ [PASO 2] No hay iframe. Buscando en principal.", flush=True)

        # 3. BUSCAR BOTÓN XML
        print("📍 [PASO 3] Buscando botón XML...", flush=True)
        link_xml = None
        try:
            btn_xml = driver.find_element(By.XPATH, "//a[contains(@href, 'getXmlFile') or contains(@class, 'button_1')]")
            link_xml = btn_xml.get_attribute('href')
        except:
            pass

        # 4. PROCESAR
        if not link_xml:
            print(f"⚠️ [RESULTADO] NO se encontró botón XML (Caso esperado en algunos exp).", flush=True)
            
            # Dejamos constancia
            try:
                bucket.blob(f"{id_proyecto}/expediente/AVISO_NO_EXISTE_XML.txt").upload_from_string("Sin botón XML.")
            except: pass
            
            driver.switch_to.default_content()
            return 1, None

        print(f"✅ [PASO 3] Enlace XML: {link_xml}", flush=True)

        # 5. DESCARGAR XML
        driver.switch_to.default_content()
        
        session = requests.Session()
        ua = driver.execute_script("return navigator.userAgent;")
        session.headers.update({"User-Agent": ua})
        for c in driver.get_cookies(): session.cookies.set(c['name'], c['value'])

        print("📍 [PASO 4] Descargando archivo...", flush=True)
        res_xml = session.get(link_xml, timeout=60, verify=False)

        if res_xml.status_code == 200:
            nombre_archivo = f"Expediente_{id_proyecto}.xml"
            blob = bucket.blob(f"{id_proyecto}/expediente/{nombre_archivo}")
            blob.content_disposition = f'attachment; filename="{nombre_archivo}"'
            blob.upload_from_string(res_xml.content, content_type='text/xml')
            
            print(f"✅ [FIN] XML guardado: {nombre_archivo}", flush=True)
            return 1, None
        else:
            print(f"❌ [ERROR] Falló descarga HTTP: {res_xml.status_code}", flush=True)
            return 0, None

    except Exception as e:
        print(f"❌ [CRASH] Excepción NO controlada: {e}", flush=True)
        traceback.print_exc()
        return 0, None
    
# ==========================================
# 4. FUNCIÓN PRINCIPAL (ORQUESTADOR)
# ==========================================

def ejecutar_scrapping(id_proyecto, nombre_proyecto, titular, fecha_presentacion, bucket_name="almacen_antecedentes"):
    """
    CONTRATO: Esta función mantiene los 6 argumentos requeridos por app.py
    """
    logger, log_stream = obtener_logger()
    driver = None
    
    try:
        # Preparación de fechas y entorno
        fecha_p_str = fecha_presentacion.strftime('%d/%m/%Y') if hasattr(fecha_presentacion, 'strftime') else str(fecha_presentacion)
        
        download_dir = "/tmp" if os.environ.get("K_SERVICE") else os.path.join(os.getcwd(), "downloads")
        if not os.path.exists(download_dir): os.makedirs(download_dir)

        # Inicializar GCS y Selenium
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        driver = configurar_driver(download_dir)
        wait = WebDriverWait(driver, 20)

        # Paso 1: Búsqueda
        realizar_busqueda(driver, wait, nombre_proyecto, titular, fecha_p_str)
        ventana_busqueda = driver.current_window_handle

        # --- DETECCIÓN DE TABLA VACÍA ---
        celda_vacia = driver.find_elements(By.CSS_SELECTOR, "td.dt-empty")
        if celda_vacia:
            params_err = (
                f"1. **Nombre:** {nombre_proyecto}\n"
                f"2. **Titular:** {titular}\n"
                f"3. **F. Presentación:** {fecha_p_str}\n"
            )

            
            return f"⚠️ SIN RESULTADOS|{params_err}", log_stream.getvalue(), None

        # Paso 2: Excel
        descargar_excel(driver, wait)

        # Paso 3: Navegación a Ficha
        link_ficha = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "td.dt-head-center a.color-primary")))
        driver.execute_script("arguments[0].click();", link_ficha)
        wait.until(EC.number_of_windows_to_be(2))
        
        for handle in driver.window_handles:
            if handle != ventana_busqueda:
                driver.switch_to.window(handle); break
        
        ventana_ficha = driver.current_window_handle
        time.sleep(5)

        # Subir Ficha HTML
        blob_ficha = bucket.blob(f"{id_proyecto}/ficha_principal.html")
        blob_ficha.content_disposition = 'attachment; filename="ficha_principal.html"'
        blob_ficha.upload_from_string(driver.page_source, content_type='text/html')

        # Paso 4: Procesar documentos de la ficha principal
        num_docs_ficha = procesar_documentos_detalle(driver, wait, bucket, id_proyecto, ventana_busqueda, ventana_ficha)

        # NUEVO Paso 4.5: Procesar el expediente de evaluación (nueva función)
        num_docs_expediente, fecha_max_expediente = procesar_expediente_evaluacion(driver, wait, bucket, id_proyecto, ventana_busqueda, ventana_ficha)
        # Sumamos ambos conteos
        total_docs = num_docs_ficha + num_docs_expediente

        # Paso 5: Subir Excel descargado
        excel_local_path = None
        for f in os.listdir(download_dir):
            if f.endswith(".xlsx"):
                excel_local_path = os.path.join(download_dir, f)
                blob_xlsx = bucket.blob(f"{id_proyecto}/{f}")
                blob_xlsx.content_disposition = f'attachment; filename="{f}"'
                blob_xlsx.upload_from_filename(excel_local_path)
                break

        ruta_gcs = f"gs://{bucket_name}/{id_proyecto}/"
        console_url = f"https://console.cloud.google.com/storage/browser/{bucket_name}/{id_proyecto}?project={storage_client.project}"
        
        # Retornamos el total consolidado de documentos
        res_str = f"✅ EXITOSO|{ruta_gcs}|{console_url}|{total_docs}"
        return res_str, log_stream.getvalue(), excel_local_path

    except Exception as e:
        if driver:
            error_img = f"error_{id_proyecto}.png"
            driver.save_screenshot(error_img)
        return f"❌ ERROR: {str(e)}", log_stream.getvalue(), None
    finally:
        if driver: driver.quit()