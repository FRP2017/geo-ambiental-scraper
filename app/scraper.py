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
    """
    1. Obtiene ID SEIA desde URL.
    2. Descarga Tabla.
    3. Itera documentos:
       - Si es PDF -> Guarda .pdf
       - Si no es PDF -> Guarda el código fuente como .html
    4. Sube Excel índice.
    """
    print(f"🚀 [INICIO] Descarga Híbrida (PDF/HTML) para: {id_proyecto}", flush=True)
    
    # --- 1. OBTENER ID SEIA ---
    try:
        if driver.current_window_handle != v_ficha:
            driver.switch_to.window(v_ficha)
        
        url_actual = driver.current_url
        match = re.search(r"id_expediente=(\d+)", url_actual)
        
        if match:
            id_seia = match.group(1)
        else:
            print("   ⚠️ No se pudo extraer ID SEIA.", flush=True)
            return 0, None
    except Exception as e:
        print(f"   ❌ Error ID URL: {e}", flush=True)
        return 0, None

    # --- 2. PREPARAR SESIÓN ---
    url_tabla = f"https://seia.sea.gob.cl/expediente/xhr_documentos.php?id_expediente={id_seia}"
    session = requests.Session()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": url_actual
    }
    
    for cookie in driver.get_cookies():
        session.cookies.set(cookie['name'], cookie['value'])

    try:
        # --- 3. OBTENER LISTADO ---
        print(f"   ⏳ Obteniendo listado: {url_tabla}...", flush=True)
        response = session.get(url_tabla, headers=headers, timeout=30)
        
        if response.status_code != 200:
            print(f"   ❌ Error HTTP {response.status_code}", flush=True)
            return 0, None

        soup = BeautifulSoup(response.text, 'html.parser')
        tabla = soup.find('table', {'id': 'tbldocumentos'})
        
        if not tabla:
            return 0, None

        datos = []
        rows = tabla.find_all('tr')[1:] 

        for tr in rows:
            cols = tr.find_all('td')
            if len(cols) < 7: continue
            
            try:
                celda_doc = cols[3]
                nombre_visual = celda_doc.get_text(strip=True)
                link_tag = celda_doc.find('a')
                fecha = cols[6].get_text(strip=True)
                
                enlace = ""
                if link_tag and 'href' in link_tag.attrs:
                    ruta = link_tag['href'].replace(r"\'", "").replace(r"'", "")
                    enlace = ruta if ruta.startswith("http") else f"https://seia.sea.gob.cl{ruta}"

                if enlace:
                    datos.append({
                        "Fecha": fecha,
                        "Documento": nombre_visual,
                        "Enlace": enlace
                    })
            except:
                continue

        print(f"   📊 Documentos detectados: {len(datos)}. Iniciando descarga...", flush=True)

        # --- 4. DESCARGA INTELIGENTE (PDF vs HTML) ---
        contador_exitos = 0
        
        for i, doc in enumerate(datos, 1):
            url_archivo = doc['Enlace']
            nombre_tabla = doc['Documento']
            
            # Limpiamos nombre de caracteres prohibidos
            nombre_limpio = re.sub(r'[\\/*?:"<>|]', "", nombre_tabla).strip()
            
            print(f"      ⬇️ [{i}/{len(datos)}] Procesando: {nombre_limpio}...", flush=True)
            
            try:
                # stream=True es vital para no cargar PDFs gigantes en memoria de golpe
                res_file = session.get(url_archivo, headers=headers, stream=True, timeout=60)
                
                if res_file.status_code == 200:
                    content_type = res_file.headers.get('Content-Type', '').lower()
                    
                    # LOGICA DE DECISIÓN
                    if 'pdf' in content_type:
                        # ES PDF
                        extension = ".pdf"
                        mime_type = 'application/pdf'
                        contenido = res_file.content # Binario
                    else:
                        # NO ES PDF -> ASUMIMOS HTML (Extraer código)
                        extension = ".html"
                        mime_type = 'text/html; charset=utf-8'
                        # Usamos .content para obtener los bytes crudos y que no se rompan los acentos
                        contenido = res_file.content 
                    
                    # Evitar duplicar extensión si el nombre ya la trae
                    if nombre_limpio.lower().endswith(extension):
                        nombre_final = f"{i:03d}_{nombre_limpio}"
                    else:
                        nombre_final = f"{i:03d}_{nombre_limpio}{extension}"
                    
                    # Subir a GCS
                    ruta_blob = f"{id_proyecto}/expediente_docs/{nombre_final}"
                    blob_file = bucket.blob(ruta_blob)
                    blob_file.upload_from_string(contenido, content_type=mime_type)
                    
                    contador_exitos += 1
                else:
                    print(f"      ⚠️ Link roto ({res_file.status_code})", flush=True)

            except Exception as e:
                print(f"      ⚠️ Error descargando {i}: {e}", flush=True)
                continue

        # --- 5. GENERAR INDICE EXCEL ---
        if datos:
            df = pd.DataFrame(datos)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            output.seek(0)
            
            nombre_indice = f"Indice_Expediente_{id_seia}.xlsx"
            blob_idx = bucket.blob(f"{id_proyecto}/expediente/{nombre_indice}")
            blob_idx.upload_from_string(output.getvalue(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            
            return len(datos), None

        return 0, None

    except Exception as e:
        print(f"   ❌ Error General: {e}", flush=True)
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