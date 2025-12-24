FROM python:3.9-slim

# Instalación de dependencias del sistema y Google Chrome
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    apt-transport-https \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome-main.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-main.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y \
    google-chrome-stable \
    libgbm1 \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copiar requerimientos e instalar dependencias de Python
# AQUÍ ESTABA EL ERROR: Asegúrate de que no haya nada después de requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del código
COPY . .

# Exponer puerto para Streamlit
EXPOSE 8080

# Ejecución
CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]