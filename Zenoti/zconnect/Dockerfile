# The zconnect service: pull the HyperConnect reports, load them into the
# BRONZE_ZENOTI_HC_* tables. Its own image on purpose -- the login needs a
# browser, and the pipeline's image (../Dockerfile) should not carry one.
#
# Build context must be the folder above (Zenoti/), not this one. zconnect reaches
# outside itself at run time: the helpers in zconnect/hc_sql_helper/ put Zenoti/
# and Zenoti/dbquery/ on sys.path to import db_helper and
# import_aesthetix_opportunity_history. A context of zconnect/ alone would break
# every load with an ImportError. On Railway: Root Directory Zenoti,
# Dockerfile Path zconnect/Dockerfile.
FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y curl gnupg2 apt-transport-https unixodbc unixodbc-dev && \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY zconnect/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# pip install playwright ships no browser. --with-deps adds the shared libraries
# headless Chromium needs on slim; it needs apt, so it runs as root here.
# login_headers.py drops channel="chrome" when RAILWAY_ENVIRONMENT is set, which
# is what makes this bundled Chromium the one it launches.
RUN playwright install --with-deps chromium

COPY . .

# No dates: sync_hc.py computes T-7..T-1 itself, which is the deployed case.
CMD ["python", "zconnect/sync_hc.py"]
