#!/usr/bin/env bash
# Provisions the Azure resources for the ETL pipeline using the Azure CLI.
# NOT tested against a live subscription - review each command and check current `az` docs
# (e.g. Function App hosting plans change over time) before running.
#
#   az login
#   export SUFFIX=$RANDOM            # makes global names unique
#   export SQL_ADMIN_PASSWORD='<strong password>'
#   bash infra/deploy.sh
set -euo pipefail

: "${SQL_ADMIN_PASSWORD:?set SQL_ADMIN_PASSWORD}"
SUFFIX="${SUFFIX:-$RANDOM}"
LOCATION="${LOCATION:-centralindia}"
RG="rg-etl-demo"
STORAGE="stetl${SUFFIX}"             # 3-24 chars, lowercase letters/digits
SQL_SERVER="sql-etl-${SUFFIX}"
SQL_DB="salesdb"
SQL_ADMIN="etladmin"
FUNC_APP="func-etl-${SUFFIX}"

az group create -n "$RG" -l "$LOCATION"

# Storage account + the five containers used by the pipeline
az storage account create -n "$STORAGE" -g "$RG" -l "$LOCATION" --sku Standard_LRS --kind StorageV2 \
  --min-tls-version TLS1_2 --allow-blob-public-access false
STORAGE_CONN=$(az storage account show-connection-string -n "$STORAGE" -g "$RG" -o tsv)
for c in raw processed rejected archive quarantine; do
  az storage container create -n "$c" --connection-string "$STORAGE_CONN"
done

# Azure SQL (serverless, auto-pauses to save cost)
az sql server create -n "$SQL_SERVER" -g "$RG" -l "$LOCATION" -u "$SQL_ADMIN" -p "$SQL_ADMIN_PASSWORD"
az sql db create -g "$RG" -s "$SQL_SERVER" -n "$SQL_DB" --edition GeneralPurpose --family Gen5 \
  --compute-model Serverless --capacity 1 --auto-pause-delay 60
# Allow Azure services (the Function App) to reach the server. Tighten for production (private endpoint / VNet).
az sql server firewall-rule create -g "$RG" -s "$SQL_SERVER" -n AllowAzureServices \
  --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0

# Function App (Python 3.11, Functions v4)
az functionapp create -n "$FUNC_APP" -g "$RG" --storage-account "$STORAGE" \
  --consumption-plan-location "$LOCATION" --runtime python --runtime-version 3.11 \
  --functions-version 4 --os-type Linux

SQL_CONN="Driver={ODBC Driver 18 for SQL Server};Server=tcp:${SQL_SERVER}.database.windows.net,1433;Database=${SQL_DB};Uid=${SQL_ADMIN};Pwd=${SQL_ADMIN_PASSWORD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
az functionapp config appsettings set -n "$FUNC_APP" -g "$RG" --settings \
  ETL_MODE=azure "SQL_CONNECTION_STRING=${SQL_CONN}"
# AzureWebJobsStorage is set by `functionapp create` and is reused for the blob trigger and blob access.

echo "Deploy the code with:  func azure functionapp publish ${FUNC_APP}"
echo "Test by uploading a file:  az storage blob upload -c raw -f sample_data/sales_2026-10-24.csv -n sales_2026-10-24.csv --connection-string \"\$STORAGE_CONN\""
