sudo apt install jq -y
curl -sX PUT "http://localhost:8083/connectors/source-postgres/config" \
-d @connectors/source-postgres.json -H "Content-Type: application/json" | jq 
curl -sX PUT "http://localhost:8083/connectors/sink-bigquery/config" \
-d @connectors/sink-bigquery.json -H "Content-Type: application/json" | jq