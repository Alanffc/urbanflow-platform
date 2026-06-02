#!/bin/bash
# Shell script to configure InfluxDB 2.x bucket and retention policies.
# This configures telemetry storage rules for UrbanFlow.

set -e

# InfluxDB Environment Variables (matching values declared in docker-compose.yml)
INFLUX_HOST="http://localhost:8086"
INFLUX_ORG="urbanflow_org"
INFLUX_TOKEN="urbanflow_super_secret_token_12345"

echo "Initializing InfluxDB Telemetry Configuration..."

# 1. Create Telemetry Bucket if it does not exist (Default is created on setup, but we make sure)
# Retention period is set to 30 days (30d = 720h) for raw GPS ping tracking.
# Older metrics are aggregated or migrated to the S3 Data Lake.
influx bucket create \
  --host "$INFLUX_HOST" \
  --token "$INFLUX_TOKEN" \
  --org "$INFLUX_ORG" \
  --name "telemetry_bucket" \
  --retention "720h" \
  || echo "Bucket telemetry_bucket already exists or initialization skipped."

# 2. Create a separate bucket for aggregated KPIs (e.g. hourly congestion indexes, emissions saved)
# This has a longer retention period (365 days = 8760h)
influx bucket create \
  --host "$INFLUX_HOST" \
  --token "$INFLUX_TOKEN" \
  --org "$INFLUX_ORG" \
  --name "analytics_kpis" \
  --retention "8760h" \
  || echo "Bucket analytics_kpis already exists or initialization skipped."

echo "InfluxDB Telemetry Buckets configured successfully."
echo "Raw GPS telemetry: 30 days retention policy."
echo "KPI aggregated analytics: 365 days retention policy."
