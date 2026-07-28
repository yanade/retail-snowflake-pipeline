#!/bin/bash
# Allows your current machine to connect to the cloud PostgreSQL Flexible Server.
# Run once per session after terraform apply — your IP can change between sessions.

set -e  # exit immediately if any command fails

LOCAL_IP=$(curl -s ifconfig.me)
echo "Adding firewall rule for IP: $LOCAL_IP..."

az postgres flexible-server firewall-rule create \
  --resource-group retail-pipeline-dev-rg \
  --server-name retail-pipeline-dev-pg \
  --name allow-local-dev \
  --start-ip-address "$LOCAL_IP" \
  --end-ip-address "$LOCAL_IP"

echo "Done. Your IP ($LOCAL_IP) can now reach retail-pipeline-dev-pg."
