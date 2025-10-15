#!/bin/bash
set -euo pipefail

NAMESPACE=${NAMESPACE:-kube-system}
SERVICE=${SERVICE:-smart-scheduler-webhook}
SECRET=${SECRET:-smart-scheduler-certs}
WEBHOOK_CFG=${WEBHOOK_CFG:-manifests/webhook-config.yaml}

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

cd "$WORKDIR"

echo "[INFO] Generating self-signed CA"
openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -subj "/CN=${SERVICE}.${NAMESPACE}.svc-ca" -days 3650 -out ca.crt

cat >server.conf <<EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = ${SERVICE}
DNS.2 = ${SERVICE}.${NAMESPACE}
DNS.3 = ${SERVICE}.${NAMESPACE}.svc
DNS.4 = ${SERVICE}.${NAMESPACE}.svc.cluster.local
EOF

echo "[INFO] Generating server key and CSR"
openssl genrsa -out server.key 2048
openssl req -new -key server.key -subj "/CN=${SERVICE}.${NAMESPACE}.svc" -out server.csr -config server.conf

echo "[INFO] Signing server cert with CA"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 365 -extensions v3_req -extfile server.conf

echo "[INFO] Creating/Updating Secret ${SECRET} in ${NAMESPACE}"
kubectl create secret tls ${SECRET} \
  --cert=server.crt \
  --key=server.key \
  -n ${NAMESPACE} \
  --dry-run=client -o yaml | kubectl apply -f -

# Also store ca.crt in the same secret (optional but handy)
kubectl patch secret ${SECRET} -n ${NAMESPACE} --type=json \
  -p='[{"op":"add","path":"/data/ca.crt","value":"'"$(base64 -w0 ca.crt)"'"}]' || true

CA_BUNDLE=$(base64 -w0 ca.crt)
echo "[INFO] Applying webhook configuration with CA bundle"
sed "s/CA_BUNDLE_PLACEHOLDER/${CA_BUNDLE}/" "${WEBHOOK_CFG}" | kubectl apply -f -

echo "[INFO] Done. Secret '${SECRET}' created/updated and webhook configured."