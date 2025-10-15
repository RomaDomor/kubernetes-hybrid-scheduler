#!/bin/bash
set -euo pipefail

NAMESPACE=${NAMESPACE:-kube-system}
SERVICE=${SERVICE:-smart-scheduler-webhook}
SECRET=${SECRET:-smart-scheduler-certs}
CSR_NAME=${CSR_NAME:-smart-scheduler-webhook-csr}

echo "[INFO] Generating private key"
openssl genrsa -out server.key 2048

cat > csr.conf <<EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = ${SERVICE}
DNS.2 = ${SERVICE}.${NAMESPACE}
DNS.3 = ${SERVICE}.${NAMESPACE}.svc
DNS.4 = ${SERVICE}.${NAMESPACE}.svc.cluster.local
EOF

echo "[INFO] Generating CSR"
openssl req -new -key server.key \
  -subj "/CN=${SERVICE}.${NAMESPACE}.svc" \
  -out server.csr \
  -config csr.conf

echo "[INFO] Deleting any previous CSR ${CSR_NAME} (if exists)"
kubectl delete csr ${CSR_NAME} 2>/dev/null || true

echo "[INFO] Creating Kubernetes CSR ${CSR_NAME}"
cat <<EOF | kubectl apply -f -
apiVersion: certificates.k8s.io/v1
kind: CertificateSigningRequest
metadata:
  name: ${CSR_NAME}
spec:
  request: $(cat server.csr | base64 | tr -d '\n')
  signerName: kubernetes.io/kubelet-serving
  usages:
  - digital signature
  - key encipherment
  - server auth
EOF

echo "[INFO] Approving CSR"
kubectl certificate approve ${CSR_NAME}

echo "[INFO] Waiting for issued certificate..."
for i in {1..60}; do
  CERT=$(kubectl get csr ${CSR_NAME} -o jsonpath='{.status.certificate}' || true)
  if [ -n "$CERT" ]; then
    echo "$CERT" | base64 -d > server.crt
    break
  fi
  sleep 1
done

if [ ! -s server.crt ]; then
  echo "[ERROR] Failed to obtain certificate from CSR"
  exit 1
fi

echo "[INFO] Extracting cluster CA bundle"
kubectl config view --raw --minify --flatten \
  -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' \
  | base64 -d > ca.crt

echo "[INFO] Creating/Updating Secret ${SECRET} in ${NAMESPACE}"
kubectl create secret generic ${SECRET} \
  --from-file=tls.crt=server.crt \
  --from-file=tls.key=server.key \
  --from-file=ca.crt=ca.crt \
  -n ${NAMESPACE} \
  --dry-run=client -o yaml | kubectl apply -f -

CA_BUNDLE=$(cat ca.crt | base64 | tr -d '\n')
echo "[INFO] Patching MutatingWebhookConfiguration with CA bundle"
sed "s/CA_BUNDLE_PLACEHOLDER/${CA_BUNDLE}/" manifests/webhook-config.yaml | kubectl apply -f -

echo "[INFO] Done. Secret '${SECRET}' created and webhook configured."

rm -f server.key server.csr server.crt ca.crt csr.conf