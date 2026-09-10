SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd $SCRIPT_DIR

source ./config

kind create cluster --config resources/kind.yaml

# Install Cert Manager

helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install cert-manager jetstack/cert-manager \
  --set crds.enabled=true \
  --namespace ${CERT_MANAGER_NAMESPACE} \
  --create-namespace

# Install Redpanda Operator

helm repo add redpanda https://charts.redpanda.com
helm repo update
helm upgrade --install redpanda-controller redpanda/operator \
  --namespace ${OPERATOR_NAMESPACE} \
  --create-namespace \
  --version v26.2.2 \
  --set crds.enabled=true

# Build the custom Kroxylicious proxy image (adds our topic-prefixer filter to the classpath -
# see kroxylicious/topic-prefixer/) and load it into the kind cluster so it can be pulled locally.

docker build -t ${KROXYLICIOUS_PROXY_IMAGE} kroxylicious/topic-prefixer
kind load docker-image ${KROXYLICIOUS_PROXY_IMAGE}

# Install Kroxylicious Operator

kubectl apply -f resources/kroxylicious-operator.yaml
kubectl wait -n ${KROXYLICIOUS_OPERATOR_NAMESPACE} deployment/kroxylicious-operator --for=condition=Available --timeout=120s

# Install Source Cluster

kubectl apply -f resources/source-cluster.yaml
kubectl wait -n ${SOURCE_REDPANDA_NAMESPACE} redpanda/redpanda --for=condition=Ready --timeout=180s

# Install Shadow Cluster

kubectl apply -f resources/shadow-cluster.yaml
kubectl wait -n ${SHADOW_REDPANDA_NAMESPACE} redpanda/redpanda --for=condition=Ready --timeout=180s

# Install Kroxylicious proxy virtual cluster (fronts the source cluster - see
# resources/kroxylicious-proxy.yaml). The ShadowLink below reads from this instead of talking to
# the source cluster directly.

kubectl apply -f resources/kroxylicious-proxy.yaml
kubectl wait -n ${KROXYLICIOUS_NAMESPACE} kafkaproxy/kroxy --for=condition=Ready --timeout=120s

# Create Shadow Link

kubectl apply -f resources/shadow-link.yaml

kubectl port-forward pod/redpanda-0 -n $SOURCE_REDPANDA_NAMESPACE 19094:9094 19644:9644 18081:8081 1>/dev/null 2>/dev/null &
echo $! > local-port-forward.pid
rpk profile create source -s brokers=localhost:19094 -s admin.hosts=localhost:19644 -s registry.hosts=http://localhost:18081/ 1>/dev/null 2>/dev/null || rpk profile use source
rpk cluster health

kubectl port-forward pod/redpanda-0 -n $SHADOW_REDPANDA_NAMESPACE 29094:9094 29644:9644 28081:8081 1>/dev/null 2>/dev/null &
echo $! > local-port-forward.pid
rpk profile create shadow -s brokers=localhost:29094 -s admin.hosts=localhost:29644 -s registry.hosts=http://localhost:28081/ 1>/dev/null 2>/dev/null || rpk profile use shadow
rpk cluster health

popd