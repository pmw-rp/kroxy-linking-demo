SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd $SCRIPT_DIR

# Before failover, this topic was shadowed from source's "foo" through the Kroxylicious proxy
# virtual cluster, which presents every topic with a "p_" prefix - so the real topic here is
# "p_foo", not "foo".

# Produce some records to the source
seq 3 5 | rpk --profile shadow topic produce p_foo

# Consume the source cluster for 10 seconds
timeout 10s rpk --profile shadow topic consume p_foo -g consumer-group-foo || true

popd