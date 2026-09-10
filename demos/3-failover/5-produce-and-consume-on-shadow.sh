SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd $SCRIPT_DIR

# Before failover, this topic and consumer group were shadowed from source's "foo" /
# "consumer-group-foo" through the Kroxylicious proxy virtual cluster, which renames both topics
# and consumer groups with a "p_" prefix - so the real names here are "p_foo" and
# "p_consumer-group-foo", not "foo" and "consumer-group-foo".

# Produce some records to the source
seq 3 5 | rpk --profile shadow topic produce p_foo

# Consume the source cluster for 10 seconds
timeout 10s rpk --profile shadow topic consume p_foo -g p_consumer-group-foo || true

popd