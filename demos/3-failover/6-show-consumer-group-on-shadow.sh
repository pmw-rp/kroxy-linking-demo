SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd $SCRIPT_DIR

source ../../config

# The Kroxylicious proxy virtual cluster renames consumer groups the same way it renames topics,
# so the group Shadow Link replicated is named "p_consumer-group-foo" on the shadow cluster.
echo On shadow cluster:
rpk --profile shadow group describe p_consumer-group-foo

popd