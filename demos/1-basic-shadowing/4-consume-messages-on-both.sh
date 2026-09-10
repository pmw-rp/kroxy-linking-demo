SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd $SCRIPT_DIR

source ../../config

echo On source cluster:
rpk --profile source topic consume basic -n1

# Shadow Link now reads the source cluster through the Kroxylicious proxy virtual cluster, which
# presents every topic with a "p_" prefix - see resources/kroxylicious-proxy.yaml. So the topic
# shadowed from source's "basic" is named "p_basic" on the shadow cluster.
echo On shadow cluster:
rpk --profile shadow topic consume p_basic -n1

popd