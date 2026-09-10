SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd $SCRIPT_DIR

source ../../config

# Expect to see "p_basic", not "basic" - the ShadowLink reads the source cluster through the
# Kroxylicious proxy virtual cluster, which presents every topic with a "p_" prefix.
rpk --profile shadow topic list

popd