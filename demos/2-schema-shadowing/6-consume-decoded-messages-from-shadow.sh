SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd $SCRIPT_DIR 1>/dev/null 2>/dev/null

source ../../config

# Shadow Link reads the source cluster through the Kroxylicious proxy virtual cluster, which
# presents every topic with a "p_" prefix - so the shadowed "syslog" topic is "p_syslog" here.
# The "_schemas" topic that backs schema lookups is left unprefixed by the proxy on purpose, so
# --use-schema-registry keeps working unmodified.
rpk --profile shadow topic consume p_syslog -o0 -n1 --use-schema-registry -f'%v\n' | jq

popd 1>/dev/null 2>/dev/null