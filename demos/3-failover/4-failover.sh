SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd $SCRIPT_DIR

source ../../config

rpk --profile shadow shadow failover shadow-link --all --no-confirm
sleep 5
rpk --profile shadow shadow status shadow-link

popd