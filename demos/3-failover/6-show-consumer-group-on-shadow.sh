SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd $SCRIPT_DIR

source ../../config

echo On shadow cluster:
rpk --profile shadow group describe consumer-group-foo

popd