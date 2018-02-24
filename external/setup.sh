##__________________________________________________________________||
# source ./setup.sh

thisdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# # absolute path to this dir, e.g., "/abs/path/bdphi-scripts/external"

# ##__________________________________________________________________||
export PYTHONPATH="$thisdir"/alphatwirl:$PYTHONPATH
export PYTHONPATH="$thisdir"/atcmsedm:$PYTHONPATH
export PYTHONPATH="$thisdir"/atnanoaod:$PYTHONPATH

##__________________________________________________________________||
