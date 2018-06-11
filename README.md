# twirl-cmsedm-test

## an example how to run

```bash
## enter cmsenv
source /cvmfs/cms.cern.ch/cmsset_default.sh
export SCRAM_ARCH=slc6_amd64_gcc530
cmsrel CMSSW_8_0_26
cd CMSSW_8_0_26/src
cmsenv
cd ../..

## check out this repo
git clone --recursive git@github.com:alphatwirl/twirl-cmsedm-test

## set env
source ./twirl-cmsedm-test/external/setup.sh

## run
./twirl-cmsedm-test/twirl.py --max-files-per-process 1 --max-events-per-process 50000 --parallel-mode htcondor --logging-level INFO

```
