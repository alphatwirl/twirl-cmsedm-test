#!/usr/bin/env python
# Tai Sakuma <tai.sakuma@cern.ch>
import os, sys
import argparse
import logging
import numpy
import pprint

##__________________________________________________________________||
import alphatwirl
import atnanoaod
import atcmsedm

##__________________________________________________________________||
parser = argparse.ArgumentParser()
parser.add_argument('--mc', action='store_const', dest='datamc', const='mc', default='mc', help='for processing MC')
parser.add_argument('--data', action='store_const', dest='datamc', const='data', help='for processing data')
parser.add_argument('--tbl-cmsdatasets', default=[ ], nargs='*', help='path to tbl_dataset_cmsdataset')
parser.add_argument('--datasets', default=None, nargs='*', help='list of data sets')
parser.add_argument('--tbl-pu-corr-path', help='path to the table of the PU corrections, MC only')
parser.add_argument('--susy-sms', action='store_true', default=False, help='add tables for SUSY mass points')
parser.add_argument('-o', '--outdir', default=os.path.join('tbl', 'out'))
parser.add_argument('-n', '--nevents', default=-1, type=int, help='maximum number of events to process for each component')
parser.add_argument('--max-events-per-process', default=-1, type=int, help='maximum number of events per process')
parser.add_argument('--max-files-per-dataset', default=-1, type=int, help='maximum number of files per data set')
parser.add_argument('--max-files-per-process', default=1, type=int, help='maximum number of files per process')
parser.add_argument('--force', action='store_true', default=False, help='recreate all output files')
parser.add_argument('--parallel-mode', default='multiprocessing', choices=['multiprocessing', 'subprocess', 'htcondor'], help='mode for concurrency')
parser.add_argument('-p', '--process', default=4, type=int, help='number of processes to run in parallel')
parser.add_argument('-q', '--quiet', default=False, action='store_true', help='quiet mode')
parser.add_argument('--profile', action='store_true', help='run profile')
parser.add_argument('--profile-out-path', default=None, help='path to write the result of profile')
parser.add_argument('--logging-level', default='WARN', choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help='level for logging')
parser.add_argument('--no-run', action='store_true', default=False, help='do not actually run')
args = parser.parse_args()

##__________________________________________________________________||
def main():

    configure_logger()

    reader_collector_pairs = configure_reader_collector_pairs()

    datasets = configure_datasets()

    if not args.no_run:
        run(reader_collector_pairs, datasets)

##__________________________________________________________________||
def configure_logger():

    log_level = logging.getLevelName(args.logging_level)
    log_handler = logging.StreamHandler()
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(log_formatter)

    names_for_logger = ['alphatwirl', 'atnanoaod', 'atcmsedm']
    for n in names_for_logger:
        logger = logging.getLogger(n)
        logger.setLevel(log_level)
        logger.handlers[:] = [ ]
        logger.addHandler(log_handler)

##__________________________________________________________________||
def configure_reader_collector_pairs():

    ret = [ ]

    ret.extend(configure_scribblers_before_event_selection())

    ret.extend(configure_tables_after_1st_event_selection())

    path = os.path.join(args.outdir, 'reader_collector_pairs.txt')
    alphatwirl.mkdir_p(os.path.dirname(path))
    with open(path, 'w') as f:
        pprint.pprint(ret, stream=f)

    return ret

##__________________________________________________________________||
def configure_scribblers_before_event_selection():

    scribblers = [
        atcmsedm.scribblers.EventAuxiliary(),
        atcmsedm.scribblers.MET(),
    ]

    ret = [(r, alphatwirl.loop.NullCollector()) for r in scribblers]
    return ret
##__________________________________________________________________||
def configure_tables_after_1st_event_selection():

    #
    Round = alphatwirl.binning.Round
    RoundLog = alphatwirl.binning.RoundLog

    #
    tblcfg = [
        dict(keyAttrNames=('met', ),
             binnings=(RoundLog(0.1, 100, min = 20), ),
        ),
    ]

    tableConfigCompleter = alphatwirl.configure.TableConfigCompleter(
        defaultSummaryClass=alphatwirl.summary.Count,
        defaultOutDir=args.outdir,
        createOutFileName=alphatwirl.configure.TableFileNameComposer(default_prefix='tbl_n_component')
    )

    tblcfg = [tableConfigCompleter.complete(c) for c in tblcfg]
    if not args.force:
        tblcfg = [c for c in tblcfg if c['outFile'] and not os.path.exists(c['outFilePath'])]

    ret = [alphatwirl.configure.build_counter_collector_pair(c) for c in tblcfg]
    return ret

##__________________________________________________________________||
def configure_datasets():

    ret = atnanoaod.query.build_datasets_from_tbl_paths(
        tbl_cmsdataset_paths=args.tbl_cmsdatasets,
        datasets=args.datasets if args.datasets else None
        # give None to datasets if args.datasets is an empty list
        # so that build_datasets() returns all datasets rather than
        # an empty list.
    )

    # 
    # ret = [
    #     Dataset(name='QCD_HT500to700', files=[
    #         '/store/mc/RunIISummer16MiniAODv2/QCD_HT500to700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/70000/000316AF-9FBE-E611-9761-0CC47A7C35F8.root',
    #         '/store/mc/RunIISummer16MiniAODv2/QCD_HT500to700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/70000/001E88A3-96BE-E611-8F2D-0025905B85EC.root',
    #         '/store/mc/RunIISummer16MiniAODv2/QCD_HT500to700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/70000/0032A7A3-9ABE-E611-9732-0CC47A4D765A.root',
    #     ]),
    #     Dataset(name='QCD_HT700to1000', files=[
    #         '/store/mc/RunIISummer16MiniAODv2/QCD_HT700to1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/50000/00D17FD4-8EBD-E611-B17D-002590D0AFC2.root',
    #         '/store/mc/RunIISummer16MiniAODv2/QCD_HT700to1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/50000/0258B60A-8FBD-E611-8B0B-24BE05C6C7F1.root',
    #         '/store/mc/RunIISummer16MiniAODv2/QCD_HT700to1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/50000/0413591E-99BD-E611-B281-0CC47A4C8ECA.root',
    #         ]),
    # ]

    # ret = [
    #     atnanoaod.dataset.Dataset(name='QCD_HT500to700', files=[
    #         'root://cms-xrd-global.cern.ch//store/mc/RunIISummer16MiniAODv2/QCD_HT500to700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/70000/000316AF-9FBE-E611-9761-0CC47A7C35F8.root',
    #         'root://cms-xrd-global.cern.ch//store/mc/RunIISummer16MiniAODv2/QCD_HT500to700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/70000/001E88A3-96BE-E611-8F2D-0025905B85EC.root',
    #         'root://cms-xrd-global.cern.ch//store/mc/RunIISummer16MiniAODv2/QCD_HT500to700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/70000/0032A7A3-9ABE-E611-9732-0CC47A4D765A.root',
    #     ]),
    #     atnanoaod.dataset.Dataset(name='QCD_HT700to1000', files=[
    #         'root://cms-xrd-global.cern.ch//store/mc/RunIISummer16MiniAODv2/QCD_HT700to1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/50000/00D17FD4-8EBD-E611-B17D-002590D0AFC2.root',
    #         'root://cms-xrd-global.cern.ch//store/mc/RunIISummer16MiniAODv2/QCD_HT700to1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/50000/0258B60A-8FBD-E611-8B0B-24BE05C6C7F1.root',
    #         'root://cms-xrd-global.cern.ch//store/mc/RunIISummer16MiniAODv2/QCD_HT700to1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/50000/0413591E-99BD-E611-B281-0CC47A4C8ECA.root',
    #         ]),
    # ]

    path = os.path.join(args.outdir, 'datasets.txt')
    if args.force or not os.path.exists(path):
        alphatwirl.mkdir_p(os.path.dirname(path))
        with open(path, 'w') as f:
            pprint.pprint(ret, stream=f)

    return ret

##__________________________________________________________________||
def run(reader_collector_pairs, datasets):

    htcondor_job_desc_extra_request = ['request_memory = 250']

    # https://lists.cs.wisc.edu/archive/htcondor-users/2014-June/msg00133.shtml
    # hold a job and release to a different machine after a certain minutes
    htcondor_job_desc_extra_resubmit = [
        'expected_runtime_minutes = 20',
        'use_x509userproxy = true',
        'job_machine_attrs = Machine',
        'job_machine_attrs_history_length = 4',
        'requirements = target.machine =!= MachineAttrMachine1 && target.machine =!= MachineAttrMachine2 &&  target.machine =!= MachineAttrMachine3',
        'periodic_hold = JobStatus == 2 && CurrentTime - EnteredCurrentStatus > 60 * $(expected_runtime_minutes)',
        'periodic_hold_subcode = 1',
        'periodic_release = HoldReasonCode == 3 && HoldReasonSubCode == 1 && JobRunCount < 3',
        'periodic_hold_reason = ifthenelse(JobRunCount<3,"Ran too long, will retry","Ran too long")',
    ]

    # http://www.its.hku.hk/services/research/htc/jobsubmission
    # avoid the machines "smXX.hadoop.cluster"
    # operator '=!=' explained at https://research.cs.wisc.edu/htcondor/manual/v7.8/4_1HTCondor_s_ClassAd.html#ClassAd:evaluation-meta
    htcondor_job_desc_extra_blacklist = [
        'requirements=!stringListMember(substr(Target.Machine, 0, 2), "sm,bs")'
    ]

    ## htcondor_job_desc_extra = htcondor_job_desc_extra_request + htcondor_job_desc_extra_resubmit
    htcondor_job_desc_extra = htcondor_job_desc_extra_request + htcondor_job_desc_extra_blacklist

    fw = atcmsedm.AtCMSEDM(
        quiet=args.quiet,
        parallel_mode=args.parallel_mode,
        htcondor_job_desc_extra=htcondor_job_desc_extra,
        process=args.process,
        user_modules=('atnanoaod', 'atcmsedm'),
        max_events_per_dataset=args.nevents,
        max_events_per_process=args.max_events_per_process,
        max_files_per_dataset=args.max_files_per_dataset,
        max_files_per_process=args.max_files_per_process,
        profile=args.profile,
        profile_out_path=args.profile_out_path
    )
    fw.run(
        datasets=datasets,
        reader_collector_pairs=reader_collector_pairs
    )

##__________________________________________________________________||
if __name__ == '__main__':
    main()
