#!/usr/bin/env python3
''' ggac_surface.py
Author:     Connor Natzke
Date:       Mar 2021
Revision:   Mar 2021
Purpose:    Generate a Pegasus workflow for OSG submission
'''
# --- Configuration -----------------------------------------------------------
import logging
import os
from Pegasus.api import *
from pathlib import Path

logging.basicConfig(level=logging.DEBUG)

# --- Working Directory Setup -------------------------------------------------
# A good working directory for workflow runs and output files
WORK_DIR = Path.home() / "workflows"
WORK_DIR.mkdir(exist_ok=True)

TOP_DIR = Path(__file__).resolve().parent

# --- Properties --------------------------------------------------------------
props = Properties()
props["pegasus.monitord.encoding"] = "json"

# Provide full kickstart record, including environment, even for successful jobs
props["pegasus.gridstart.arguments"] = "-f"

# Limit number of idle jobs
props["dagman.maxidle"] = "1000"
props["dagman.maxjobs"] = "1000"

# Set retry limit
props["dagman.retry"] = "3"

# Help Pegasus developers by sharing performance data
props["pegasus.catalog.workflow.amqp.url"] = "amqp://friend:donatedata@msgs.pegasus.isi.edu:5672/prod/workflows"

# Write properties file to ./pegasus.properties
props.write()

# --- Sites -------------------------------------------------------------------
sc = SiteCatalog()

# local site (submit node)
local_site = Site(name="local", arch=Arch.X86_64)

local_shared_scratch = Directory(
    directory_type=Directory.SHARED_SCRATCH, path=WORK_DIR / "scratch")
local_shared_scratch.add_file_servers(FileServer(
    url="file://" + str(WORK_DIR / "scratch"), operation_type=Operation.ALL))
local_site.add_directories(local_shared_scratch)

local_storage = Directory(
    directory_type=Directory.LOCAL_STORAGE, path=WORK_DIR / "outputs")
local_storage.add_file_servers(FileServer(
    url="file://" + str(WORK_DIR / "outputs"), operation_type=Operation.ALL))
local_site.add_directories(local_storage)

local_site.add_env(PATH=os.environ["PATH"])
local_site.add_profiles(Namespace.PEGASUS, key='SSH_PRIVATE_KEY',
                        value='/home/cnatzke/.ssh/id_rsa.pegasus')
sc.add_sites(local_site)

# condorpool (execution nodes)
condorpool_site = Site(
    name="condorpool", arch=Arch.X86_64, os_type=OS.LINUX)
condorpool_site.add_pegasus_profile(style="condor")
condorpool_site.add_condor_profile(
    universe="vanilla",
    requirements="HAS_SINGULARITY == TRUE",
    request_cpus=1,
    request_memory="1 GB",
    request_disk="1 GB"
)
sc.add_sites(condorpool_site)

# remote server (for analysis)
remote_site = Site(
    name="remote", arch=Arch.X86_64, os_type=OS.LINUX)

remote_site.add_pegasus_profile(style="condor")
remote_storage = Directory(
    directory_type=Directory.LOCAL_STORAGE, path="/data_fast/cnatzke/Tests")
remote_storage.add_file_servers(FileServer(
    url="scp://cnatzke@cronos.mines.edu/data_fast/cnatzke/Tests", operation_type=Operation.ALL))
remote_site.add_directories(remote_storage)

sc.add_sites(remote_site)

# write SiteCatalog to ./sites.yml
sc.write()

# --- Transformations ---------------------------------------------------------
file_preparation = Transformation(
    name="file_preparation",
    site="local",
    pfn=TOP_DIR / "bin/prepare_files",
    is_stageable=True,
    arch=Arch.X86_64
)

simulation = Transformation(
    name="simulation",
    site="local",
    pfn=TOP_DIR / "bin/run_simulation",
    is_stageable=True,
    arch=Arch.X86_64
)

ntuple = Transformation(
    name="ntuple",
    site="local",
    pfn=TOP_DIR / "bin/run_ntuple",
    is_stageable=True,
    arch=Arch.X86_64
)

tc = TransformationCatalog()
tc.add_transformations(file_preparation, simulation, ntuple)

# write TransformationCatalog to ./transformations.yml
tc.write()

# --- Replicas ----------------------------------------------------------------
# Use all input files in "inputs" directory
input_files = [File(f.name) for f in (TOP_DIR / "inputs").iterdir()]

rc = ReplicaCatalog()
for f in input_files:
    rc.add_replica(site="local", lfn=f, pfn=TOP_DIR / "inputs" / f.lfn)

# write ReplicaCatalog to replicas.yml
rc.write()

# --- WorkFlow ----------------------------------------------------------------
jobs = 10
z_list = ['Z0', 'Z2', 'Z4']

wf = Workflow(name="ggac_surface-workflow")

for z in z_list:
    out_file_name_preparation = f'run_macro_{z}.mac'
    preparation_job = Job(file_preparation)\
        .add_args('simulation.cfg', z, out_file_name_preparation)\
        .add_inputs(File('simulation.cfg'))\
        .add_outputs(File(out_file_name_preparation))\
        .add_profiles(
            Namespace.CONDOR,
            key="+SingularityImage",
            value='"/cvmfs/singularity.opensciencegrid.org/cnatzke/prepare_files:latest"'
    )

    wf.add_jobs(preparation_job)

    for job in range(jobs):
        out_file_name_simulation = f'g4out_{z}_{job:03d}.root'
        out_file_name_ntuple = f'Converted_{z}_{job:03d}.root'

        simulation_job = Job(simulation)\
            .add_args(out_file_name_simulation)\
            .add_inputs(*input_files, File(out_file_name_preparation))\
            .add_outputs(File(out_file_name_simulation))\
            .add_profiles(
                Namespace.CONDOR,
                key="+SingularityImage",
                value='"/cvmfs/singularity.opensciencegrid.org/cnatzke/griffin_simulation:geant4.10.01"'
        )

        ntuple_job = Job(ntuple)\
            .add_args(out_file_name_simulation, out_file_name_ntuple)\
            .add_inputs(File(out_file_name_simulation))\
            .add_outputs(File(out_file_name_ntuple))\
            .add_profiles(
                Namespace.CONDOR,
                key="+SingularityImage",
                value='"/cvmfs/singularity.opensciencegrid.org/cnatzke/ntuple:ggac_surface"'
        )

        wf.add_jobs(simulation_job)
        wf.add_jobs(ntuple_job)

# plan workflow
wf.plan(
    dir=WORK_DIR / "runs",
    output_sites=["remote"],
    submit=True
)
