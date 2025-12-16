#!/bin/bash
#
#SBATCH --job-name=current_disk_1_rocksdb_build
#SBATCH --output=rocksdb_disk_1_res.txt
#
#SBATCH --ntasks=1
#SBATCH --time=720:00:00
#SBATCH --mem-per-cpu=820000


cd /homes/mahfouz/code/branchwater
cargo run -p branchwater-index index  -k 21 --manifest /hps/nobackup/rdf/branchwater/disk1_filtered_manifest.csv --output /hps/nobackup/rdf/branchwater/app_folder/bw_db/benchmarks/current/sigs_indexed_I /hps/nobackup/rdf/branchwater/disk>