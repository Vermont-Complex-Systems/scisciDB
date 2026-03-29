#!/bin/bash
#SBATCH --partition=short
#SBATCH --job-name=oa_works_import
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --time=3:00:00
#SBATCH --output=logs/oa_works_%j.out

set -e

mkdir -p logs

cd /gpfs1/home/j/s/jstonge1/scisciDB

echo "Starting oa_works import on $(hostname) at $(date)"

bash load/oa_works.sh

echo "Done at $(date)"
