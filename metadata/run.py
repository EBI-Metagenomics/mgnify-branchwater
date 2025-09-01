#!/usr/bin/env python3
import argparse
import os
import sys

# Import local modules
from prepare_bq import main as bq_main
from prepare_sra import main as sra_main
from load_duckdb import main as duckdb_main


def run_bq(args: argparse.Namespace):
    # Allow GOOGLE_APPLICATION_CREDENTIALS to override --key-path for convenience
    key_path = args.key_path
    gac = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if gac and not args.key_path_overridden:
        key_path = gac
    bq_main(
        accs=args.acc,
        limit=args.limit,
        output=args.output,
        key_path=key_path,
    )


def run_sra(args: argparse.Namespace):
    sra_main(
        accs=args.acc,
        sra_metadata=args.sra_metadata,
        build_full_db=args.build_full_db,
        output=args.output,
    )


def run_duckdb(args: argparse.Namespace):
    duckdb_main(
        parquet_metadata=args.parquet_metadata,
        output=args.output,
        force=args.force,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Metadata container runner: build parquet metadata via BigQuery or S3, and/or load into DuckDB."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # BigQuery flow
    p_bq = sub.add_parser("bq", help="Build metadata parquet using BigQuery")
    p_bq.add_argument("--acc", "-a", default="/data/bw_db/sraids", help="Path to file containing accession IDs, one per line")
    p_bq.add_argument("--output", "-o", default="/data/bw_db/metadata.parquet", help="Output parquet path")
    p_bq.add_argument("--key-path", "-k", default="/data/bw_db/bqKey.json", help="Path to BigQuery service account JSON key. Overridden by GOOGLE_APPLICATION_CREDENTIALS if set and --key-path not provided explicitly.")
    p_bq.add_argument("--limit", "-l", action="store_true", help="Limit to 150,000 rows for faster testing")
    p_bq.set_defaults(func=run_bq, key_path_overridden=False)

    # Detect if the user explicitly set --key-path to avoid env override
    def _set_key_path_overridden(ns):
        ns.key_path_overridden = True
        return ns
    p_bq.add_argument("--set-key-path-overridden", action="store_true", help=argparse.SUPPRESS)

    # S3 flow (SRA public metadata)
    p_sra = sub.add_parser("sra", help="Build metadata parquet using S3-hosted SRA public metadata")
    p_sra.add_argument("--acc", "-a", default="/data/bw_db/sraids", help="Path to file containing accession IDs, one per line")
    p_sra.add_argument("--sra-metadata", "-s", default="s3://sra-pub-metadata-us-east-1/sra/metadata/", help="S3 URL to SRA metadata parquet files")
    p_sra.add_argument("--output", "-o", default="/data/bw_db/metadata.parquet", help="Output parquet path")
    sra_mode = p_sra.add_mutually_exclusive_group()
    sra_mode.add_argument("--build-full-db", dest="build_full_db", action="store_true", default=True, help="Build full DB")
    sra_mode.add_argument("--build-test-db", dest="build_full_db", action="store_false", help="Build smaller test DB (first 150k)")
    p_sra.set_defaults(func=run_sra)

    # DuckDB loader
    p_duck = sub.add_parser("duckdb", help="Load parquet into DuckDB database")
    p_duck.add_argument("parquet_metadata", nargs="?", default="/data/bw_db/metadata.parquet", help="Path to input parquet metadata")
    p_duck.add_argument("--output", "-o", default="/data/bw_db/metadata.duckdb", help="Output DuckDB file path")
    p_duck.add_argument("--force", action="store_true", help="Force recreate DuckDB file if exists")
    p_duck.set_defaults(func=run_duckdb)

    args = parser.parse_args()

    # If user passed --key-path explicitly, mark overridden (support older argparse versions)
    if "--key-path" in sys.argv or "-k" in sys.argv:
        setattr(args, "key_path_overridden", True)

    return args.func(args)


if __name__ == "__main__":
    main()
