#!/usr/bin/env python3
"""
Select rows present in file1 but NOT present in file2 (ksize=21), keeping all
columns from file1 except 'sha256'. In the first column, replace 'sigs/xxxxx.sig'
(or any path) with just 'xxxxx.sig'.

- file1: first column may look like 'sigs/xxxxx.sig', has 'sha256' column
- file2: first column is 'xxxxx.sig' (no 'sigs/'), may lack 'sha256'
- non-match criterion: basename of file1 first col is NOT found as the first col in file2
- output: CSV with file1's columns except 'sha256'; first column normalized to 'xxxxx.sig'
"""

import argparse
import csv
from pathlib import PurePosixPath

def smart_csv_reader(path):
    f = open(path, newline='', encoding='utf-8')
    first = f.readline()
    if not first.startswith("#"):
        f.seek(0)
    reader = csv.reader(f)
    try:
        header = next(reader)
    except StopIteration:
        f.close()
        raise ValueError(f"{path}: empty CSV")
    return f, reader, header

def find_indices(header, names):
    m = {n: header.index(n) for n in names if n in header}
    missing = [n for n in names if n not in m]
    if missing:
        raise ValueError(f"Missing required column(s) {missing} in header: {header}")
    return m

def basename_only(s: str) -> str:
    return PurePosixPath(s.strip()).name

def load_file2_names(path):
    f, reader, header = smart_csv_reader(path)
    try:
        idx = find_indices(header, ["internal_location"])
        names = set()
        for row in reader:
            if not row or len(row) <= idx["internal_location"]:
                continue
            names.add(basename_only(row[idx["internal_location"]]))
        return names
    finally:
        f.close()

def main():
    ap = argparse.ArgumentParser(description="Keep rows from file1 that are NOT in file2 (ksize=21), normalize first column to xxxxx.sig, drop sha256.")
    ap.add_argument("--file1", required=True, help="CSV with 'sigs/xxxxx.sig' and 'sha256'")
    ap.add_argument("--file2", required=True, help="CSV with 'xxxxx.sig' (no 'sigs/')")
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--ksize", type=int, default=21, help="ksize value to keep from file1 (default: 21)")
    ap.add_argument("--keep-sha256", action="store_true", help="If set, keep the sha256 column instead of dropping it")
    args = ap.parse_args()

    # load set of names from file2
    names2 = load_file2_names(args.file2)

    # iterate file1, select matches
    f1, r1, h1 = smart_csv_reader(args.file1)
    try:
        needed = ["internal_location", "ksize"]
        idx = find_indices(h1, needed)

        # decide output header: file1 header, optionally dropping sha256
        out_header = list(h1)
        if not args.keep_sha256 and "sha256" in out_header:
            out_header.remove("sha256")

        # We'll always write internal_location (first column) normalized
        # If internal_location is not the first column, we still rewrite that field.
        internal_loc_i = h1.index("internal_location")
        drop_sha256_i = h1.index("sha256") if ("sha256" in h1 and not args.keep_sha256) else None

        with open(args.out, "w", newline="", encoding="utf-8") as out_f:
            w = csv.writer(out_f)
            w.writerow(out_header)
            kept = 0

            for row in r1:
                if not row:
                    continue
                # guard: ensure row length
                if len(row) < len(h1):
                    # pad short rows (rare but safer)
                    row = row + [""] * (len(h1) - len(row))

                # filter by ksize
                try:
                    k = int(row[idx["ksize"]].strip())
                except ValueError:
                    continue
                if k != args.ksize:
                    continue

                # normalize first column value to basename
                norm = basename_only(row[internal_loc_i])

                # keep rows whose basename is NOT present in file2
                if norm in names2:
                    continue

                # build output row:
                out_row = []
                for j, colname in enumerate(h1):
                    if drop_sha256_i is not None and j == drop_sha256_i:
                        continue
                    if j == internal_loc_i:
                        out_row.append(norm)  # normalized xxxxx.sig
                    else:
                        out_row.append(row[j])
                w.writerow(out_row)
                kept += 1

        print(f"Wrote {kept} non-matching row(s) to {args.out}")
    finally:
        f1.close()

if __name__ == "__main__":
    main()
