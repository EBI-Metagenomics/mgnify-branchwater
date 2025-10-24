#!/usr/bin/env python3
"""
Flatten ENA JSON:
- Input:  [{"ena_accessions": ["ERR1","SRR2"], "is_private": false, ...}, ...]
- Output: [{"ena_accession": "ERR1", "is_private": false}, {"ena_accession": "SRR2", "is_private": false}, ...]
"""

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any, Set, Tuple


def flatten_ena_json(records: List[Dict[str, Any]], dedup: bool = False) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    if dedup:
        seen: Set[Tuple[str, bool]] = set()

    for rec in records:
        is_private = bool(rec.get("is_private", False))
        for acc in rec.get("ena_accessions", []) or []:
            if acc is None:
                continue
            ena = str(acc).strip()
            if not ena:
                continue

            if dedup:
                key = (ena, is_private)
                if key in seen:
                    continue
                seen.add(key)

            out.append({"ena_accession": ena, "is_private": is_private})

    return out


def main():
    ap = argparse.ArgumentParser(description="Flatten ENA JSON to one row per accession.")
    ap.add_argument("input", help="Path to input JSON file (array of records).")
    ap.add_argument("output", help="Path to write flattened JSON array.")
    ap.add_argument("--dedup", action="store_true", help="Remove duplicate (ena_accession, is_private) pairs.")
    ap.add_argument("--indent", type=int, default=2, help="Indent for pretty JSON (default: 2).")
    args = ap.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)

    with in_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("Input JSON must be an array of records.")

    flattened = flatten_ena_json(data, dedup=args.dedup)

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(flattened, f, ensure_ascii=False, indent=args.indent)

    print(f"Wrote {len(flattened)} rows to {out_path}")


if __name__ == "__main__":
    main()
