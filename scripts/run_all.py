"""run_all.py — run the full pipeline end-to-end.

Stage order:
  1. Ingest tracking data   (run_ingest_tracking)
  2. Ingest weather data    (run_ingest_weather)
  3. Build Silver tables    (run_build_tables)
  4. Build Gold tables      (run_build_gold)
  5. Build features         (run_build_features)
  6. Train model            (run_train)
"""

import runpy
import sys


def _run(script: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {script}")
    print(f"{'='*60}")
    runpy.run_path(f"scripts/{script}.py", run_name="__main__")


def main() -> None:
    stages = [
        "run_ingest_tracking",
        "run_ingest_weather",
        "run_build_tables",
        "run_build_gold",
        "run_build_features",
        "run_train",
    ]

    # Allow running a subset: python scripts/run_all.py run_build_gold run_train
    selected = sys.argv[1:] if len(sys.argv) > 1 else stages

    for stage in selected:
        _run(stage)


if __name__ == "__main__":
    main()
