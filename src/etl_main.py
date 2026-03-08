# src/etl_main.py
"""ETL entry point — demonstrates the full crosswalk → lineage → pipeline workflow."""

import os
from api.data_model import O3DataModel
from etl.registry import load_model_registry
from etl.manifest import load_semantic_manifest
from etl.mapping.crosswalk import Crosswalk
from etl.lineage.lineage_builder import LineageBuilder
from etl.lineage.lineage_report import LineageReport
from etl.pipeline.extractor import Extractor
from etl.pipeline.loader import Loader
from etl.pipeline.runner import ETLRunner


RESOURCES = os.path.join(os.path.dirname(__file__), "Resources")
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "Sql_Commands", "etl")
CROSSWALK_PATH = os.path.join(RESOURCES, "crosswalk.json")


def main() -> None:
    # 1. Load data sources
    print("Loading O3 data model...")
    o3 = O3DataModel(os.path.join(RESOURCES, "O3_20250128_Fixed.json"), clean=True)

    print("Loading model registry...")
    registry = load_model_registry(os.path.join(RESOURCES, "model_registry.json"))

    print("Loading semantic manifests...")
    manifest = load_semantic_manifest(
        os.path.join(RESOURCES, "semantic_manifest_from_variandw_schema.json"),
        os.path.join(RESOURCES, "semantic_manifest_with_models.json"),
    )

    # 2. Generate or load crosswalk
    cw = Crosswalk(manifest, registry, o3)

    if os.path.exists(CROSSWALK_PATH):
        print(f"Loading curated crosswalk from {CROSSWALK_PATH}...")
        entries = cw.load_curated(CROSSWALK_PATH)
        print(f"  {len(entries)} curated entries loaded.")
    else:
        print("Generating crosswalk suggestions...")
        entries = cw.generate_suggestions()
        cw.save_curated(entries, CROSSWALK_PATH)
        print(f"  {len(entries)} suggestions saved to {CROSSWALK_PATH}")
        print("  Review and edit crosswalk.json, then re-run.")

    # 3. Build lineage
    print("Building lineage graph...")
    graph = LineageBuilder(entries, manifest, o3).build()
    report = LineageReport(graph)

    summary = report.coverage_summary()
    print(f"  Coverage: {summary['mapped']}/{summary['total_o3_attributes']} "
          f"({summary['coverage_pct']}%)")

    lineage_json = os.path.join(OUTPUT, "lineage.json")
    lineage_md = os.path.join(OUTPUT, "lineage.md")
    os.makedirs(OUTPUT, exist_ok=True)
    report.to_json(lineage_json)
    report.to_markdown(lineage_md)
    print(f"  Lineage exported to {lineage_json} and {lineage_md}")

    # 4. Export ETL SQL (offline mode)
    print("Generating ETL SQL...")
    active_entries = [e for e in entries if e.is_active]
    extractor = Extractor(active_entries, manifest, registry)
    loader = Loader(o3)
    runner = ETLRunner(extractor, loader)
    runner.export_sql(OUTPUT)
    print(f"  ETL SQL files written to {OUTPUT}")

    print("Done.")


if __name__ == "__main__":
    main()
