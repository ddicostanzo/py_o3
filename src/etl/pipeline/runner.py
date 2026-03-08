"""ETL orchestration: run live against database or export SQL files offline."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field

from etl.pipeline.extractor import Extractor, ExtractQuery
from etl.pipeline.loader import Loader, LoadCommand


@dataclass
class EntryPointResult:
    """Result of running ETL for a single entry point."""

    entry_point: str
    rows_extracted: int = 0
    rows_loaded: int = 0
    duration_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)


@dataclass
class ETLResult:
    """Result of a complete ETL run."""

    results: list[EntryPointResult] = field(default_factory=list)
    total_duration: float = 0.0

    @property
    def success(self) -> bool:
        return all(len(r.errors) == 0 for r in self.results)


class ETLRunner:
    """Orchestrates extract-transform-load: live execution or offline SQL export."""

    def __init__(
        self,
        extractor: Extractor,
        loader: Loader,
        connection=None,
    ):
        self.__extractor = extractor
        self.__loader = loader
        self.__connection = connection

    def export_sql(
        self,
        output_dir: str,
        entry_points: list[str] | None = None,
    ) -> None:
        """Write extract + load SQL to files in output_dir."""
        os.makedirs(output_dir, exist_ok=True)

        if entry_points:
            queries = [
                self.__extractor.generate_query(ep) for ep in entry_points
            ]
        else:
            queries = self.__extractor.generate_all_queries()

        for query in queries:
            load_cmd = self.__loader.generate_insert(query)
            filename = f"{query.entry_point}_etl.sql"
            filepath = os.path.join(output_dir, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(f"-- ETL: {query.entry_point}\n")
                f.write(f"-- Model: {query.model_name}\n")
                f.write(f"-- Base Table: {query.base_table}\n")
                f.write(f"-- Date Key: {query.date_key}\n\n")
                f.write("-- ===== EXTRACT =====\n\n")
                f.write(query.sql)
                f.write("\n\n-- ===== LOAD =====\n\n")
                f.write(load_cmd.sql)
                f.write("\n")

    def run(
        self,
        entry_points: list[str] | None = None,
        date_basis: str | None = None,
        lookback_days: int | None = None,
        dry_run: bool = False,
    ) -> ETLResult:
        """Execute ETL pipeline. Requires connection unless dry_run=True."""
        if not dry_run and self.__connection is None:
            raise ValueError(
                "Live execution requires a database connection. "
                "Pass connection to ETLRunner or use dry_run=True."
            )

        start = time.time()
        results: list[EntryPointResult] = []

        if entry_points:
            queries = [
                self.__extractor.generate_query(ep, date_basis, lookback_days)
                for ep in entry_points
            ]
        else:
            queries = self.__extractor.generate_all_queries()

        for query in queries:
            ep_result = self.__run_entry_point(query, dry_run)
            results.append(ep_result)

        total_duration = time.time() - start

        return ETLResult(
            results=results,
            total_duration=total_duration,
        )

    def __run_entry_point(
        self, query: ExtractQuery, dry_run: bool
    ) -> EntryPointResult:
        """Run ETL for a single entry point."""
        ep_start = time.time()
        result = EntryPointResult(entry_point=query.entry_point)

        if dry_run:
            result.duration_seconds = time.time() - ep_start
            return result

        try:
            cursor = self.__connection.cursor()
            cursor.execute(query.sql)
            rows = cursor.fetchall()
            result.rows_extracted = len(rows)
        except Exception as e:
            result.errors.append(
                f"Extract failed for '{query.entry_point}': "
                f"{type(e).__name__}: {e}"
            )
            result.duration_seconds = time.time() - ep_start
            return result

        try:
            load_cmd = self.__loader.generate_insert(query)
            cursor.execute(load_cmd.sql)
            result.rows_loaded = cursor.rowcount
            self.__connection.commit()
        except Exception as e:
            result.errors.append(
                f"Load failed for '{query.entry_point}': "
                f"{type(e).__name__}: {e}"
            )
            try:
                self.__connection.rollback()
            except Exception as rollback_err:
                result.errors.append(
                    f"Rollback failed: {type(rollback_err).__name__}: "
                    f"{rollback_err}"
                )

        result.duration_seconds = time.time() - ep_start
        return result


if __name__ == "__main__":
    pass
