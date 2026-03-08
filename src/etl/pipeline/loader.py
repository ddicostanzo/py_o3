"""Generates INSERT and MERGE SQL to load extracted data into O3 tables."""

from __future__ import annotations

from dataclasses import dataclass

from etl.pipeline.extractor import ExtractQuery
from helpers.string_helpers import leave_only_letters_numbers_or_underscore
from api.data_model import O3DataModel


@dataclass
class LoadCommand:
    """A generated load SQL statement."""

    target_table: str
    sql: str
    column_map: dict[str, str]  # {o3_column: source_alias}


class Loader:
    """Generates INSERT/MERGE SQL for loading into O3 tables (MSSQL dialect)."""

    def __init__(
        self,
        o3_model: O3DataModel,
    ):
        self.__o3_model = o3_model

    def generate_insert(self, extract: ExtractQuery) -> LoadCommand:
        """Generate an INSERT INTO ... SELECT statement."""
        column_map, target_table = self.__build_column_map(extract)

        if not column_map:
            raise ValueError(
                f"No mapped columns for target table '{target_table}'. "
                f"Check the crosswalk entries in the extract query."
            )

        o3_columns = ", ".join(f"[{col}]" for col in column_map.keys())
        source_columns = ", ".join(
            f"src.[{alias}]" for alias in column_map.values()
        )

        sql = (
            f"INSERT INTO {target_table} ({o3_columns})\n"
            f"SELECT {source_columns}\n"
            f"FROM (\n{extract.sql}\n) AS src"
        )

        return LoadCommand(target_table=target_table, sql=sql, column_map=column_map)

    def generate_merge(
        self, extract: ExtractQuery, merge_key: list[str]
    ) -> LoadCommand:
        """Generate a MERGE (upsert) statement."""
        column_map, target_table = self.__build_column_map(extract)

        if not column_map:
            raise ValueError(
                f"No mapped columns for target table '{target_table}'. "
                f"Check the crosswalk entries in the extract query."
            )

        missing_keys = [k for k in merge_key if k not in column_map]
        if missing_keys:
            raise ValueError(
                f"Merge keys {missing_keys} not found in column map for "
                f"{target_table}. Available: {list(column_map.keys())}"
            )

        on_clause = " AND ".join(
            f"target.[{k}] = source.[{column_map[k]}]"
            for k in merge_key
        )

        update_cols = [
            f"target.[{k}] = source.[{v}]"
            for k, v in column_map.items()
            if k not in merge_key
        ]

        insert_cols = ", ".join(f"[{k}]" for k in column_map.keys())
        insert_vals = ", ".join(f"source.[{v}]" for v in column_map.values())

        sql_parts = [
            f"MERGE {target_table} AS target",
            f"USING (\n{extract.sql}\n) AS source",
            f"ON {on_clause}",
        ]

        if update_cols:
            sql_parts.append(
                "WHEN MATCHED THEN UPDATE SET\n  " + ",\n  ".join(update_cols)
            )

        sql_parts.append(
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols})\n"
            f"  VALUES ({insert_vals});"
        )

        sql = "\n".join(sql_parts)

        return LoadCommand(target_table=target_table, sql=sql, column_map=column_map)

    def __build_column_map(
        self, extract: ExtractQuery
    ) -> tuple[dict[str, str], str]:
        """Build {o3_column: source_alias} map and determine target table."""
        column_map: dict[str, str] = {}
        target_elements: set[str] = set()

        for entry in extract.columns_mapped:
            if not entry.is_active:
                continue
            o3_col = leave_only_letters_numbers_or_underscore(entry.o3_attribute)
            source_alias = entry.model_alias or entry.dwh_column
            column_map[o3_col] = source_alias
            target_elements.add(entry.o3_key_element)

        # Use the first O3 key element as the target table
        if target_elements:
            element_name = sorted(target_elements)[0]
            ke = self.__o3_model.key_elements.get(element_name)
            if ke:
                table_name = leave_only_letters_numbers_or_underscore(ke.string_code)
            else:
                table_name = leave_only_letters_numbers_or_underscore(element_name)
        else:
            raise ValueError(
                "No O3 key elements found in the extract query's mapped columns."
            )

        return column_map, table_name


if __name__ == "__main__":
    pass
