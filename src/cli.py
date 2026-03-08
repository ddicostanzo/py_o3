"""Command-line interface for O3 schema parsing and SQL generation."""
from __future__ import annotations

import argparse
import json
import os
import sys

from api.workflow import (
    create_model,
    create_standard_value_lookup_table,
    create_tables,
    foreign_key_constraints,
    write_sql_to_text,
)
from helpers.enums import SupportedSQLServers
from sql.data_model_to_sql.table_generator import PatientIdentifierHash


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="py-o3",
        description="Parse an O3 JSON schema and generate SQL DDL for database creation.",
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Path to the O3 JSON schema file.",
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Path for the output SQL file.",
    )
    parser.add_argument(
        "-s", "--server",
        choices=["mssql", "psql"],
        default="mssql",
        help="Target SQL server type (default: mssql).",
    )
    parser.add_argument(
        "--phi-allowed",
        action="store_true",
        default=False,
        help="Allow PHI columns (MRN) to be NOT NULL.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        default=False,
        help="Clean common typos and data type issues during parsing.",
    )
    parser.add_argument(
        "--include-lookup",
        action="store_true",
        default=False,
        help="Include the standard values lookup table and INSERT commands.",
    )
    parser.add_argument(
        "--include-patient-hash",
        action="store_true",
        default=False,
        help="Include the PatientIdentifierHash table.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for the py-o3 CLI."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    server_map = {
        "mssql": SupportedSQLServers.MSSQL,
        "psql": SupportedSQLServers.PSQL,
    }
    sql_type = server_map[args.server]

    try:
        model = create_model(args.input, clean=args.clean)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error: Failed to parse input schema: {e}", file=sys.stderr)
        return 1

    try:
        tables = create_tables(model, sql_type, args.phi_allowed)

        all_commands: list[str] = [v for v in tables.values()]

        if args.include_lookup:
            lookup = create_standard_value_lookup_table(model, sql_type)
            all_commands.append(lookup.sql_table())
            all_commands.extend(lookup.insert_commands())

        if args.include_patient_hash:
            patient_hash = PatientIdentifierHash(sql_type, "PatientIdentifierHash")
            all_commands.append(patient_hash.sql_table())
            all_commands.append(patient_hash.foreign_key)

        fk_commands = foreign_key_constraints(model, sql_type)
        all_commands.extend(fk_commands)

        output_dir = os.path.dirname(args.output)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        write_sql_to_text(args.output, all_commands, write_mode="w")
        print(f"SQL written to {args.output}")

        return 0
    except (ValueError, TypeError) as e:
        print(f"Error during SQL generation: {e}", file=sys.stderr)
        return 1
    except OSError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
