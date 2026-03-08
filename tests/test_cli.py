"""Tests for the CLI interface."""
import os
import tempfile

import pytest

from cli import _build_parser, main


class TestBuildParser:
    def test_required_input(self):
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--output", "out.sql"])

    def test_required_output(self):
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--input", "in.json"])

    def test_default_server_is_mssql(self):
        parser = _build_parser()
        args = parser.parse_args(["-i", "in.json", "-o", "out.sql"])
        assert args.server == "mssql"

    def test_server_choices(self):
        parser = _build_parser()
        args = parser.parse_args(["-i", "in.json", "-o", "out.sql", "-s", "psql"])
        assert args.server == "psql"

    def test_invalid_server_rejected(self):
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["-i", "in.json", "-o", "out.sql", "-s", "mysql"])

    def test_phi_allowed_default_false(self):
        parser = _build_parser()
        args = parser.parse_args(["-i", "in.json", "-o", "out.sql"])
        assert args.phi_allowed is False

    def test_phi_allowed_flag(self):
        parser = _build_parser()
        args = parser.parse_args(["-i", "in.json", "-o", "out.sql", "--phi-allowed"])
        assert args.phi_allowed is True

    def test_clean_default_false(self):
        parser = _build_parser()
        args = parser.parse_args(["-i", "in.json", "-o", "out.sql"])
        assert args.clean is False

    def test_clean_flag(self):
        parser = _build_parser()
        args = parser.parse_args(["-i", "in.json", "-o", "out.sql", "--clean"])
        assert args.clean is True

    def test_include_lookup_default_false(self):
        parser = _build_parser()
        args = parser.parse_args(["-i", "in.json", "-o", "out.sql"])
        assert args.include_lookup is False

    def test_include_patient_hash_default_false(self):
        parser = _build_parser()
        args = parser.parse_args(["-i", "in.json", "-o", "out.sql"])
        assert args.include_patient_hash is False


_SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__), '..', 'src', 'Resources', 'O3_20250128.json'
)


class TestMainErrorHandling:
    def test_file_not_found_returns_1(self, capsys):
        result = main(["-i", "/nonexistent/schema.json", "-o", "/tmp/out.sql"])
        assert result == 1
        captured = capsys.readouterr()
        assert "Error:" in captured.err

    def test_invalid_json_returns_1(self, capsys):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("not valid json{{{")
            path = f.name

        try:
            result = main(["-i", path, "-o", "/tmp/out.sql"])
            assert result == 1
            captured = capsys.readouterr()
            assert "Error:" in captured.err
        finally:
            os.unlink(path)


class TestMainIntegration:
    @pytest.mark.skipif(
        not os.path.exists(_SCHEMA_PATH),
        reason="Schema file O3_20250128.json not available in Resources/"
    )
    def test_generates_sql_output(self, capsys):
        with tempfile.NamedTemporaryFile(suffix='.sql', delete=False) as f:
            output_path = f.name

        try:
            result = main(["-i", _SCHEMA_PATH, "-o", output_path, "-s", "psql", "--clean"])
            assert result == 0
            captured = capsys.readouterr()
            assert "SQL written to" in captured.out

            with open(output_path) as f:
                content = f.read()
            assert "CREATE TABLE" in content
        finally:
            os.unlink(output_path)

    @pytest.mark.skipif(
        not os.path.exists(_SCHEMA_PATH),
        reason="Schema file O3_20250128.json not available in Resources/"
    )
    def test_creates_output_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nested = os.path.join(tmpdir, "subdir", "output.sql")
            result = main(["-i", _SCHEMA_PATH, "-o", nested, "--clean"])
            assert result == 0
            assert os.path.exists(nested)
