# src/etl/mapping/match_engine.py
"""Scoring engine for DWH→O3 column name and type matching."""

from __future__ import annotations

import re
from dataclasses import dataclass


# Prefixes stripped before name comparison
_STRIP_PREFIXES = re.compile(
    r"^(Dim|Fact|DimLookupID_|DimDateID_|DimUserID_|DimDoctorID_|ID_?|Lookup)", re.IGNORECASE
)

# Token splitter: camelCase and underscores
_TOKEN_SPLIT = re.compile(r"[A-Z][a-z]+|[a-z]+|[A-Z]+(?=[A-Z][a-z]|\d|\b)|[A-Z]+|\d+")

# Type compatibility groups
_TYPE_GROUPS: dict[str, set[str]] = {
    "string": {
        "string", "varchar", "nvarchar", "char", "text", "ntext",
        "vdt_patientid", "vdt_id", "vdt_name", "vdt_string",
        "vdt_string1", "vdt_string10", "vdt_string16", "vdt_string30",
        "vdt_string32", "vdt_string64", "vdt_string128", "vdt_string254",
        "vdt_string256", "vdt_string512", "vdt_status16", "vdt_status32",
        "vdt_username", "vdt_phonenumber", "vdt_tablename", "vdt_sex",
        "vdt_collmode", "vdt_energymode", "vdt_scale",
    },
    "integer": {
        "integer", "int", "bigint", "smallint", "tinyint",
        "vdt_int", "vdt_count", "vdt_serialnumber", "vdt_tinyint",
    },
    "decimal": {
        "decimal", "numeric", "float", "real", "money",
        "vdt_float", "vdt_dose", "vdt_doserate", "vdt_angle",
        "vdt_mu", "vdt_energy", "vdt_couchparam", "vdt_collparam",
        "vdt_overrideflag", "vdt_time",
    },
    "date": {
        "date", "datetime", "datetime2", "datetimeoffset", "smalldatetime",
        "vdt_datetime", "vdt_datetimestamp",
    },
    "boolean": {
        "boolean", "bit", "binary",
        "vdt_flag_false_default",
    },
}

# Reverse map: type string → group name
_TYPE_TO_GROUP: dict[str, str] = {}
for group, members in _TYPE_GROUPS.items():
    for member in members:
        _TYPE_TO_GROUP[member] = group


@dataclass
class MatchCandidate:
    """A scored candidate mapping between a DWH source and an O3 attribute."""

    dwh_source: str
    o3_target: str
    score: float
    signals: dict[str, float]


class MatchEngine:
    """Scores potential DWH→O3 column mappings using name, type, and context signals."""

    def __init__(
        self,
        name_weight: float = 0.6,
        type_weight: float = 0.25,
        context_weight: float = 0.15,
    ):
        self.__name_weight = name_weight
        self.__type_weight = type_weight
        self.__context_weight = context_weight

    def score(
        self,
        dwh_name: str,
        dwh_type: str,
        o3_name: str,
        o3_type: str,
        dwh_context: str | None = None,
        o3_context: str | None = None,
    ) -> MatchCandidate:
        """Score a single DWH column against a single O3 attribute."""
        name_score = self._name_similarity(dwh_name, o3_name)
        type_score = self._type_compatibility(dwh_type, o3_type)
        context_score = self._context_similarity(dwh_context, o3_context)

        composite = (
            name_score * self.__name_weight
            + type_score * self.__type_weight
            + context_score * self.__context_weight
        )

        return MatchCandidate(
            dwh_source=dwh_name,
            o3_target=o3_name,
            score=round(composite, 4),
            signals={
                "name": round(name_score, 4),
                "type": round(type_score, 4),
                "context": round(context_score, 4),
            },
        )

    def _name_similarity(self, dwh_name: str, o3_name: str) -> float:
        """Token-overlap similarity after stripping common prefixes."""
        # Compare lowered prefix-stripped strings for exact equivalence
        dwh_stripped = _STRIP_PREFIXES.sub("", dwh_name).lower()
        o3_stripped = _STRIP_PREFIXES.sub("", o3_name).lower()
        if dwh_stripped and o3_stripped and dwh_stripped == o3_stripped:
            return 1.0

        dwh_tokens = self.__tokenize(dwh_name)
        o3_tokens = self.__tokenize(o3_name)

        if not dwh_tokens or not o3_tokens:
            return 0.0

        # Count partial matches: token A matches token B if one starts with the other
        matched = 0
        all_tokens = dwh_tokens | o3_tokens
        for dt in dwh_tokens:
            for ot in o3_tokens:
                if dt == ot or dt.startswith(ot) or ot.startswith(dt):
                    matched += 1
                    break

        # Jaccard-like: matched tokens over total unique tokens
        if not all_tokens:
            return 0.0

        return matched / len(all_tokens)

    def _type_compatibility(self, dwh_type: str, o3_type: str) -> float:
        """Check if DWH and O3 types belong to the same compatibility group."""
        dwh_group = _TYPE_TO_GROUP.get(dwh_type.lower())
        o3_group = _TYPE_TO_GROUP.get(o3_type.lower())

        if dwh_group is not None and dwh_group == o3_group:
            return 1.0
        if dwh_group is not None and o3_group is not None:
            return 0.0
        # One or both types unrecognized — partial credit
        return 0.3

    def _context_similarity(
        self, dwh_context: str | None, o3_context: str | None
    ) -> float:
        """Boost score if both belong to the same semantic domain."""
        if dwh_context is None or o3_context is None:
            return 0.0

        dwh_tokens = self.__tokenize(dwh_context)
        o3_tokens = self.__tokenize(o3_context)

        if not dwh_tokens or not o3_tokens:
            return 0.0

        intersection = dwh_tokens & o3_tokens
        union = dwh_tokens | o3_tokens

        return len(intersection) / len(union)

    def __tokenize(self, name: str) -> set[str]:
        """Split a name into lowercase tokens, stripping common prefixes."""
        stripped = _STRIP_PREFIXES.sub("", name)
        if not stripped:
            stripped = name
        tokens = _TOKEN_SPLIT.findall(stripped)
        return {t.lower() for t in tokens if len(t) > 1}


if __name__ == "__main__":
    pass
