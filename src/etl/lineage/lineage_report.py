"""Export lineage graphs as JSON, markdown, and coverage summaries."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict

from etl.lineage.lineage_builder import LineageGraph, LineageNode


class LineageReport:
    """Generates reports from a LineageGraph."""

    def __init__(self, graph: LineageGraph):
        self.__graph = graph

    def coverage_summary(self) -> dict:
        """Return coverage statistics for O3 attributes."""
        targets = [n for n in self.__graph.nodes if n.node_type == "target"]
        connected_targets = {e.target for e in self.__graph.edges}
        mapped = [t for t in targets if t in connected_targets]

        total = len(targets)
        mapped_count = len(mapped)
        unmapped_count = total - mapped_count

        return {
            "total_o3_attributes": total,
            "mapped": mapped_count,
            "unmapped": unmapped_count,
            "coverage_pct": round(mapped_count / total * 100, 1) if total else 0.0,
        }

    def to_json(self, path: str) -> None:
        """Write the full lineage graph as JSON."""
        data = {
            "nodes": [
                {
                    "node_type": n.node_type,
                    "table": n.table,
                    "column": n.column,
                    "metadata": n.metadata,
                }
                for n in self.__graph.nodes
            ],
            "edges": [
                {
                    "source_table": e.source.table,
                    "source_column": e.source.column,
                    "target_table": e.target.table,
                    "target_column": e.target.column,
                    "transform_expr": e.transform_expr,
                    "model_name": e.model_name,
                    "confidence": e.confidence,
                }
                for e in self.__graph.edges
            ],
            "coverage": self.coverage_summary(),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def to_markdown(self, path: str) -> None:
        """Write a human-readable markdown lineage report."""
        lines: list[str] = ["# Data Lineage Report\n"]

        # Coverage summary
        summary = self.coverage_summary()
        lines.append("## Coverage Summary\n")
        lines.append(f"- **Total O3 Attributes:** {summary['total_o3_attributes']}")
        lines.append(f"- **Mapped:** {summary['mapped']}")
        lines.append(f"- **Unmapped:** {summary['unmapped']}")
        lines.append(f"- **Coverage:** {summary['coverage_pct']}%\n")

        # Group edges by O3 key element
        edges_by_element: dict[str, list] = defaultdict(list)
        for edge in self.__graph.edges:
            if edge.target.node_type == "target":
                edges_by_element[edge.target.table].append(edge)
            elif edge.source.node_type == "transform":
                # Transform -> target edge: group by target
                edges_by_element[edge.target.table].append(edge)

        # Mapped attributes table per key element
        if edges_by_element:
            lines.append("## Mapped Attributes\n")
            for element in sorted(edges_by_element.keys()):
                lines.append(f"### {element}\n")
                lines.append("| O3 Attribute | DWH Source | Transform | Model | Confidence |")
                lines.append("|---|---|---|---|---|")
                for edge in edges_by_element[element]:
                    source = f"{edge.source.table}.{edge.source.column}"
                    transform = edge.transform_expr or "— (direct)"
                    model = edge.model_name or "—"
                    lines.append(
                        f"| {edge.target.column} | {source} | {transform} | {model} | {edge.confidence} |"
                    )
                lines.append("")

        # Unmapped O3 attributes
        unmapped_targets = self.__graph.unmapped_targets()
        if unmapped_targets:
            lines.append("## Unmapped O3 Attributes\n")
            for node in unmapped_targets:
                lines.append(f"- {node.table}.{node.column}")
            lines.append("")

        # Unmapped DWH columns
        unmapped_sources = self.__graph.unmapped_sources()
        if unmapped_sources:
            lines.append("## Unmapped DWH Columns\n")
            for node in unmapped_sources:
                lines.append(f"- {node.table}.{node.column}")
            lines.append("")

        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))


if __name__ == "__main__":
    pass
