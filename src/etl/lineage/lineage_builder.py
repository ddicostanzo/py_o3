"""Builds a directed lineage graph from crosswalk entries."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Literal

from etl.mapping.mapping_store import CrosswalkEntry
from etl.manifest import SemanticManifest
from api.data_model import O3DataModel

NodeType = Literal["source", "transform", "target"]


@dataclass(frozen=True)
class LineageNode:
    """A node in the lineage graph: source, transform, or target."""

    node_type: NodeType
    table: str
    column: str
    metadata: dict = field(default_factory=dict, hash=False, compare=False)


@dataclass(frozen=True)
class LineageEdge:
    """A directed edge in the lineage graph."""

    source: LineageNode
    target: LineageNode
    transform_expr: str | None = None
    model_name: str | None = None
    confidence: float = 0.0


@dataclass
class LineageGraph:
    """Directed graph of data lineage: source → (transform) → target."""

    nodes: list[LineageNode] = field(default_factory=list)
    edges: list[LineageEdge] = field(default_factory=list)

    def __post_init__(self):
        self.__adjacency: dict[LineageNode, list[LineageEdge]] = {}
        self.__reverse: dict[LineageNode, list[LineageEdge]] = {}
        for edge in self.edges:
            self.__adjacency.setdefault(edge.source, []).append(edge)
            self.__reverse.setdefault(edge.target, []).append(edge)

    def trace_forward(self, source_table: str, source_column: str) -> list[LineageNode]:
        """Find all target nodes reachable from a source column.

        Uses frozen LineageNode equality — a new node with the same
        (node_type, table, column) matches existing nodes because
        metadata is excluded from comparison.
        """
        start = LineageNode(node_type="source", table=source_table, column=source_column)
        visited: set[LineageNode] = set()
        targets: list[LineageNode] = []
        queue: deque[LineageNode] = deque([start])

        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            if current.node_type == "target":
                targets.append(current)
            for edge in self.__adjacency.get(current, []):
                queue.append(edge.target)

        return targets

    def trace_backward(self, o3_element: str, o3_attribute: str) -> list[LineageNode]:
        """Find all source nodes that feed an O3 attribute.

        Uses frozen LineageNode equality — see trace_forward docstring.
        """
        target = LineageNode(node_type="target", table=o3_element, column=o3_attribute)
        visited: set[LineageNode] = set()
        sources: list[LineageNode] = []
        queue: deque[LineageNode] = deque([target])

        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            if current.node_type == "source":
                sources.append(current)
            for edge in self.__reverse.get(current, []):
                queue.append(edge.source)

        return sources

    def unmapped_sources(self) -> list[LineageNode]:
        """DWH source nodes with no outgoing edges."""
        source_nodes = {n for n in self.nodes if n.node_type == "source"}
        connected = {e.source for e in self.edges}
        return sorted(source_nodes - connected, key=lambda n: (n.table, n.column))

    def unmapped_targets(self) -> list[LineageNode]:
        """O3 target nodes with no incoming edges."""
        target_nodes = {n for n in self.nodes if n.node_type == "target"}
        connected = {e.target for e in self.edges}
        return sorted(target_nodes - connected, key=lambda n: (n.table, n.column))


class LineageBuilder:
    """Constructs a LineageGraph from crosswalk entries and manifest data."""

    def __init__(
        self,
        crosswalk: list[CrosswalkEntry],
        manifest: SemanticManifest,
        o3_model: O3DataModel,
    ):
        self.__crosswalk = crosswalk
        self.__manifest = manifest
        self.__o3_model = o3_model

    def build(self) -> LineageGraph:
        nodes: set[LineageNode] = set()
        edges: list[LineageEdge] = []

        # Add all O3 attributes as target nodes
        for ke_name, key_element in self.__o3_model.key_elements.items():
            for attr in key_element.list_attributes:
                nodes.add(
                    LineageNode(
                        node_type="target",
                        table=ke_name,
                        column=attr.value_name,
                        metadata={"data_type": attr.value_data_type},
                    )
                )

        # Add all DWH columns as source nodes
        for table in self.__manifest.tables.values():
            for col in table.columns:
                nodes.add(
                    LineageNode(
                        node_type="source",
                        table=table.full_name,
                        column=col.name,
                        metadata={"data_type": col.data_type},
                    )
                )

        # Build edges from active crosswalk entries
        active = [e for e in self.__crosswalk if e.is_active]
        for entry in active:
            source = LineageNode(
                node_type="source",
                table=entry.dwh_table,
                column=entry.dwh_column,
            )
            target = LineageNode(
                node_type="target",
                table=entry.o3_key_element,
                column=entry.o3_attribute,
            )
            nodes.add(source)
            nodes.add(target)

            if entry.model_expr and entry.model_expr != entry.dwh_column:
                # Insert transform node
                transform = LineageNode(
                    node_type="transform",
                    table=entry.model_name or "",
                    column=entry.model_alias or entry.dwh_column,
                    metadata={"expr": entry.model_expr},
                )
                nodes.add(transform)
                edges.append(
                    LineageEdge(
                        source=source,
                        target=transform,
                        model_name=entry.model_name,
                        confidence=entry.confidence,
                    )
                )
                edges.append(
                    LineageEdge(
                        source=transform,
                        target=target,
                        transform_expr=entry.model_expr,
                        model_name=entry.model_name,
                        confidence=entry.confidence,
                    )
                )
            else:
                edges.append(
                    LineageEdge(
                        source=source,
                        target=target,
                        model_name=entry.model_name,
                        confidence=entry.confidence,
                    )
                )

        graph = LineageGraph(nodes=list(nodes), edges=edges)
        return graph


if __name__ == "__main__":
    pass
