import logging
import os
from typing import DefaultDict, Dict, List, Set

from rich.progress import (  # TimeRemainingColumn,
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

logger = logging.getLogger(__package__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    f"[%(asctime)s] [{os.getpid()}] [%(levelname)s] - %(name)s: %(message)s",
    "%Y/%m/%d %H:%M:%S %z",
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def rdf_track(text: str, color: str) -> Progress:
    return Progress(
        TextColumn(text),
        BarColumn(complete_style=color, finished_style=color),
        TaskProgressColumn(),
        TextColumn("({task.completed}/{task.total})"),
        TimeElapsedColumn(),
        # TimeRemainingColumn()
    )


def adb_track(text: str) -> Progress:
    return Progress(
        TextColumn(text),
        TimeElapsedColumn(),
        TextColumn("{task.fields[action]}"),
        SpinnerColumn("aesthetic", "#5BC0DE"),
    )


class Node:
    def __init__(self, name: str, depth: int = 0) -> None:
        self.name = name
        self.depth = depth
        self.children: List[Node] = []


class Tree:
    def __init__(self, root: Node, submap: DefaultDict[str, Set[str]]) -> None:
        self.root = root
        self.submap = submap
        self.nodes: Dict[str, Node] = {}
        self.build_tree(root, root.name)

    def build_tree(self, current: Node, parent: str, depth: int = 0) -> None:
        self.nodes[current.name] = current
        for sub_val in self.submap[parent]:
            child_node = Node(sub_val, depth + 1)
            current.children.append(child_node)
            self.build_tree(child_node, child_node.name, depth + 1)

    def get_node_depth(self, node_id: str) -> int:
        return self.nodes[node_id].depth if node_id in self.nodes else -1

    def __contains__(self, node_id: str) -> bool:
        return node_id in self.nodes

    def show(self) -> None:  # pragma: no cover
        self.show_rec(self.root)

    def show_rec(self, node: Node) -> None:  # pragma: no cover
        print("|" + "-" * node.depth + node.name)
        for child_node in node.children:
            self.show_rec(child_node)
