import logging
import os

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
