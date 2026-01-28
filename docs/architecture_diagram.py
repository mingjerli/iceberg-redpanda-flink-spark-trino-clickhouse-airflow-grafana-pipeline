#!/usr/bin/env python3
"""
Architecture Diagram Generator
==============================

Generates an Excalidraw-compatible architecture diagram for the
Iceberg + Redpanda + Flink + Spark + Trino + ClickHouse + Airflow + Grafana pipeline.

Usage:
    python architecture_diagram.py

Output:
    - docs/architecture.excalidraw (Excalidraw JSON)
    - docs/architecture.png (PNG export via Excalidraw CLI if available)
"""

import json
import uuid
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Element:
    """Base Excalidraw element."""
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    type: str = "rectangle"
    x: int = 0
    y: int = 0
    width: int = 200
    height: int = 100
    strokeColor: str = "#1e1e1e"
    backgroundColor: str = "#a5d8ff"
    fillStyle: str = "solid"
    strokeWidth: int = 2
    roughness: int = 1
    opacity: int = 100
    groupIds: list = field(default_factory=list)
    roundness: Optional[dict] = field(default_factory=lambda: {"type": 3})
    text: str = ""

    def to_dict(self) -> dict:
        base = {
            "id": self.id,
            "type": self.type,
            "x": self.x,
            "y": self.y,
            "width": self.width,
            "height": self.height,
            "strokeColor": self.strokeColor,
            "backgroundColor": self.backgroundColor,
            "fillStyle": self.fillStyle,
            "strokeWidth": self.strokeWidth,
            "roughness": self.roughness,
            "opacity": self.opacity,
            "groupIds": self.groupIds,
            "seed": hash(self.id) % 2147483647,
            "version": 1,
            "versionNonce": hash(self.id + "v") % 2147483647,
            "isDeleted": False,
            "boundElements": None,
            "updated": 1704067200000,
            "link": None,
            "locked": False,
        }
        if self.roundness:
            base["roundness"] = self.roundness
        return base


def create_box(x: int, y: int, w: int, h: int, color: str, label: str) -> list:
    """Create a labeled box with text."""
    box_id = str(uuid.uuid4())[:8]
    text_id = str(uuid.uuid4())[:8]

    box = {
        "id": box_id,
        "type": "rectangle",
        "x": x,
        "y": y,
        "width": w,
        "height": h,
        "strokeColor": "#1e1e1e",
        "backgroundColor": color,
        "fillStyle": "solid",
        "strokeWidth": 2,
        "roughness": 1,
        "opacity": 100,
        "groupIds": [],
        "roundness": {"type": 3},
        "seed": hash(box_id) % 2147483647,
        "version": 1,
        "versionNonce": hash(box_id + "v") % 2147483647,
        "isDeleted": False,
        "boundElements": [{"id": text_id, "type": "text"}],
        "updated": 1704067200000,
        "link": None,
        "locked": False,
    }

    text = {
        "id": text_id,
        "type": "text",
        "x": x + w // 2,
        "y": y + h // 2,
        "width": len(label) * 8,
        "height": 25,
        "strokeColor": "#1e1e1e",
        "backgroundColor": "transparent",
        "fillStyle": "solid",
        "strokeWidth": 1,
        "roughness": 1,
        "opacity": 100,
        "groupIds": [],
        "roundness": None,
        "seed": hash(text_id) % 2147483647,
        "version": 1,
        "versionNonce": hash(text_id + "v") % 2147483647,
        "isDeleted": False,
        "boundElements": None,
        "updated": 1704067200000,
        "link": None,
        "locked": False,
        "text": label,
        "fontSize": 16,
        "fontFamily": 1,
        "textAlign": "center",
        "verticalAlign": "middle",
        "containerId": box_id,
        "originalText": label,
    }

    return [box, text]


def create_arrow(x1: int, y1: int, x2: int, y2: int) -> dict:
    """Create an arrow between two points."""
    arrow_id = str(uuid.uuid4())[:8]
    return {
        "id": arrow_id,
        "type": "arrow",
        "x": x1,
        "y": y1,
        "width": x2 - x1,
        "height": y2 - y1,
        "strokeColor": "#1e1e1e",
        "backgroundColor": "transparent",
        "fillStyle": "solid",
        "strokeWidth": 2,
        "roughness": 1,
        "opacity": 100,
        "groupIds": [],
        "roundness": {"type": 2},
        "seed": hash(arrow_id) % 2147483647,
        "version": 1,
        "versionNonce": hash(arrow_id + "v") % 2147483647,
        "isDeleted": False,
        "boundElements": None,
        "updated": 1704067200000,
        "link": None,
        "locked": False,
        "points": [[0, 0], [x2 - x1, y2 - y1]],
        "lastCommittedPoint": None,
        "startBinding": None,
        "endBinding": None,
        "startArrowhead": None,
        "endArrowhead": "arrow",
    }


def generate_architecture_diagram() -> dict:
    """Generate the full architecture diagram."""
    elements = []

    # Colors
    BLUE = "#a5d8ff"      # Data sources
    GREEN = "#b2f2bb"     # Processing
    ORANGE = "#ffc078"    # Storage
    PURPLE = "#d0bfff"    # Query engines
    PINK = "#fcc2d7"      # Orchestration
    GRAY = "#dee2e6"      # Infrastructure

    # Layout constants
    START_X = 50
    START_Y = 50
    BOX_W = 150
    BOX_H = 60
    GAP = 30

    # Row 1: Data Sources
    y = START_Y
    sources = ["Shopify", "Stripe", "HubSpot"]
    for i, src in enumerate(sources):
        x = START_X + i * (BOX_W + GAP)
        elements.extend(create_box(x, y, BOX_W, BOX_H, BLUE, src))

    # Row 2: Ingestion API
    y += BOX_H + GAP * 2
    elements.extend(create_box(START_X + BOX_W + GAP // 2, y, BOX_W, BOX_H, GREEN, "FastAPI Ingestion"))

    # Row 3: Message Queue
    y += BOX_H + GAP * 2
    elements.extend(create_box(START_X + BOX_W + GAP // 2, y, BOX_W, BOX_H, ORANGE, "Redpanda"))

    # Row 4: Streaming (Flink)
    y += BOX_H + GAP * 2
    elements.extend(create_box(START_X + BOX_W + GAP // 2, y, BOX_W, BOX_H, GREEN, "Apache Flink"))

    # Row 5: Storage (Iceberg + MinIO)
    y += BOX_H + GAP * 2
    elements.extend(create_box(START_X, y, BOX_W, BOX_H, ORANGE, "MinIO (S3)"))
    elements.extend(create_box(START_X + BOX_W + GAP, y, BOX_W, BOX_H, ORANGE, "Iceberg REST"))
    elements.extend(create_box(START_X + 2 * (BOX_W + GAP), y, BOX_W, BOX_H, ORANGE, "PostgreSQL"))

    # Row 6: Batch Processing (Spark)
    y += BOX_H + GAP * 2
    elements.extend(create_box(START_X + BOX_W + GAP // 2, y, BOX_W, BOX_H, GREEN, "Apache Spark"))

    # Row 7: Orchestration (Airflow)
    y += BOX_H + GAP * 2
    elements.extend(create_box(START_X + BOX_W + GAP // 2, y, BOX_W, BOX_H, PINK, "Apache Airflow"))

    # Row 8: Query Engines
    y += BOX_H + GAP * 2
    engines = ["Trino", "ClickHouse"]
    for i, eng in enumerate(engines):
        x = START_X + i * (BOX_W + GAP) + BOX_W // 2
        elements.extend(create_box(x, y, BOX_W, BOX_H, PURPLE, eng))

    # Row 9: Monitoring
    y += BOX_H + GAP * 2
    elements.extend(create_box(START_X, y, BOX_W, BOX_H, GRAY, "Prometheus"))
    elements.extend(create_box(START_X + BOX_W + GAP, y, BOX_W, BOX_H, GRAY, "Grafana"))

    # Excalidraw document structure
    return {
        "type": "excalidraw",
        "version": 2,
        "source": "architecture_diagram.py",
        "elements": elements,
        "appState": {
            "gridSize": None,
            "viewBackgroundColor": "#ffffff"
        },
        "files": {}
    }


def main():
    diagram = generate_architecture_diagram()

    output_path = "docs/architecture.excalidraw"
    with open(output_path, "w") as f:
        json.dump(diagram, f, indent=2)

    print(f"Generated {output_path}")
    print("Open this file in Excalidraw (https://excalidraw.com) to view and export.")


if __name__ == "__main__":
    main()
