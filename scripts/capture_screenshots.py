#!/usr/bin/env python3
"""
Capture screenshots of all demo UI dashboards using Playwright.

Usage:
    uv run python scripts/capture_screenshots.py

Screenshots are saved to docs/screenshots/
"""

import asyncio
import os
from pathlib import Path
from playwright.async_api import async_playwright

# Output directory
SCREENSHOT_DIR = Path(__file__).parent.parent / "docs" / "screenshots"

# Get credentials from environment variables
AIRFLOW_ADMIN_USER = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
AIRFLOW_ADMIN_PASSWORD = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "admin123")
GRAFANA_ADMIN_USER = os.environ.get("GRAFANA_ADMIN_USER", "admin")
GRAFANA_ADMIN_PASSWORD = os.environ.get("GRAFANA_ADMIN_PASSWORD", "admin123")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "admin123")

# UI configurations: (name, url, auth, wait_for, actions)
UIS = [
    {
        "name": "airflow_dag_graph",
        "url": "http://localhost:8086/dags/iceberg_pipeline/graph",
        "auth": (AIRFLOW_ADMIN_USER, AIRFLOW_ADMIN_PASSWORD),
        "wait_for": "canvas",  # DAG graph canvas
        "width": 1400,
        "height": 900,
    },
    {
        "name": "airflow_dag_list",
        "url": "http://localhost:8086/dags",
        "auth": (AIRFLOW_ADMIN_USER, AIRFLOW_ADMIN_PASSWORD),
        "wait_for": "table",
        "width": 1400,
        "height": 700,
    },
    {
        "name": "grafana_dashboard",
        "url": "http://localhost:3000/dashboards",
        "auth": (GRAFANA_ADMIN_USER, GRAFANA_ADMIN_PASSWORD),
        "wait_for": "text=Dashboards",
        "width": 1400,
        "height": 800,
    },
    {
        "name": "spark_master",
        "url": "http://localhost:8084",
        "auth": None,
        "wait_for": "text=Spark Master",
        "width": 1200,
        "height": 800,
    },
    {
        "name": "flink_dashboard",
        "url": "http://localhost:8083",
        "auth": None,
        "wait_for": "text=Apache Flink",
        "width": 1200,
        "height": 800,
    },
    {
        "name": "flink_jobs",
        "url": "http://localhost:8083/#/job/running",
        "auth": None,
        "wait_for": "text=Running Jobs",
        "width": 1200,
        "height": 600,
    },
    {
        "name": "minio_console",
        "url": "http://localhost:9001/browser/warehouse",
        "auth": (MINIO_ROOT_USER, MINIO_ROOT_PASSWORD),
        "wait_for": "text=warehouse",
        "width": 1200,
        "height": 800,
    },
    {
        "name": "redpanda_console",
        "url": "http://localhost:8080/topics",
        "auth": None,
        "wait_for": "text=Topics",
        "width": 1200,
        "height": 700,
    },
    {
        "name": "prometheus_targets",
        "url": "http://localhost:9090/targets",
        "auth": None,
        "wait_for": "text=Targets",
        "width": 1200,
        "height": 800,
    },
]


async def capture_screenshot(page, ui_config: dict, screenshot_dir: Path):
    """Capture a screenshot of a single UI."""
    name = ui_config["name"]
    url = ui_config["url"]
    auth = ui_config.get("auth")
    wait_for = ui_config.get("wait_for")
    width = ui_config.get("width", 1200)
    height = ui_config.get("height", 800)

    print(f"  Capturing {name}...")

    try:
        # Set viewport
        await page.set_viewport_size({"width": width, "height": height})

        # Handle basic auth if needed
        if auth and "localhost:8086" in url:
            # Airflow uses form-based auth
            await page.goto("http://localhost:8086/login/")
            await page.fill('input#username', auth[0])
            await page.fill('input#password', auth[1])
            await page.click('input[type="submit"]')
            await asyncio.sleep(3)  # Wait for login
            await page.goto(url)
        elif auth and "localhost:3000" in url:
            # Grafana uses form-based auth
            await page.goto("http://localhost:3000/login")
            await page.fill('input[name="user"]', auth[0])
            await page.fill('input[name="password"]', auth[1])
            await page.click('button[type="submit"]')
            await asyncio.sleep(2)  # Wait for redirect
            await page.goto(url)
        elif auth and "localhost:9001" in url:
            # MinIO uses form-based auth
            await page.goto("http://localhost:9001/login")
            await page.fill('input[id="accessKey"]', auth[0])
            await page.fill('input[id="secretKey"]', auth[1])
            await page.click('button[type="submit"]')
            await asyncio.sleep(2)
            await page.goto(url)
        else:
            await page.goto(url)

        # Wait for content to load
        if wait_for:
            try:
                if wait_for.startswith("text="):
                    await page.wait_for_selector(f"text={wait_for[5:]}", timeout=10000)
                else:
                    await page.wait_for_selector(wait_for, timeout=10000)
            except Exception:
                pass  # Continue even if wait fails

        # Extra wait for rendering
        await asyncio.sleep(2)

        # Take screenshot
        screenshot_path = screenshot_dir / f"{name}.png"
        await page.screenshot(path=str(screenshot_path), full_page=False)
        print(f"    Saved: {screenshot_path.name}")
        return True

    except Exception as e:
        print(f"    Failed: {e}")
        return False


async def main():
    """Capture all screenshots."""
    print("Capturing UI screenshots...")

    # Create output directory
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        success = 0
        failed = 0

        for ui_config in UIS:
            if await capture_screenshot(page, ui_config, SCREENSHOT_DIR):
                success += 1
            else:
                failed += 1

        await browser.close()

    print(f"\nDone: {success} captured, {failed} failed")
    print(f"Screenshots saved to: {SCREENSHOT_DIR}")


if __name__ == "__main__":
    asyncio.run(main())
