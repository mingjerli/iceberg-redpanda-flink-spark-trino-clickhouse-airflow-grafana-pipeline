#!/usr/bin/env python3
"""Export Excalidraw file to PNG using browser automation."""

import asyncio
import base64
import json
from pathlib import Path

from playwright.async_api import async_playwright


async def export_to_png(excalidraw_path: str, output_path: str, debug: bool = False):
    """Open Excalidraw in browser, import file, and export as PNG."""

    excalidraw_path = Path(excalidraw_path).resolve()
    output_path = Path(output_path).resolve()

    if not excalidraw_path.exists():
        raise FileNotFoundError(f"Excalidraw file not found: {excalidraw_path}")

    # Read the excalidraw file content
    with open(excalidraw_path, "r") as f:
        excalidraw_data = json.load(f)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            accept_downloads=True,
        )
        page = await context.new_page()

        print("Opening Excalidraw...")
        await page.goto("https://excalidraw.com/", wait_until="networkidle")
        await page.wait_for_timeout(2000)

        # Dismiss the welcome screen by pressing Escape
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(500)

        # Load data by simulating a file drop
        print("Loading diagram via file drop simulation...")
        json_content = json.dumps(excalidraw_data)

        await page.evaluate(f"""
            async () => {{
                const jsonContent = {json.dumps(json_content)};
                const file = new File([jsonContent], 'architecture.excalidraw', {{
                    type: 'application/json'
                }});

                const dataTransfer = new DataTransfer();
                dataTransfer.items.add(file);

                const dropEvent = new DragEvent('drop', {{
                    bubbles: true,
                    cancelable: true,
                    dataTransfer: dataTransfer
                }});

                const canvas = document.querySelector('canvas') || document.querySelector('.excalidraw') || document.body;
                canvas.dispatchEvent(dropEvent);
            }}
        """)

        await page.wait_for_timeout(3000)

        # Fit to screen using Shift+1
        await page.keyboard.press("Shift+1")
        await page.wait_for_timeout(1000)

        if debug:
            await page.screenshot(path="/tmp/excalidraw_01_fitted.png")
            print("Screenshot saved: /tmp/excalidraw_01_fitted.png")

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Use JavaScript to export as PNG via canvas
        print("Exporting via canvas API...")

        # Select all elements first
        await page.keyboard.press("Control+a")
        await page.wait_for_timeout(500)

        # Get the canvas and export it
        png_data = await page.evaluate("""
            async () => {
                // Find the main Excalidraw canvas
                const canvas = document.querySelector('canvas');
                if (!canvas) {
                    throw new Error('Canvas not found');
                }

                // Get the canvas as PNG data URL
                return canvas.toDataURL('image/png');
            }
        """)

        if png_data and png_data.startswith('data:image/png;base64,'):
            # Decode base64 and save
            base64_data = png_data.split(',')[1]
            png_bytes = base64.b64decode(base64_data)
            with open(output_path, 'wb') as f:
                f.write(png_bytes)
            print(f"Saved canvas PNG to: {output_path}")
        else:
            print("Canvas export failed, falling back to screenshot...")
            # Fallback: take a screenshot of the canvas area
            canvas = page.locator('canvas').first
            await canvas.screenshot(path=str(output_path))
            print(f"Saved canvas screenshot to: {output_path}")

        await browser.close()


async def main():
    script_dir = Path(__file__).parent.parent
    excalidraw_file = script_dir / "docs" / "architecture.excalidraw"
    output_file = script_dir / "docs" / "architecture.png"

    await export_to_png(str(excalidraw_file), str(output_file), debug=False)


if __name__ == "__main__":
    asyncio.run(main())
