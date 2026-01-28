#!/usr/bin/env python3
"""Export Excalidraw file to PNG/SVG using browser automation."""

import asyncio
import base64
import json
from pathlib import Path

from playwright.async_api import async_playwright


async def export_to_image(
    excalidraw_path: str,
    output_path: str,
    debug: bool = False,
):
    """Open Excalidraw in browser, import file, and export via preview screenshot."""
    excalidraw_path = Path(excalidraw_path).resolve()
    output_path = Path(output_path).resolve()

    if not excalidraw_path.exists():
        raise FileNotFoundError(f"Excalidraw file not found: {excalidraw_path}")

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

        await page.keyboard.press("Escape")
        await page.wait_for_timeout(500)

        # Load data via file drop
        print("Loading diagram...")
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

                const canvas = document.querySelector('canvas') || document.body;
                canvas.dispatchEvent(dropEvent);
            }}
        """)

        await page.wait_for_timeout(3000)
        await page.keyboard.press("Shift+1")
        await page.wait_for_timeout(1000)

        # Open export dialog
        print("Opening export dialog...")
        hamburger = page.locator("button").first
        await hamburger.click()
        await page.wait_for_timeout(500)

        export_item = page.locator('button:has-text("Export image")')
        await export_item.click()
        await page.wait_for_timeout(1500)

        # Set scale to 2x
        scale_button = page.locator('text=2×')
        if await scale_button.count() > 0:
            await scale_button.first.click()
            await page.wait_for_timeout(500)

        # Turn OFF background by finding and clicking the toggle
        print("Turning off background...")

        # Get HTML of the modal for debugging
        if debug:
            modal_html = await page.evaluate("""
                () => {
                    const modal = document.querySelector('.Modal, [class*="Modal"]');
                    if (modal) {
                        // Find the Background row
                        const bgRow = Array.from(modal.querySelectorAll('*')).find(
                            el => el.textContent.includes('Background') && el.querySelector('button')
                        );
                        if (bgRow) {
                            return bgRow.innerHTML;
                        }
                    }
                    return 'Modal not found';
                }
            """)
            print(f"Background row HTML: {modal_html[:500] if modal_html else 'None'}...")

        # Try clicking the toggle using JavaScript directly
        await page.evaluate("""
            () => {
                // Find all toggle switches
                const switches = document.querySelectorAll('[role="switch"]');
                console.log('Found switches:', switches.length);

                // The Background toggle should be the first one
                if (switches.length > 0) {
                    const bgSwitch = switches[0];
                    console.log('Background switch aria-checked:', bgSwitch.getAttribute('aria-checked'));
                    if (bgSwitch.getAttribute('aria-checked') === 'true') {
                        bgSwitch.click();
                        console.log('Clicked background switch');
                    }
                }
            }
        """)
        await page.wait_for_timeout(500)

        if debug:
            await page.screenshot(path="/tmp/excalidraw_01_dialog.png")
            print("Screenshot saved: /tmp/excalidraw_01_dialog.png")

        # Get the preview image data directly from the canvas in the modal
        output_path.parent.mkdir(parents=True, exist_ok=True)

        print("Extracting preview image...")

        # The preview is rendered to a canvas, not an img
        png_data = await page.evaluate("""
            () => {
                // Find canvas in the modal (the preview canvas)
                const modal = document.querySelector('.Modal, [class*="Modal"]');
                if (!modal) return null;

                // Look for the preview canvas
                const canvases = modal.querySelectorAll('canvas');
                for (const canvas of canvases) {
                    if (canvas.width > 100 && canvas.height > 100) {
                        return canvas.toDataURL('image/png');
                    }
                }

                // Fallback: look for img with data URL
                const img = modal.querySelector('img[src^="data:"]');
                if (img) {
                    return img.src;
                }

                return null;
            }
        """)

        if png_data and png_data.startswith('data:image/png'):
            base64_data = png_data.split(",", 1)[1]
            png_bytes = base64.b64decode(base64_data)
            with open(output_path, "wb") as f:
                f.write(png_bytes)
            print(f"Saved PNG from preview to: {output_path}")
        else:
            print("Could not extract preview, falling back to canvas screenshot...")
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(500)
            canvas = page.locator("canvas").first
            await canvas.screenshot(path=str(output_path))
            print(f"Saved canvas screenshot to: {output_path}")

        await browser.close()


async def main():
    script_dir = Path(__file__).parent.parent
    excalidraw_file = script_dir / "docs" / "architecture.excalidraw"
    output_file = script_dir / "docs" / "architecture.png"

    await export_to_image(
        str(excalidraw_file),
        str(output_file),
        debug=False,
    )


if __name__ == "__main__":
    asyncio.run(main())
