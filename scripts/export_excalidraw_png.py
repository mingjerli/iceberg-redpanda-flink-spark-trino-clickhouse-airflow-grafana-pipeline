#!/usr/bin/env python3
"""Export Excalidraw file to PNG/SVG using browser automation.

Supports all Excalidraw export options:
- Format: PNG or SVG
- Scale: 1x, 2x, or 3x
- Background: on/off
- Dark mode: on/off
- Embed scene: on/off (embeds .excalidraw data in the image)
"""

import argparse
import asyncio
import base64
import json
from pathlib import Path

from playwright.async_api import async_playwright


async def export_to_image(
    excalidraw_path: str,
    output_path: str | None = None,
    format: str = "png",
    scale: int = 2,
    background: bool = True,
    dark_mode: bool = False,
    embed_scene: bool = False,
    debug: bool = False,
):
    """Export an Excalidraw file to PNG or SVG.

    Args:
        excalidraw_path: Path to the .excalidraw file
        output_path: Output path (default: same name with .png/.svg extension)
        format: Output format - 'png' or 'svg'
        scale: Export scale 1, 2, or 3 (default: 2)
        background: Include background (default: True)
        dark_mode: Use dark mode (default: False)
        embed_scene: Embed scene data in image (default: False)
        debug: Save debug screenshots (default: False)
    """
    excalidraw_path = Path(excalidraw_path).resolve()

    if output_path is None:
        output_path = excalidraw_path.with_suffix(f".{format}")
    else:
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

        # Capture console logs for debugging
        page.on("console", lambda msg: print(f"BROWSER: {msg.text}") if debug else None)

        # Mock File System Access API to capture saved content
        await page.add_init_script("""
            window.__savedContent = null;
            window.__savedFilename = null;

            // Mock showSaveFilePicker that captures the content instead of saving
            window.showSaveFilePicker = async (options) => {
                console.log('showSaveFilePicker called with options:', JSON.stringify(options));

                // Return a mock FileSystemFileHandle
                return {
                    createWritable: async () => {
                        let chunks = [];
                        return {
                            write: async (data) => {
                                console.log('write() called, data type:', typeof data, data instanceof Blob ? 'Blob' : '');
                                if (data instanceof Blob) {
                                    const text = await data.text();
                                    chunks.push(text);
                                    window.__savedContent = text;
                                    console.log('Captured content length:', text.length);
                                } else if (typeof data === 'string') {
                                    chunks.push(data);
                                    window.__savedContent = data;
                                }
                            },
                            close: async () => {
                                console.log('WritableStream closed');
                                if (chunks.length > 0) {
                                    window.__savedContent = chunks.join('');
                                }
                            }
                        };
                    },
                    name: options?.suggestedName || 'export.svg'
                };
            };
        """)

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
                const file = new File([jsonContent], 'diagram.excalidraw', {{
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
        await page.keyboard.press("Shift+1")  # Fit to screen
        await page.wait_for_timeout(1000)

        # Open export dialog
        print("Opening export dialog...")
        hamburger = page.locator("button").first
        await hamburger.click()
        await page.wait_for_timeout(500)

        export_item = page.locator('button:has-text("Export image")')
        await export_item.click()
        await page.wait_for_timeout(1500)

        # Configure export options
        print(f"Configuring export: format={format}, scale={scale}x, background={background}, dark_mode={dark_mode}, embed_scene={embed_scene}")

        # Set scale (1x, 2x, or 3x)
        scale_button = page.locator(f'text={scale}×')
        if await scale_button.count() > 0:
            await scale_button.first.click()
            await page.wait_for_timeout(300)

        # Configure toggles using JavaScript
        # Toggle order in Excalidraw export dialog:
        # 0: Background
        # 1: Dark mode
        # 2: Embed scene
        await page.evaluate(f"""
            () => {{
                const switches = document.querySelectorAll('[role="switch"]');
                console.log('Found', switches.length, 'toggle switches');

                // Background toggle (index 0)
                if (switches.length > 0) {{
                    const bgSwitch = switches[0];
                    const isOn = bgSwitch.getAttribute('aria-checked') === 'true';
                    const wantOn = {'true' if background else 'false'};
                    if (isOn !== wantOn) {{
                        bgSwitch.click();
                        console.log('Toggled background');
                    }}
                }}

                // Dark mode toggle (index 1)
                if (switches.length > 1) {{
                    const darkSwitch = switches[1];
                    const isOn = darkSwitch.getAttribute('aria-checked') === 'true';
                    const wantOn = {'true' if dark_mode else 'false'};
                    if (isOn !== wantOn) {{
                        darkSwitch.click();
                        console.log('Toggled dark mode');
                    }}
                }}

                // Embed scene toggle (index 2)
                if (switches.length > 2) {{
                    const embedSwitch = switches[2];
                    const isOn = embedSwitch.getAttribute('aria-checked') === 'true';
                    const wantOn = {'true' if embed_scene else 'false'};
                    if (isOn !== wantOn) {{
                        embedSwitch.click();
                        console.log('Toggled embed scene');
                    }}
                }}
            }}
        """)
        await page.wait_for_timeout(500)

        if debug:
            await page.screenshot(path="/tmp/excalidraw_export_dialog.png")
            print("Debug screenshot saved: /tmp/excalidraw_export_dialog.png")

        # Extract the image
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if format == "svg":
            print("Extracting SVG...")

            # Setup to intercept downloads
            svg_content = None

            async def handle_download(download):
                nonlocal svg_content
                print(f"Download started: {download.suggested_filename}")
                if download.suggested_filename.endswith('.svg'):
                    # Save to temp and read content
                    temp_path = str(output_path) + '.tmp'
                    await download.save_as(temp_path)
                    with open(temp_path, 'r') as f:
                        svg_content = f.read()
                    import os
                    os.remove(temp_path)
                    print(f"Captured SVG from download, length: {len(svg_content)}")

            page.on("download", handle_download)

            await page.evaluate("""
                () => {
                    window.__svgContent = null;

                    // Capture blob content when created
                    const originalCreateObjectURL = URL.createObjectURL;
                    URL.createObjectURL = function(blob) {
                        const url = originalCreateObjectURL.call(URL, blob);
                        console.log('createObjectURL called, type:', blob?.type, 'size:', blob?.size);
                        if (blob?.type?.includes('svg') || blob?.type?.includes('xml')) {
                            blob.text().then(text => {
                                console.log('Captured SVG blob content, length:', text.length);
                                window.__svgContent = text;
                            });
                        }
                        return url;
                    };
                }
            """)

            await page.wait_for_timeout(500)

            # Click SVG button
            svg_btn = page.locator('button:has-text("SVG")').last
            print(f"Found SVG buttons: {await svg_btn.count()}")

            if await svg_btn.count() > 0:
                print("Clicking SVG button...")
                await svg_btn.click()
                await page.wait_for_timeout(5000)

            # Check if we got the SVG from mock File System API
            svg_data = await page.evaluate("() => window.__savedContent")
            print(f"Captured from mock API: {bool(svg_data)}, length: {len(svg_data) if svg_data else 0}")

            if svg_data and '<svg' in svg_data:
                with open(output_path, "w") as f:
                    f.write(svg_data)
                print(f"Saved SVG to: {output_path}")
            elif svg_content and '<svg' in svg_content:
                # Fallback: download event
                with open(output_path, "w") as f:
                    f.write(svg_content)
                print(f"Saved SVG to: {output_path}")
            else:
                # Last fallback: try __svgContent
                svg_data = await page.evaluate("() => window.__svgContent")
                if svg_data and '<svg' in svg_data:
                    with open(output_path, "w") as f:
                        f.write(svg_data)
                    print(f"Saved SVG to: {output_path}")
                else:
                    print("Could not capture SVG content")

        else:  # PNG
            print("Extracting PNG...")
            # Make sure PNG tab is selected
            png_tab = page.locator('button:has-text("PNG")')
            if await png_tab.count() > 0:
                await png_tab.click()
                await page.wait_for_timeout(500)

            # Get PNG from the preview canvas
            png_data = await page.evaluate("""
                () => {
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
                    const img = modal.querySelector('img[src^="data:image/png"]');
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
                print(f"Saved PNG to: {output_path}")
            else:
                print("Could not extract PNG preview, using download method...")
                # Trigger download
                async with page.expect_download() as download_info:
                    download_btn = page.locator('button:has-text("Export to PNG")')
                    if await download_btn.count() > 0:
                        await download_btn.click()
                download = await download_info.value
                await download.save_as(str(output_path))
                print(f"Downloaded PNG to: {output_path}")

        await browser.close()
        print("Export complete!")


def main():
    parser = argparse.ArgumentParser(
        description="Export Excalidraw files to PNG or SVG",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic PNG export (default settings)
  python export_excalidraw_png.py diagram.excalidraw

  # Export as SVG
  python export_excalidraw_png.py diagram.excalidraw -f svg

  # High resolution PNG without background
  python export_excalidraw_png.py diagram.excalidraw -s 3 --no-background

  # Dark mode with embedded scene data
  python export_excalidraw_png.py diagram.excalidraw --dark-mode --embed-scene

  # Specify output path
  python export_excalidraw_png.py diagram.excalidraw -o docs/images/arch.png
        """
    )

    parser.add_argument("input", help="Path to .excalidraw file")
    parser.add_argument("-o", "--output", help="Output path (default: same name with .png/.svg)")
    parser.add_argument(
        "-f", "--format",
        choices=["png", "svg"],
        default="png",
        help="Output format (default: png)"
    )
    parser.add_argument(
        "-s", "--scale",
        type=int,
        choices=[1, 2, 3],
        default=2,
        help="Export scale 1x, 2x, or 3x (default: 2)"
    )
    parser.add_argument(
        "--background/--no-background",
        dest="background",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Include background (default: on)"
    )
    parser.add_argument(
        "--dark-mode",
        action="store_true",
        default=False,
        help="Use dark mode (default: off)"
    )
    parser.add_argument(
        "--embed-scene",
        action="store_true",
        default=False,
        help="Embed scene data in image (default: off)"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Save debug screenshots"
    )

    args = parser.parse_args()

    asyncio.run(export_to_image(
        excalidraw_path=args.input,
        output_path=args.output,
        format=args.format,
        scale=args.scale,
        background=args.background,
        dark_mode=args.dark_mode,
        embed_scene=args.embed_scene,
        debug=args.debug,
    ))


if __name__ == "__main__":
    main()
