#!/usr/bin/env python3
"""Build the deployable site from the repository sources.

The source of truth stays `index.html` (one file, edited and tested as-is).
For deployment this script:

* moves large inline <script> and <style> blocks of index.html into
  content-hashed files under /assets/ (minified with esbuild), so browsers
  cache them for a year and the HTML itself shrinks to the markup;
* also content-hashes the classic scripts/styles index.html loads
  (ads.js, vehicle-positions.js, ...) and rewrites their URLs;
* stamps `window.__SG_BUILD` with the git revision for client telemetry;
* copies every other published file unchanged.

Small inline scripts (telemetry, analytics, JSON-LD, environment setup) stay
inline: they must run before anything else loads.

Usage: python3 scripts/build_site.py --out _site
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ESBUILD = ["npx", "--yes", "esbuild@0.24.2"]
INLINE_EXTRACT_MIN_BYTES = 20_000
COPY_ITEMS = [
    "auth-callback.html", "dev.config.js", "legal.css", "robots.txt", "sitemap.xml", "CNAME",
    "fonts", "auth", "page", "images", "privacy", "terms",
]
HASHED_ASSETS = ["ads.js", "ads.css", "gps-network.js", "vehicle-positions.js", "vehicle-positions.css"]


def esbuild_minify(source: str, loader: str) -> str:
    proc = subprocess.run(
        ESBUILD + [f"--loader={loader}", "--minify", "--target=es2017,chrome64,safari12", "--log-level=warning"]
        if loader == "js" else ESBUILD + [f"--loader={loader}", "--minify", "--log-level=warning"],
        input=source.encode("utf-8"), capture_output=True, check=False,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr.decode("utf-8", "replace"))
        raise SystemExit(f"esbuild failed for {loader}")
    return proc.stdout.decode("utf-8")


def write_hashed(out: Path, stem: str, ext: str, content: bytes) -> str:
    digest = hashlib.sha256(content).hexdigest()[:12]
    name = f"{stem}.{digest}.{ext}"
    (out / "assets").mkdir(parents=True, exist_ok=True)
    (out / "assets" / name).write_bytes(content)
    return f"/assets/{name}"


def git_revision() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "--short=12", "HEAD"], cwd=ROOT, capture_output=True,
                              text=True, check=True).stdout.strip()
    except Exception:
        return "unknown"


def build_index(out: Path, revision: str, minify: bool) -> dict:
    html = (ROOT / "index.html").read_text(encoding="utf-8")
    report = {"source_bytes": len(html.encode()), "extracted": []}

    # Hash the classic external assets and rewrite their references.
    for asset in HASHED_ASSETS:
        src = (ROOT / asset).read_bytes()
        stem, ext = asset.rsplit(".", 1)
        url = write_hashed(out, stem, ext, src)
        html, n = re.subn(r'(src|href)="/?' + re.escape(asset) + r'(\?v=[^"]*)?"', rf'\1="{url}"', html)
        if n == 0:
            raise SystemExit(f"{asset} is not referenced from index.html")

    counter = {"js": 0, "css": 0}

    def extract_script(match: re.Match) -> str:
        attrs, body = match.group(1), match.group(2)
        if attrs.strip() or len(body.encode()) < INLINE_EXTRACT_MIN_BYTES:
            return match.group(0)
        code = esbuild_minify(body, "js") if minify else body
        counter["js"] += 1
        url = write_hashed(out, f"app{counter['js']}", "js", code.encode("utf-8"))
        report["extracted"].append({"kind": "js", "url": url, "source": len(body.encode()), "built": len(code.encode())})
        return f'<script src="{url}"></script>'

    def extract_style(match: re.Match) -> str:
        attrs, body = match.group(1), match.group(2)
        if attrs.strip() or len(body.encode()) < INLINE_EXTRACT_MIN_BYTES:
            return match.group(0)
        code = esbuild_minify(body, "css") if minify else body
        counter["css"] += 1
        url = write_hashed(out, f"app{counter['css']}", "css", code.encode("utf-8"))
        report["extracted"].append({"kind": "css", "url": url, "source": len(body.encode()), "built": len(code.encode())})
        return f'<link rel="stylesheet" href="{url}" />'

    html = re.sub(r"<style\b([^>]*)>(.*?)</style>", extract_style, html, flags=re.S)
    html = re.sub(r"<script\b([^>]*)>(.*?)</script>", extract_script, html, flags=re.S)
    stamp = f'<script>window.__SG_BUILD={json.dumps(revision)};</script>'
    html = html.replace("<head>", "<head>\n  " + stamp, 1)
    (out / "index.html").write_text(html, encoding="utf-8")
    report["html_bytes"] = len(html.encode())
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="_site")
    parser.add_argument("--no-minify", action="store_true")
    args = parser.parse_args(argv)
    out = Path(args.out).resolve()
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    # Unhashed copies stay published for pages cached before this build
    # (they reference e.g. ads.js?v=4) until those caches expire.
    for item in COPY_ITEMS + HASHED_ASSETS:
        src = ROOT / item
        if src.is_dir():
            shutil.copytree(src, out / item)
        elif src.exists():
            shutil.copy2(src, out / item)
    report = build_index(out, git_revision(), not args.no_minify)
    (out / ".nojekyll").touch()
    print(json.dumps(report, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
