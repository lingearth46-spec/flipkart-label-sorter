"""
Flipkart Shipping Label Extractor — Local Test Script
Extracts SKU ID and AWB No. from each page of a Flipkart shipping label PDF.
Run this first to validate extraction before building the full pipeline.

Usage:
    python barcode.py path/to/labels.pdf
"""

import sys
import re
import pdfplumber

# ──────────────────────────────────────────────────────────────
# Patterns — tuned to Flipkart label layout
# ──────────────────────────────────────────────────────────────

# AWB: alphanumeric, 10–20 chars, often prefixed by "AWB No." or "AWB:"
AWB_PATTERN = re.compile(
    r"AWB\s*(?:No\.?|#|:)?\s*([A-Z0-9]{10,20})",
    re.IGNORECASE,
)

# SKU ID: anchored on the table header that every Flipkart label has:
#   "SKU ID | Description QTY"
# followed by a row line like "1ZIG GOLD_5 | Litchi..." (qty glued to SKU).
# We strip the leading digit(s) and capture everything up to the next pipe.
SKU_PATTERN = re.compile(
    r"SKU\s*ID\s*\|\s*Description\s*QTY\s*\d\s*([A-Za-z0-9][A-Za-z0-9_\-\. ]*?)\s*\|",
    re.IGNORECASE,
)

# ──────────────────────────────────────────────────────────────
# Extraction helpers
# ──────────────────────────────────────────────────────────────

def extract_field(pattern: re.Pattern, text: str) -> str | None:
    """Return the first capture group of pattern in text, stripped."""
    match = pattern.search(text)
    if match:
        return match.group(1).strip()
    return None


def extract_page_fields(page_text: str) -> dict:
    """
    Parse a single page's raw text and return extracted fields.
    Returns a dict with keys: awb, sku, raw_text, errors.
    """
    result = {
        "awb": None,
        "sku": None,
        "raw_text": page_text,
        "errors": [],
    }

    # Collapse newlines to spaces so SKUs split across lines still match.
    flat_text = " ".join(page_text.split())

    result["awb"] = extract_field(AWB_PATTERN, flat_text)
    raw_sku = extract_field(SKU_PATTERN, flat_text)
    # Normalize any residual internal whitespace in SKU (e.g. "ZIG  GOLD_5")
    result["sku"] = " ".join(raw_sku.split()) if raw_sku else None

    if not result["awb"]:
        result["errors"].append("AWB No. not found")
    if not result["sku"]:
        result["errors"].append("SKU ID not found")

    return result


# ──────────────────────────────────────────────────────────────
# Main extraction loop
# ──────────────────────────────────────────────────────────────

def process_pdf(pdf_path: str, limit: int | None = None) -> list[dict]:
    """
    Open a PDF, extract SKU + AWB from every page (or first `limit` pages),
    and return a list of per-page result dicts.
    """
    results = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            pages_to_process = pdf.pages[:limit] if limit else pdf.pages
            print(f"\nOpened: {pdf_path}")
            print(f"Total pages: {total_pages}"
                  + (f"  (processing first {len(pages_to_process)})" if limit else ""))
            print()
            print("=" * 60)

            for i, page in enumerate(pages_to_process, start=1):
                text = page.extract_text() or ""
                fields = extract_page_fields(text)
                fields["page"] = i

                status = "OK" if not fields["errors"] else "WARN"
                print(f"Page {i:>4}/{total_pages}  [{status}]")
                print(f"  AWB : {fields['awb'] or '— NOT FOUND'}")
                print(f"  SKU : {fields['sku'] or '— NOT FOUND'}")
                if fields["errors"]:
                    for err in fields["errors"]:
                        print(f"  !! {err}")

                # Uncomment to see raw extracted text for debugging:
                # print("  --- RAW TEXT ---")
                # print(text[:500])
                # print("  ----------------")

                print()
                results.append(fields)

    except FileNotFoundError:
        print(f"ERROR: File not found — {pdf_path}")
        sys.exit(1)
    except Exception as exc:
        print(f"ERROR opening PDF: {exc}")
        sys.exit(1)

    return results


def print_summary(results: list[dict]) -> None:
    """Print extraction summary stats."""
    total = len(results)
    ok = sum(1 for r in results if not r["errors"])
    warn = total - ok

    skus_found = {r["sku"] for r in results if r["sku"]}
    awbs_found = {r["awb"] for r in results if r["awb"]}
    dup_awbs = {awb for awb in awbs_found
                if sum(1 for r in results if r["awb"] == awb) > 1}

    print("=" * 60)
    print("SUMMARY")
    print(f"  Pages processed : {total}")
    print(f"  Fully extracted : {ok}")
    print(f"  Warnings        : {warn}")
    print(f"  Unique SKUs     : {len(skus_found)}")
    print(f"  Unique AWBs     : {len(awbs_found)}")

    if dup_awbs:
        print(f"\n  DUPLICATE AWBs in this file ({len(dup_awbs)}):")
        for awb in sorted(dup_awbs):
            pages = [r["page"] for r in results if r["awb"] == awb]
            print(f"    {awb}  →  pages {pages}")

    print("\n  SKU breakdown:")
    for sku in sorted(skus_found):
        pages = [r["page"] for r in results if r["sku"] == sku]
        print(f"    {sku:<30}  {len(pages)} label(s)  pages: {pages}")

    print("=" * 60)


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python barcode.py <path_to_pdf> [--limit N]")
        sys.exit(1)

    pdf_file = sys.argv[1]
    limit = None
    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        limit = int(sys.argv[idx + 1])

    results = process_pdf(pdf_file, limit=limit)
    print_summary(results)
