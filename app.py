"""
Flipkart Shipping Label Sorter — Streamlit UI

Upload one or more Flipkart label PDFs:
- Extracts SKU + AWB from every page across all files
- Flags duplicate AWBs and forces a decision before downloads activate
- Per-SKU download button (PDF with all matching pages)
- SKU "pools" let you group multiple SKUs you know are the same product;
  pool names persist between sessions, SKU contents are picked fresh each upload

Run:
    streamlit run app.py
"""

import csv
import io
import json
from collections import defaultdict
from pathlib import Path

import streamlit as st
from pypdf import PdfReader, PdfWriter

from barcode import extract_page_fields


POOLS_FILE = Path(__file__).parent / "pools.json"


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def extract_pages_from_file(file_name: str, pdf_bytes: bytes,
                            progress, progress_msg) -> list[dict]:
    """Extract every page; each entry knows its source file + page index."""
    results = []
    reader = PdfReader(io.BytesIO(pdf_bytes))
    total = len(reader.pages)
    for i, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""
        fields = extract_page_fields(text)
        fields["source"] = file_name
        fields["src_page"] = i
        results.append(fields)
        if i % 10 == 0 or i == total:
            progress.progress(i / total, text=f"{progress_msg}: page {i}/{total}")
    return results


def build_pdf_from_entries(entries: list[dict],
                           source_bytes_map: dict[str, bytes]) -> bytes:
    """Build a PDF containing the pages referenced by the entries."""
    # Cache PdfReader per source file to avoid re-parsing
    readers: dict[str, PdfReader] = {}
    writer = PdfWriter()
    for e in entries:
        src = e["source"]
        if src not in readers:
            readers[src] = PdfReader(io.BytesIO(source_bytes_map[src]))
        writer.add_page(readers[src].pages[e["src_page"] - 1])
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def safe_filename(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in name)


def load_pool_names() -> list[str]:
    if POOLS_FILE.exists():
        try:
            return list(json.loads(POOLS_FILE.read_text()))
        except Exception:
            return []
    return []


def save_pool_names(names: list[str]) -> None:
    POOLS_FILE.write_text(json.dumps(sorted(set(names)), indent=2))


# ──────────────────────────────────────────────────────────────
# UI
# ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Flipkart Label Sorter",
    page_icon="📦",
    layout="wide",
)

st.markdown("""
<style>
  .hero {
    background: linear-gradient(135deg, #2874F0 0%, #1851A6 100%);
    padding: 1.8rem 2rem;
    border-radius: 14px;
    color: white;
    margin-bottom: 1.5rem;
    box-shadow: 0 8px 24px rgba(40, 116, 240, 0.25);
  }
  .hero h1 { margin: 0; color: white; font-size: 2.1rem; font-weight: 700; }
  .hero p  { margin: 0.4rem 0 0; color: rgba(255,255,255,0.92); font-size: 1rem; }
  .stDownloadButton button[kind="primary"] { font-weight: 600; }
</style>
<div class="hero">
  <h1>📦 Flipkart Label Sorter</h1>
  <p>Upload one or more shipping label PDFs — get clean, SKU-sorted PDFs in seconds.</p>
</div>
""", unsafe_allow_html=True)

uploaded_files = st.file_uploader(
    "Upload Flipkart label PDF(s)",
    type=["pdf"],
    accept_multiple_files=True,
)

if not uploaded_files:
    st.info("Drag one or more PDFs above to begin.")
    st.stop()

# Build source_bytes map and an upload signature (used to reset session state)
source_bytes_map: dict[str, bytes] = {}
for f in uploaded_files:
    source_bytes_map[f.name] = f.getvalue()
upload_signature = tuple(sorted((f.name, len(b)) for f, b in
                                zip(uploaded_files,
                                    source_bytes_map.values())))

# Reset the "duplicates resolved" flag whenever the upload set changes
if st.session_state.get("upload_signature") != upload_signature:
    st.session_state["upload_signature"] = upload_signature
    st.session_state["dupes_resolved"] = False
    st.session_state["dupes_action"] = None  # "remove" or "keep"


# ── Extraction (cached by file content) ──
@st.cache_data(show_spinner=False)
def _cached_extract_all(files_data: tuple[tuple[str, bytes], ...]) -> list[dict]:
    progress = st.progress(0.0, text="Reading…")
    all_results = []
    for fname, b in files_data:
        msg = f"Reading {fname}"
        all_results.extend(extract_pages_from_file(fname, b, progress, msg))
    progress.empty()
    return all_results

with st.spinner("Extracting SKU + AWB from each page…"):
    all_entries = _cached_extract_all(
        tuple((f.name, source_bytes_map[f.name]) for f in uploaded_files)
    )

# ── Top summary ──
total = len(all_entries)
ok = sum(1 for r in all_entries if not r["errors"])
warn = total - ok

sku_to_entries_raw: dict[str, list[dict]] = defaultdict(list)
for r in all_entries:
    sku = r["sku"] or "— UNKNOWN —"
    sku_to_entries_raw[sku].append(r)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Files", len(uploaded_files))
c2.metric("Total pages", total)
c3.metric("Fully extracted", ok)
c4.metric("Warnings", warn)
c5.metric("Unique SKUs", len(sku_to_entries_raw))

if warn:
    with st.expander(f"{warn} page(s) had extraction warnings", expanded=False):
        warn_rows = [
            {"File": r["source"], "Page": r["src_page"],
             "AWB": r["awb"] or "—", "SKU": r["sku"] or "—",
             "Issue": "; ".join(r["errors"])}
            for r in all_entries if r["errors"]
        ]
        st.dataframe(warn_rows, use_container_width=True, hide_index=True)

# ── Duplicate AWB detection ──
awb_to_entries: dict[str, list[dict]] = defaultdict(list)
for r in all_entries:
    if r["awb"]:
        awb_to_entries[r["awb"]].append(r)
duplicates = {awb: entries for awb, entries in awb_to_entries.items()
              if len(entries) > 1}

st.divider()

if duplicates:
    st.warning(f"⚠ {len(duplicates)} duplicate AWB(s) detected across uploads "
               f"({sum(len(v) for v in duplicates.values())} pages total). "
               f"You must choose how to handle them before downloads activate.")

    with st.expander("Show duplicate AWBs", expanded=True):
        dup_rows = []
        for awb, entries in duplicates.items():
            for e in entries:
                dup_rows.append({"AWB": awb, "File": e["source"],
                                 "Page": e["src_page"],
                                 "SKU": e["sku"] or "—"})
        st.dataframe(dup_rows, use_container_width=True, hide_index=True)

    if not st.session_state.get("dupes_resolved"):
        col_a, col_b = st.columns(2)
        if col_a.button("✓ Remove duplicates (keep first occurrence)",
                        type="primary", use_container_width=True):
            st.session_state["dupes_resolved"] = True
            st.session_state["dupes_action"] = "remove"
            st.rerun()
        if col_b.button("Keep all (proceed without removing)",
                        use_container_width=True):
            st.session_state["dupes_resolved"] = True
            st.session_state["dupes_action"] = "keep"
            st.rerun()
        st.stop()
    else:
        action = st.session_state.get("dupes_action")
        if action == "remove":
            st.success(f"Duplicates removed — kept first occurrence of each AWB.")
        else:
            st.info("Proceeding with duplicates kept.")

# Apply duplicate decision
final_entries: list[dict]
if duplicates and st.session_state.get("dupes_action") == "remove":
    seen_awbs: set[str] = set()
    final_entries = []
    for r in all_entries:
        awb = r["awb"]
        if awb and awb in seen_awbs:
            continue
        if awb:
            seen_awbs.add(awb)
        final_entries.append(r)
else:
    final_entries = all_entries

# Rebuild SKU groupings from final entries
sku_to_entries: dict[str, list[dict]] = defaultdict(list)
for r in final_entries:
    sku = r["sku"] or "— UNKNOWN —"
    sku_to_entries[sku].append(r)

# ──────────────────────────────────────────────────────────────
# Build a "fully sorted" PDF (all pages, grouped by SKU)
# ──────────────────────────────────────────────────────────────
sorted_skus = sorted(sku_to_entries.items(),
                     key=lambda kv: (-len(kv[1]), kv[0]))

available_skus = [sku for sku, _ in sorted_skus if sku != "— UNKNOWN —"]

sorted_entries: list[dict] = []
for _sku, _entries in sorted_skus:
    sorted_entries.extend(_entries)

# Build CSV of the extraction table
csv_buf = io.StringIO()
csv_writer = csv.writer(csv_buf)
csv_writer.writerow(["File", "Page", "AWB", "SKU", "Status"])
for r in final_entries:
    csv_writer.writerow([
        r["source"], r["src_page"], r["awb"] or "",
        r["sku"] or "", "WARN" if r["errors"] else "OK",
    ])
csv_data = csv_buf.getvalue().encode("utf-8")

st.divider()
st.subheader("📥 Quick exports")

qcol1, qcol2 = st.columns(2)
with qcol1:
    sorted_pdf = build_pdf_from_entries(sorted_entries, source_bytes_map)
    st.download_button(
        label=f"⬇ Download everything sorted ({len(sorted_entries)} pages)",
        data=sorted_pdf,
        file_name="all_labels_sorted_by_sku.pdf",
        mime="application/pdf",
        type="primary",
        use_container_width=True,
        help="One PDF with every page reordered so same-SKU labels are consecutive.",
    )
with qcol2:
    st.download_button(
        label=f"⬇ Download extraction table (CSV)",
        data=csv_data,
        file_name="extraction_table.csv",
        mime="text/csv",
        use_container_width=True,
        help="Spreadsheet of every page with its file, AWB, SKU, and status.",
    )

# ──────────────────────────────────────────────────────────────
# Per-SKU download buttons
# ──────────────────────────────────────────────────────────────
st.divider()
st.subheader("Download by SKU")

search_query = st.text_input(
    "🔍 Filter SKUs",
    placeholder="Type to filter (e.g. ZIG GOLD)",
    label_visibility="collapsed",
).strip().lower()

filtered_skus = [
    (sku, entries) for sku, entries in sorted_skus
    if not search_query or search_query in sku.lower()
]

if not filtered_skus:
    st.info(f"No SKUs match '{search_query}'.")
else:
    if search_query:
        st.caption(f"Showing {len(filtered_skus)} of {len(sorted_skus)} SKUs.")

    for sku, entries in filtered_skus:
        pages_preview = ", ".join(
            f"{e['source']}#{e['src_page']}" for e in entries[:5]
        )
        if len(entries) > 5:
            pages_preview += f", … (+{len(entries) - 5} more)"

        col1, col2, col3 = st.columns([4, 2, 2])
        col1.markdown(f"**{sku}**  \n<small>{pages_preview}</small>",
                      unsafe_allow_html=True)
        col2.write(f"{len(entries)} label(s)")

        pdf_bytes_out = build_pdf_from_entries(entries, source_bytes_map)
        col3.download_button(
            label="⬇ Download",
            data=pdf_bytes_out,
            file_name=f"{safe_filename(sku)}_{len(entries)}labels.pdf",
            mime="application/pdf",
            key=f"dl_{sku}",
            use_container_width=True,
        )

# ──────────────────────────────────────────────────────────────
# SKU Pools — lives in the sidebar (hamburger menu)
# ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 📦 SKU Pools")
    st.caption("Group multiple SKUs that are the same product. Pool names "
               "are saved between sessions; SKUs are picked fresh every upload.")

    pool_names = load_pool_names()

    with st.expander("➕ Create a new pool", expanded=not pool_names):
        new_pool_name = st.text_input(
            "Pool name",
            placeholder="e.g. ZIG GOLD all variants",
            key="new_pool_name",
        )
        if st.button("Create pool", key="create_pool_btn",
                     use_container_width=True):
            name = (new_pool_name or "").strip()
            if not name:
                st.warning("Pool name cannot be empty.")
            elif name in pool_names:
                st.warning(f"Pool '{name}' already exists.")
            else:
                pool_names.append(name)
                save_pool_names(pool_names)
                st.rerun()

    if not pool_names:
        st.info("No saved pools yet.")
    else:
        st.divider()
        for pool_name in sorted(pool_names):
            with st.container(border=True):
                head_a, head_b = st.columns([5, 1])
                head_a.markdown(f"**{pool_name}**")

                if head_b.button("🗑", key=f"delete_{pool_name}",
                                 help="Delete this pool"):
                    pool_names = [n for n in pool_names if n != pool_name]
                    save_pool_names(pool_names)
                    st.rerun()

                with st.popover("✏ Rename", use_container_width=True):
                    renamed = st.text_input("New name", value=pool_name,
                                            key=f"rename_input_{pool_name}")
                    if st.button("Save", key=f"rename_save_{pool_name}"):
                        new = renamed.strip()
                        if new and new != pool_name and new not in pool_names:
                            pool_names = [new if n == pool_name else n
                                          for n in pool_names]
                            save_pool_names(pool_names)
                            st.rerun()

                selected = st.multiselect(
                    "SKUs in this pool",
                    options=available_skus,
                    key=f"pool_skus_{pool_name}",
                    label_visibility="collapsed",
                    placeholder="Pick SKUs for this pool…",
                )

                if selected:
                    pool_entries = []
                    for sku in selected:
                        pool_entries.extend(sku_to_entries.get(sku, []))

                    if pool_entries:
                        pool_pdf = build_pdf_from_entries(pool_entries,
                                                         source_bytes_map)
                        st.download_button(
                            label=f"⬇ Download ({len(pool_entries)} labels)",
                            data=pool_pdf,
                            file_name=f"{safe_filename(pool_name)}_"
                                      f"{len(pool_entries)}labels.pdf",
                            mime="application/pdf",
                            key=f"pool_dl_{pool_name}",
                            type="primary",
                            use_container_width=True,
                        )
