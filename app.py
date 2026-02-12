import streamlit as st
import pandas as pd
import os
import io
from engine import CompanyDedupEngine
from outputs import generate_outputs

st.set_page_config(page_title="DataFusion Dedup AI", page_icon="🏢", layout="wide")

# Side-bar Branding (Subtle icon)
st.logo("logo.svg", icon_image="logo.svg")

# Main Header with Logo
col_logo, col_title = st.columns([0.1, 0.9])
with col_logo:
    st.image("logo.svg", width=80)
with col_title:
    st.title("DataFusion Dedup AI")
    st.markdown("##### Enterprise-grade Company Deduplication & Data Enrichment")

# Sidebar Configuration
st.sidebar.header("Settings")
st.sidebar.caption("Last Sync: Feb 12, 11:59 PM")
st.sidebar.success("🚀 Version 2.1 (Enhanced UI + Batch Processing)")

hard_thresh = st.sidebar.slider("Hard Threshold (Strict Match)", 0.00, 1.00, 0.90, 0.01)
soft_thresh = st.sidebar.slider("Soft Threshold (Token Match)", 0.00, 1.00, 0.85, 0.01)

web_search = st.sidebar.checkbox("Enable Web Search Verification", value=False, help="Uses DuckDuckGo to verify low-confidence matches. Parallelized for speed.")
enrichment = st.sidebar.checkbox("Enable Website & Industry Enrichment", value=False, help="Finds company domains and classifies industries. Parallelized for speed.")
no_subsidiary_fold = st.sidebar.checkbox("Disable Subsidiary Folding", value=False)

st.sidebar.subheader("Custom Mappings")
add_map_str = st.sidebar.text_area("Add Mappings (e.g. GE->GENERAL ELECTRIC; P&G->PROCTER & GAMBLE)", "")
add_map = {}
if add_map_str:
    for pair in add_map_str.split(";"):
        if "->" in pair:
            split_pair = pair.split("->")
            if len(split_pair) == 2:
                k, v = split_pair
                add_map[k.strip().upper()] = v.strip().upper()

# Main Tabs
tab1, tab2 = st.tabs(["🚀 Multitasking Tool", "📖 How it Works"])

with tab1:
    uploaded_files = st.file_uploader("Upload Excel or CSV files (Multiple files allowed)", type=["xlsx", "csv"], accept_multiple_files=True)

    if uploaded_files:
        st.write(f"Files in Queue: {len(uploaded_files)}")
        
        # We need a primary column name for consistency or we ask per file. 
        # For simple multitasking, we'll ask for one column name that should exist in all files or be selected.
        all_cols = []
        for f in uploaded_files:
            if f.name.endswith(".csv"):
                temp_df = pd.read_csv(f, nrows=1)
            else:
                temp_df = pd.read_excel(f, nrows=1)
            all_cols.extend(temp_df.columns.tolist())
        
        unique_cols = sorted(list(set(all_cols)))
        column = st.selectbox("Select Company Name Column (Should exist in files)", unique_cols)

        if st.button("🚀 Process All Tasks"):
            main_progress = st.progress(0)
            status_text = st.empty()
            
            for i, uploaded_file in enumerate(uploaded_files):
                filename = uploaded_file.name
                status_text.markdown(f"**Task {i+1}/{len(uploaded_files)}**: Processing `{filename}`...")
                
                # Load data
                if filename.endswith(".csv"):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                if column not in df.columns:
                    st.warning(f"Skipping `{filename}`: Column `{column}` not found.")
                    continue

                # Process
                settings = {
                    'hard': hard_thresh,
                    'soft': soft_thresh,
                    'no_subsidiary_fold': no_subsidiary_fold,
                    'web_search': web_search,
                    'enrichment': enrichment,
                    'add_map': add_map
                }
                
                engine = CompanyDedupEngine(settings=settings)
                processed_rows = engine.process(df, column)
                
                results = generate_outputs(processed_rows, settings)
                stats = results['stats']
                
                # Success for this file
                with st.expander(f"✅ Completed: {filename}", expanded= (len(uploaded_files) == 1)):
                    st.success(f"Processed {stats['total_rows']} rows into {stats['total_clusters']} clusters.")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.download_button(f"Download Full Clusters ({filename})", data=results['final'], file_name=f"dedup_final_{filename}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"dl_final_{i}")
                    with col2:
                        st.download_button(f"Download Golden Mapping ({filename})", data=results['golden'], file_name=f"golden_{filename}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"dl_golden_{i}")
                    with col3:
                        st.download_button(f"Download Review File ({filename})", data=results['review'], file_name=f"review_{filename}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"dl_review_{i}")
                    
                    st.dataframe(pd.DataFrame(processed_rows).head(10))

                main_progress.progress((i + 1) / len(uploaded_files))
            
            status_text.success("🎉 All multitasking jobs completed!")
    else:
        st.info("Upload one or more files to begin batch processing.")

with tab2:
    st.header("App Documentation & Multitasking Guide")
    
    st.subheader("🚀 New: Parallel Multitasking")
    st.write("""
    This version of DataFusion Dedup AI is supercharged with **Parallel Multitasking**:
    - **Multithreaded Search**: Web enrichment now runs on multiple threads, making it 5-10x faster.
    - **Batch Processing**: You can now upload multiple files at once. The app will queue them and provide separate downloads for each.
    """)

    st.subheader("1. What is DataFusion Dedup AI?")
    st.write("""
    This app is an enterprise-grade tool designed to clean, normalize, and deduplicate company datasets. 
    It doesn't just look for exact matches; it understands that **'Apple Inc.'**, **'apple'**, and **'Apple India'** are likely the same entity.
    """)
    
    st.subheader("2. How to Use Multitasking")
    st.markdown("""
    1.  **Select Multiple Files**: Click 'Browse files' and select multiple CSVs or Excels (use Ctrl or Shift).
    2.  **Harmonize Columns**: Ensure the company name column exists in all files (or at least the ones you want to process).
    3.  **Run Batch**: Click 'Process All Tasks'. The app will cycle through each file and provide a download summary for each.
    """)
    
    st.subheader("3. Understanding Thresholds")
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Hard Threshold**")
        st.write("""
        The strictness dial (0.0 to 1.0).
        - **0.90 (Default)**: Balanced accuracy.
        """)
    with col_b:
        st.markdown("**Soft Threshold**")
        st.write("""
        Used for word-shuffled matches.
        - **0.85 (Default)**: Catches 'IBM India' vs 'India IBM'.
        """)
