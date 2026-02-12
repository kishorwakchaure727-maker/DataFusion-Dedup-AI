import streamlit as st
import pandas as pd
import os
import io
from engine import CompanyDedupEngine
from outputs import generate_outputs

st.set_page_config(page_title="DataFusion Dedup AI", page_icon="🏢", layout="wide")

st.title("🏢 DataFusion Dedup AI")
st.markdown("### Enterprise-grade Company Deduplication & Data Enrichment")

# Sidebar Configuration
st.sidebar.header("Settings")
st.sidebar.caption("Last Sync: Feb 12, 10:35 PM")

hard_thresh = st.sidebar.slider("Hard Threshold (Strict Match)", 0.00, 1.00, 0.90, 0.01)
soft_thresh = st.sidebar.slider("Soft Threshold (Token Match)", 0.00, 1.00, 0.85, 0.01)

web_search = st.sidebar.checkbox("Enable Web Search Verification", value=False, help="Uses DuckDuckGo to verify low-confidence matches. Slower but more accurate.")
enrichment = st.sidebar.checkbox("Enable Website & Industry Enrichment", value=False, help="Finds company domains and classifies industries.")
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
tab1, tab2 = st.tabs(["🚀 Deduplicator Tool", "📖 How it Works"])

with tab1:
    uploaded_file = st.file_uploader("Upload your Excel or CSV file", type=["xlsx", "csv"])

    if uploaded_file:
        if uploaded_file.name.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        
        st.write(f"Loaded {len(df)} rows.")
        
        string_cols = df.select_dtypes(include=['object']).columns.tolist()
        if string_cols:
            column = st.selectbox("Select Company Name Column", string_cols)

            if st.button("🚀 Run Dedup"):
                with st.spinner("Processing... This may take a moment."):
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
                    
                    st.success("Deduplication Complete!")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Total Rows", stats['total_rows'])
                    col2.metric("Clusters", stats['total_clusters'])
                    col3.metric("Multi-record Clusters", stats['multi_record_clusters'])
                    col4.metric("High-confidence Review", stats['high_confidence_review_rows'])
                    
                    st.markdown("### 📥 Download Results")
                    col_dl1, col_dl2, col_dl3 = st.columns(3)
                    
                    with col_dl1:
                        st.download_button("Download Full Clusters", data=results['final'], file_name="company_duplicates_final.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    with col_dl2:
                        st.download_button("Download Golden Mapping", data=results['golden'], file_name="golden_mapping.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    with col_dl3:
                        st.download_button("Download Review File", data=results['review'], file_name="high_confidence_review.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    
                    st.markdown("### 🔍 Preview (First 50 rows)")
                    st.dataframe(pd.DataFrame(processed_rows).head(50))
        else:
            st.error("No text columns found in the uploaded file.")
    else:
        st.info("Please upload a file to start.")

with tab2:
    st.header("App Documentation & User Guide")
    
    st.subheader("1. What is DataFusion Dedup AI?")
    st.write("""
    This app is an enterprise-grade tool designed to clean, normalize, and deduplicate company datasets. 
    It doesn't just look for exact matches; it understands that **'Apple Inc.'**, **'apple'**, and **'Apple India'** are likely the same entity.
    """)
    
    st.subheader("2. How to Use the App")
    st.markdown("""
    1.  **Upload Data**: Use the 'Tool' tab to upload your CSV or Excel file.
    2.  **Select Column**: Choose the column that contains company names.
    3.  **Adjust Settings**: Use the sidebar to tweak sensitivity (see Thresholds below).
    4.  **Enrich (Optional)**: Enable 'Web Search' or 'Enrichment' for deeper data gathering.
    5.  **Run & Download**: Click the button and download your 'Golden Mapping' or full cluster report.
    """)
    
    st.subheader("3. Understanding Thresholds (The AI Settings)")
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Hard Threshold**")
        st.write("""
        This is the strictness dial. It uses **Jaro-Winkler Similarity** (0.0 to 1.0).
        - **0.90 (Default)**: Catches typos (e.g., 'Gogle' vs 'Google').
        - **1.00**: Only exact matches.
        """)
    with col_b:
        st.markdown("**Soft Threshold**")
        st.write("""
        This is used for **Token-Sorted Matches** (same words, different order).
        - **0.85 (Default)**: Catches 'Google India' vs 'India Google'.
        - Relaxing this helps map complex entity names.
        """)
        
    st.subheader("4. Features & Logic")
    with st.expander("Normalization Logic"):
        st.write("""
        - **Text Cleaning**: Converts to uppercase, removes punctuation, collapses spaces.
        - **Legal Suffix Stripping**: Removes 'LTD', 'LLC', 'INC', 'PVT LTD' etc. to find the true 'Base Name'.
        - **Subsidiary Folding**: Removes geographic tokens like 'INDIA', 'USA' from the end of names.
        """)
    
    with st.expander("Data Enrichment"):
        st.write("""
        - **Domain Finder**: Uses AI search to find official domains (e.g., ibm.com).
        - **Industry Classifier**: Categorizes companies into Technology, Finance, etc.
        - **Web Verification**: Performs a live look-up for low-confidence clusters.
        """)

    with st.expander("Output Files Explained"):
        st.write("""
        - **Full Clusters**: Detailed report showing every row grouped by ID.
        - **Golden Mapping**: A simple 2-column map of 'Original Name' -> 'Best Canonical Name'.
        - **Review File**: Automatically flags only high-confidence matches for quick auditing.
        """)
