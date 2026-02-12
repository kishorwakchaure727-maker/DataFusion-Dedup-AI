import streamlit as st
import pandas as pd
import os
import io
from engine import CompanyDedupEngine
from outputs import generate_outputs

st.set_page_config(page_title="DataFusion Dedup AI", page_icon="🏢", layout="wide")

st.title("🏢 DataFusion Dedup AI")
st.markdown("### Enterprise-grade Company Deduplication & Normalization")

st.sidebar.header("Settings")

# File Upload
uploaded_file = st.file_uploader("Upload your Excel or CSV file", type=["xlsx", "csv"])

if uploaded_file:
    # Load data
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)
    
    st.write(f"Loaded {len(df)} rows.")
    
    # Column selection
    string_cols = df.select_dtypes(include=['object']).columns.tolist()
    column = st.sidebar.selectbox("Select Company Name Column", string_cols)
    
    # Thresholds
    hard_thresh = st.sidebar.slider("Hard Threshold (Strict Match)", 0.80, 1.00, 0.90, 0.01)
    soft_thresh = st.sidebar.slider("Soft Threshold (Token Match)", 0.70, 0.95, 0.85, 0.01)
    
    # Options
    web_search = st.sidebar.checkbox("Enable Web Search Verification", value=False, help="Uses DuckDuckGo to verify low-confidence matches. Slower but more accurate.")
    enrichment = st.sidebar.checkbox("Enable Website & Industry Enrichment", value=False, help="Finds company domains and classifies industries.")
    no_subsidiary_fold = st.sidebar.checkbox("Disable Subsidiary Folding", value=False)
    
    # Custom Mappings
    st.sidebar.subheader("Custom Mappings")
    add_map_str = st.sidebar.text_area("Add Mappings (e.g. GE->GENERAL ELECTRIC; P&G->PROCTER & GAMBLE)", "")
    
    add_map = {}
    if add_map_str:
        for pair in add_map_str.split(";"):
            if "->" in pair:
                k, v = pair.split("->")
                add_map[k.strip().upper()] = v.strip().upper()

    if st.button("🚀 Run Dedup"):
        with st.spinner("Processing... This may take a moment."):
            # Setup Engine
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
            
            # Generate outputs (in-memory)
            results = generate_outputs(processed_rows, settings)
            stats = results['stats']
            
            st.success("Deduplication Complete!")
            
            # Display Stats
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Rows", stats['total_rows'])
            col2.metric("Clusters", stats['total_clusters'])
            col3.metric("Multi-record Clusters", stats['multi_record_clusters'])
            col4.metric("High-confidence Review", stats['high_confidence_review_rows'])
            
            # Download Buttons
            st.markdown("### 📥 Download Results")
            
            st.download_button(
                "Download Full Clusters (Final)", 
                data=results['final'], 
                file_name="company_duplicates_final.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
                
            st.download_button(
                "Download Golden Mapping", 
                data=results['golden'], 
                file_name="golden_mapping.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
                
            st.download_button(
                "Download High Confidence Review", 
                data=results['review'], 
                file_name="high_confidence_review.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
            # Preview
            st.markdown("### 🔍 Preview (First 50 rows)")
            st.dataframe(pd.DataFrame(processed_rows).head(50))

else:
    st.info("Please upload a file to start.")
    st.markdown("""
    #### Supported Features:
    - **Normalization**: Standardizes naming conventions.
    - **Legal Suffix Stripping**: Removes LTD, LLC, INC, etc.
    - **Subsidiary Folding**: Identifies 'Brand India' as 'Brand'.
    - **Fuzzy Clustering**: Groups similar names based on similarity scores.
    - **Web Verification**: (Optional) Live lookup for low-confidence clusters.
    """)
