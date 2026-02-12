import os
import pandas as pd
from engine import CompanyDedupEngine
from outputs import generate_outputs

def run_dedup(file_path, column=None, **kwargs):
    print(f"Loading file: {file_path}")
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)

    if not column:
        # Simple auto-detect: first column with string-like data
        column = df.select_dtypes(include=['object']).columns[0]
        print(f"Auto-detected column: {column}")

    engine = CompanyDedupEngine(settings=kwargs)
    processed_rows = engine.process(df, column)
    
    output_dir = os.path.dirname(os.path.abspath(file_path))
    stats = generate_outputs(processed_rows, kwargs, output_dir)
    
    print("\nProcessing Complete!")
    print(f"Total Rows: {stats['total_rows']}")
    print(f"Total Clusters: {stats['total_clusters']}")
    print(f"Multi-record Clusters: {stats['multi_record_clusters']}")
    print(f"High-confidence Review Rows: {stats['high_confidence_review_rows']}")
    
    return stats

if __name__ == "__main__":
    # Test with a dummy file if needed
    test_data = {
        'Company Name': [
            'IBM India Pvt Ltd',
            'IBM',
            'TCS',
            'Tata Consultancy Services Limited',
            'Google LLC',
            'Alphabet Inc',
            'Microsoft',
            'Ltd' # Empty case
        ]
    }
    df_test = pd.DataFrame(test_data)
    df_test.to_excel('test_input.xlsx', index=False)
    
    run_dedup('test_input.xlsx', column='Company Name', web_search=True)
