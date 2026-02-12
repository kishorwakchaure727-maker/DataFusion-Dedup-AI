import pandas as pd
import io

def generate_outputs(processed_rows, settings, output_dir=None):
    df_rows = pd.DataFrame(processed_rows)
    results = {}
    
    # helper to save to buffer or disk
    def save_xlsx(df_dict, filename):
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            for sheet_name, df in df_dict.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        buf.seek(0)
        
        if output_dir:
            with open(f"{output_dir}/{filename}", "wb") as f:
                f.write(buf.getbuffer())
        
        return buf

    # 1. company_duplicates_final.xlsx
    cols = ['row_order', 'original_name', 'normalized_name', 'base_name', 
            'cluster_id', 'cluster_size', 'canonical_name', 'confidence', 'reason']
    
    summary = df_rows.groupby(['cluster_id', 'canonical_name']).size().reset_index(name='count')
    
    settings_data = {
        'Setting': ['hard_threshold', 'soft_threshold', 'suffix_list_size', 'explicit_maps', 'subsidiary_folding', 'preserved_all_rows'],
        'Value': [
            settings.get('hard', 0.90),
            settings.get('soft', 0.85),
            26,
            str(settings.get('add_map', {})),
            not settings.get('no_subsidiary_fold', False),
            True
        ]
    }
    
    final_buf = save_xlsx({
        'clusters': df_rows[cols],
        'canonical_summary': summary,
        'settings': pd.DataFrame(settings_data)
    }, "company_duplicates_final.xlsx")
    results['final'] = final_buf

    # 2. golden_mapping.xlsx
    golden_buf = save_xlsx({'mapping': df_rows[['original_name', 'canonical_name']]}, "golden_mapping.xlsx")
    results['golden'] = golden_buf

    # 3. high_confidence_review.xlsx
    high_conf = df_rows[(df_rows['confidence'] >= 0.95) & (df_rows['cluster_size'] >= 2)]
    high_conf = high_conf.sort_values('row_order')
    review_buf = save_xlsx({'review': high_conf[cols]}, "high_confidence_review.xlsx")
    results['review'] = review_buf

    results['stats'] = {
        'total_rows': len(df_rows),
        'total_clusters': df_rows['cluster_id'].nunique(),
        'multi_record_clusters': (df_rows['cluster_size'] >= 2).sum(),
        'high_confidence_review_rows': len(high_conf)
    }
    
    return results
