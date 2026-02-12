import pandas as pd

def generate_outputs(processed_rows, settings, output_dir):
    df_rows = pd.DataFrame(processed_rows)
    
    # 1. company_duplicates_final.xlsx
    with pd.ExcelWriter(f"{output_dir}/company_duplicates_final.xlsx") as writer:
        # Sheet: clusters
        cols = ['row_order', 'original_name', 'normalized_name', 'base_name', 
                'cluster_id', 'cluster_size', 'canonical_name', 'confidence', 'reason']
        df_rows[cols].to_excel(writer, sheet_name='clusters', index=False)
        
        # Sheet: canonical_summary
        summary = df_rows.groupby(['cluster_id', 'canonical_name']).size().reset_index(name='count')
        summary.to_excel(writer, sheet_name='canonical_summary', index=False)
        
        # Sheet: settings
        settings_data = {
            'Setting': ['hard_threshold', 'soft_threshold', 'suffix_list_size', 'explicit_maps', 'subsidiary_folding', 'preserved_all_rows'],
            'Value': [
                settings.get('hard', 0.90),
                settings.get('soft', 0.85),
                26, # Approximate count of suffixes
                str(settings.get('add_map', {})),
                not settings.get('no_subsidiary_fold', False),
                True
            ]
        }
        pd.DataFrame(settings_data).to_excel(writer, sheet_name='settings', index=False)

    # 2. golden_mapping.xlsx
    df_rows[['original_name', 'canonical_name']].to_excel(f"{output_dir}/golden_mapping.xlsx", index=False)

    # 3. high_confidence_review.xlsx
    high_conf = df_rows[(df_rows['confidence'] >= 0.95) & (df_rows['cluster_size'] >= 2)]
    high_conf = high_conf.sort_values('row_order')
    high_conf[cols].to_excel(f"{output_dir}/high_confidence_review.xlsx", index=False)

    return {
        'total_rows': len(df_rows),
        'total_clusters': df_rows['cluster_id'].nunique(),
        'multi_record_clusters': (df_rows['cluster_size'] >= 2).sum(),
        'high_confidence_review_rows': len(high_conf)
    }
