import pandas as pd

def generate_overlap_stats(fin_df, brand_1="Rojabet", brand_2="Latribet"):
    """
    Calculates the Cross-Brand Cannibalization metrics (overlapping ID count and shared GGR)
    between two specific brands.
    """
    if fin_df.empty:
        return {"overlap_count": 0, "overlap_ggr": 0.0}
        
    # 1. Filter the dataframe for the two target brands
    target_brands = fin_df[fin_df['brand'].isin([brand_1, brand_2])]
    
    if target_brands.empty:
        return {"overlap_count": 0, "overlap_ggr": 0.0}

    # 2. Group by Player id and count the nunique() of brands they have played on
    brand_counts = target_brands.groupby('id')['brand'].nunique().reset_index()
    
    # 3. Identify the cohort of ids where the unique brand count > 1
    overlapping_ids = brand_counts[brand_counts['brand'] > 1]['id']
    
    if overlapping_ids.empty:
        return {"overlap_count": 0, "overlap_ggr": 0.0}
        
    # 4. Filter original financial dataframe down to JUST these overlapping players to get their full Lifetime GGR
    overlap_cohort = fin_df[fin_df['id'].isin(overlapping_ids)]
    
    # 5. Calculate specific metrics requested
    overlap_count = len(overlapping_ids)
    overlap_ggr = overlap_cohort['revenue'].sum() if 'revenue' in overlap_cohort.columns else 0.0
    
    return {
        "overlap_count": overlap_count,
        "overlap_ggr": overlap_ggr
    }
