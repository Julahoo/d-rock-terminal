import pandas as pd
import numpy as np

def generate_pareto_curve(fin_df):
    """
    Calculates the cumulative distribution of GGR across the player base to visualize 
    the 80/20 Pareto principle (Whale Concentration).
    """
    if fin_df.empty:
        return pd.DataFrame()

    # 1. Group by Player ID and sum Lifetime_GGR
    player_ggr = fin_df.groupby('id')['revenue'].sum().reset_index()
    player_ggr.columns = ['id', 'Lifetime_GGR']

    # 2. Filter out negative or zero GGR to ensure a clean revenue curve
    player_ggr = player_ggr[player_ggr['Lifetime_GGR'] > 0].copy()
    
    if player_ggr.empty:
        return pd.DataFrame()

    # 3. Sort players descending by Lifetime_GGR
    player_ggr = player_ggr.sort_values(by='Lifetime_GGR', ascending=False).reset_index(drop=True)

    # 4. Calculate cumulative percentages
    total_ggr = player_ggr['Lifetime_GGR'].sum()
    total_players = len(player_ggr)

    player_ggr['cumulative_ggr'] = player_ggr['Lifetime_GGR'].cumsum()
    player_ggr['cumulative_ggr_pct'] = (player_ggr['cumulative_ggr'] / total_ggr) * 100
    
    # +1 because index is 0-based, we want player count
    player_ggr['player_rank'] = player_ggr.index + 1
    player_ggr['cumulative_players_pct'] = (player_ggr['player_rank'] / total_players) * 100

    return player_ggr

def generate_ltv_curves(fin_df):
    """
    Calculates the Cumulative Lifetime Value (LTV) progression of monthly cohorts.
    """
    if fin_df.empty:
        return pd.DataFrame()
        
    df = fin_df.copy()
    
    # 1. Identify the cohort_month (first report_month a player appears in the dataset)
    first_months = df.groupby('id')['report_month'].min().reset_index()
    first_months.columns = ['id', 'cohort_month']
    
    df = pd.merge(df, first_months, on='id', how='left')

    # Calculate difference in months for month_index
    def diff_month(d1_str, d2_str):
        try:
            d1 = pd.to_datetime(d1_str)
            d2 = pd.to_datetime(d2_str)
            return (d1.year - d2.year) * 12 + d1.month - d2.month
        except:
            return 0
            
    # 2. Calculate the month_index (months since their cohort_month)
    df['month_index'] = df.apply(lambda row: diff_month(row['report_month'], row['cohort_month']), axis=1)
    
    # Keep only valid non-negative indices
    df = df[df['month_index'] >= 0]
    
    # 3. Group by cohort_month and month_index, summing the GGR (revenue)
    cohort_data = df.groupby(['cohort_month', 'month_index'])['revenue'].sum().reset_index()
    cohort_data.rename(columns={'revenue': 'GGR'}, inplace=True)
    
    # 4. Calculate the cumulative sum of GGR over the month_index for each cohort
    cohort_data['Cumulative_GGR'] = cohort_data.groupby('cohort_month')['GGR'].cumsum()
    
    return cohort_data
