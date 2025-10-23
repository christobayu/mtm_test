# Import necessary libraries
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import ipywidgets as widgets
from ipywidgets import interact, Layout

# --- 1. Load and Prepare Data ---
# Load the processed datasets
try:
    partners_df = pd.read_csv('attribution-partners.csv')
    durations_df = pd.read_csv('processed_match_durations.csv')
    activity_df = pd.read_csv('processed_daily_activity.csv')
except FileNotFoundError:
    print("Error: Make sure you have run the 'process_logs.py' script first.")
    # Create empty dataframes to avoid crashing the notebook
    partners_df, durations_df, activity_df = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# Convert date columns to datetime objects
if not activity_df.empty:
    activity_df['date'] = pd.to_datetime(activity_df['date'])
if not durations_df.empty:
    durations_df['date'] = pd.to_datetime(durations_df['date'])

# Merge data into a single master dataframe
if not activity_df.empty and not partners_df.empty:
    master_df = pd.merge(activity_df, partners_df, on='player_id', how='left')
    master_df['attribution_partner'].fillna('organic', inplace=True) # Assume players with no partner are organic
else:
    master_df = pd.DataFrame()

# --- 2. Calculate Metrics ---

# a) Calculate Cohorts for Retention
if not master_df.empty:
    # Find the acquisition date for each player
    cohort_df = master_df.groupby('player_id')['date'].min().reset_index()
    cohort_df.rename(columns={'date': 'cohort_date'}, inplace=True)

    # Merge cohort date back into the main activity log
    retention_df = pd.merge(master_df, cohort_df, on='player_id', how='left')
    retention_df['cohort_date'] = retention_df['cohort_date'].dt.date
    retention_df['activity_date'] = retention_df['date'].dt.date

    # Calculate the age of the user at the time of activity
    retention_df['day_number'] = (retention_df['activity_date'] - retention_df['cohort_date']).dt.days

# b) Prepare playtime data
if not durations_df.empty and not partners_df.empty:
    playtime_df = pd.merge(durations_df, partners_df, on='player_id', how='left')
    playtime_df['attribution_partner'].fillna('organic', inplace=True)
else:
    playtime_df = pd.DataFrame()

# --- 3. Build Interactive Dashboard ---

# Create widgets
partner_list = ['All'] + partners_df['attribution_partner'].unique().tolist()
partner_dropdown = widgets.Dropdown(options=partner_list, value='All', description='Partner:')

# Create a container for the plots
fig_container = go.FigureWidget(make_subplots(rows=2, cols=1, subplot_titles=("Daily User Retention (D1, D3, D7)", "Daily Average Playtime per Player (minutes)")))

def update_dashboard(partner):
    # Filter data based on selection
    filtered_retention = retention_df.copy()
    filtered_playtime = playtime_df.copy()
    
    if partner != 'All':
        filtered_retention = filtered_retention[filtered_retention['attribution_partner'] == partner]
        filtered_playtime = filtered_playtime[filtered_playtime['attribution_partner'] == partner]

    # --- Calculate Retention Metric ---
    cohort_sizes = filtered_retention.groupby('cohort_date')['player_id'].nunique()

    # Calculate actives on D1, D3, D7
    d1_actives = filtered_retention[filtered_retention['day_number'] == 1].groupby('cohort_date')['player_id'].nunique()
    d3_actives = filtered_retention[filtered_retention['day_number'] == 3].groupby('cohort_date')['player_id'].nunique()
    d7_actives = filtered_retention[filtered_retention['day_number'] == 7].groupby('cohort_date')['player_id'].nunique()

    # Calculate retention rates
    retention_rates = pd.DataFrame(cohort_sizes).rename(columns={'player_id': 'cohort_size'})
    retention_rates['D1'] = (d1_actives / cohort_sizes) * 100
    retention_rates['D3'] = (d3_actives / cohort_sizes) * 100
    retention_rates['D7'] = (d7_actives / cohort_sizes) * 100
    retention_rates = retention_rates.fillna(0)

    # --- Calculate Playtime Metric ---
    daily_playtime = filtered_playtime.groupby('date').agg(
        total_duration_seconds=('duration_seconds', 'sum'),
        player_count=('player_id', 'nunique')
    ).reset_index()

    daily_playtime['avg_playtime_minutes'] = (daily_playtime['total_duration_seconds'] / daily_playtime['player_count']) / 60

    # --- Update Plots ---
    fig_container.batch_update(True) # Use batch update for smoother rendering
    fig_container.data = [] # Clear previous traces

    # Retention Plot
    fig_container.add_trace(go.Scatter(x=retention_rates.index, y=retention_rates['D1'], name='D1 Retention', mode='lines+markers'), row=1, col=1)
    fig_container.add_trace(go.Scatter(x=retention_rates.index, y=retention_rates['D3'], name='D3 Retention', mode='lines+markers'), row=1, col=1)
    fig_container.add_trace(go.Scatter(x=retention_rates.index, y=retention_rates['D7'], name='D7 Retention', mode='lines+markers'), row=1, col=1)

    # Playtime Plot
    fig_container.add_trace(go.Bar(x=daily_playtime['date'], y=daily_playtime['avg_playtime_minutes'], name='Avg Playtime'), row=2, col=1)

    # Update layout
    fig_container.update_yaxes(title_text="Retention Rate (%)", row=1, col=1)
    fig_container.update_yaxes(title_text="Avg Minutes / Player", row=2, col=1)
    fig_container.update_layout(height=700, title_text=f"Performance Dashboard for: {partner}")
    fig_container.batch_update(False)


# Display the dashboard
if 'retention_df' in locals() and not retention_df.empty:
    display(widgets.VBox([partner_dropdown, fig_container]))
    interact(update_dashboard, partner=partner_dropdown)
else:
    print("Could not generate dashboard due to missing data.")
