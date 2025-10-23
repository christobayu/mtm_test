import pandas as pd
import json
import warnings

def get_playing_time(row):
    try:
        data = json.loads(row['event_data'])
        player_id = data.get('player_id')
        time_spent = 0.0

        if row['event_type'] == 'polar_hero_match_finish_event':
            hero_data = data.get('hero_data')
            if isinstance(hero_data, list) and len(hero_data) > 0:
                time_spent = hero_data[0].get('time_spent_in_match', 0)
                if time_spent is None:
                    time_spent = 0.0

        return pd.Series([player_id, time_spent])
    except:
        return pd.Series([None, 0.0])

def extract_player_id(event_data_str):
    try:
        data = json.loads(event_data_str)
        return data.get('player_id')
    except:
        return None


partners_df = pd.read_csv("attribution-partners.csv")
partners_df = partners_df[['player_id', 'attribution_partner']].drop_duplicates()
###########################################################################################################################
player_acquisition_dates = {}
chunksize = 1000
event_file_path = "event-data.csv"


for chunk in pd.read_csv(event_file_path, chunksize=chunksize, usecols=['event_timestamp', 'event_data']):
    chunk['timestamp'] = pd.to_datetime(chunk['event_timestamp'], unit='s')
    chunk['player_id'] = chunk['event_data'].apply(extract_player_id)
    chunk = chunk.dropna(subset=['player_id', 'timestamp'])

    chunk_min_ts = chunk.groupby('player_id')['timestamp'].min()
    for pid, ts in chunk_min_ts.items():
        if pid not in player_acquisition_dates or ts < player_acquisition_dates[pid]:
            player_acquisition_dates[pid] = ts

acq_df = pd.DataFrame(player_acquisition_dates.items(), columns=['player_id', 'acq_timestamp'])
acq_df['acq_date'] = pd.to_datetime(acq_df['acq_timestamp'].dt.date)

acq_df = acq_df.merge(partners_df, on='player_id', how='left')
acq_df['attribution_partner'] = acq_df['attribution_partner'].fillna('organic')

cohort_sizes = acq_df.groupby(['acq_date', 'attribution_partner']).size().to_frame(name='cohort_size').reset_index()
acq_df = acq_df[['player_id', 'acq_date', 'attribution_partner']]


###################################################################################################
##
daily_stats_chunks = []
cols_to_use = ['event_type', 'event_timestamp', 'event_data']
for chunk in pd.read_csv(event_file_path, chunksize=chunksize, usecols=cols_to_use):

    parsed_data = chunk.apply(get_playing_time, axis=1)
    parsed_data.columns = ['player_id', 'time_spent']


    chunk['date'] = pd.to_datetime(chunk['event_timestamp'], unit='s').dt.date
    chunk = pd.concat([chunk[['date']], parsed_data], axis=1)

    chunk = chunk.dropna(subset=['player_id'])
    chunk_stats = chunk.groupby(['player_id', 'date']).agg(total_time_spent=('time_spent', 'sum')).reset_index()
    daily_stats_chunks.append(chunk_stats)

all_daily_stats = pd.concat(daily_stats_chunks)
final_daily_stats = all_daily_stats.groupby(['player_id', 'date']).agg(total_time_spent=('total_time_spent', 'sum')).reset_index()
final_daily_stats['date'] = pd.to_datetime(final_daily_stats['date'])


###################################################################################################
##
data = final_daily_stats.merge(acq_df, on='player_id')
data['days_since_acq'] = (data['date'] - data['acq_date']).dt.days

retained_users = data[data['days_since_acq'].isin([1, 3, 7])]
retained_counts = retained_users.groupby(['acq_date', 'attribution_partner', 'days_since_acq'])['player_id'].nunique().to_frame(name='retained_count').reset_index()

retention_data = retained_counts.merge(cohort_sizes, on=['acq_date', 'attribution_partner'])
retention_data['retention_rate'] = retention_data['retained_count'] / retention_data['cohort_size']

retention_pivot = retention_data.pivot_table(
    index=['acq_date', 'attribution_partner'],
    columns='days_since_acq',
    values='retention_rate'
).reset_index()

retention_pivot = retention_pivot.rename(columns={1: 'D1_Retention', 3: 'D3_Retention', 7: 'D7_Retention', 'acq_date': 'date'})

retention_chart_data = retention_pivot.melt(
    id_vars=['date', 'attribution_partner'],
    value_vars=['D1_Retention', 'D3_Retention', 'D7_Retention'],
    var_name='retention_day',
    value_name='retention_rate'
)
retention_chart_data = retention_chart_data.dropna()
daily_avg_time = data.groupby(['date', 'attribution_partner']).agg(total_time_played_seconds=('total_time_spent', 'sum'),active_players=('player_id', 'nunique')).reset_index()

daily_avg_time['avg_time_per_player_minutes'] = (daily_avg_time['total_time_played_seconds'] / daily_avg_time['active_players']) / 60

avg_time_chart_data = daily_avg_time[['date', 'attribution_partner', 'avg_time_per_player_minutes']]


dashboard_data = pd.merge(retention_pivot, avg_time_chart_data, on=['date', 'attribution_partner'], how='outer')
dashboard_data.to_csv('dashboard_data.csv', index=False)
