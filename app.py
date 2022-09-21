import sqlite3
import json
import pandas as pd
from constants import db_name, dump_data_file_path

def create_database(db_name):
    conn = sqlite3.connect(db_name, timeout=5) 
    cur = conn.cursor()
    return cur, conn

def extract_dump_data(dump_data_file_path, cur):
    try:
        with open(dump_data_file_path, 'r') as f:
            query=f.read()
        queries=query.split(";")
        for q in queries:
            cur.execute(q)
    except Exception as e:
        print(e)

def process_data(cur):
    cur.execute('''
          SELECT
          * FROM MY_TABLE
          ''')
    df = pd.DataFrame(cur.fetchall(), columns=['id', 'timestamp', 'name', 'data'])
    df.data=df.data.apply(lambda x:json.loads(x))
    data=pd.DataFrame.from_records(df.data.values)
    data.timestamp=pd.to_datetime(data.timestamp)
    data['date']=data.timestamp.dt.date
    data['review_value_speed']=data['review_value_speed'].astype('float')
    data['review_value_print_quality']=data['review_value_print_quality'].astype('float')
    groups=data.groupby(by=['hub_id', 'date']).groups
    avg_rating_df=get_average_rating(groups, data)
    acceptance_ratio_df=get_acceptance_ratio(groups, data)
    supplier_score_metrics_df=pd.concat([avg_rating_df, acceptance_ratio_df])
    supplier_score_metrics_df.sort_values(by=['calculated_at', 'supplier_id'], inplace=True)
    supplier_score_metrics_df.to_csv("supplier_score_metrics_df.csv", index=None)
    supplier_score_metrics_df=supplier_score_metrics_df.reindex(columns=['calculated_at', 'supplier_id', 'metric', 'value'])
    return supplier_score_metrics_df


def get_average_rating(groups, data):
    average_rating_data=pd.DataFrame()
    for group in groups:
        average_rating=0
        hub_day_data=data.iloc[groups[group]]
        review_events=hub_day_data[hub_day_data.event.str.contains('review')][['review_value_speed', 'review_value_print_quality']]
        if review_events.shape[0]>0:
            review_events.fillna(0, inplace=True)
            average_rating=sum(review_events.mean())/2
        average_rating_data=average_rating_data.append({"calculated_at":group[1], "supplier_id":group[0], "metric":"average_rating", "value":int(average_rating)}, ignore_index=True)
    return average_rating_data

def get_acceptance_ratio(groups, data):
    acceptance_ratio_data=pd.DataFrame()
    for group in groups:
        hub_day_data=data.iloc[groups[group]]
        accepted_orders=hub_day_data[hub_day_data.event.str.contains('payment')]['order_id'].unique()
        total_orders=hub_day_data['order_id'].unique()
        acceptance_ratio=(len(accepted_orders)/len(total_orders))*100
        acceptance_ratio_data=acceptance_ratio_data.append({"calculated_at":group[1], "supplier_id":group[0], "metric":"acceptance_ratio", "value":int(acceptance_ratio)}, ignore_index=True)
    return acceptance_ratio_data

def load_processed_data_to_db(conn, processed_data):
    processed_data.to_sql('supplier_score_metrics', conn, if_exists='replace', index=False)

def main():
    cur, conn = create_database(db_name)
    extract_dump_data(dump_data_file_path, cur)
    processed_data=process_data(cur)
    load_processed_data_to_db(conn, processed_data)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
