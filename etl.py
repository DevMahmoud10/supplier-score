import sqlite3
import json
import pandas as pd
from constants import db_name, dump_data_file_path

def create_database(db_name):
    """
    creating sqlite database if not exist or connecting to existed one

    Args:
        db_name (str): database name and path if it's located in subfolder

    Returns:
        cur, conn: db cursor, opened connection engine
    """    
    conn = sqlite3.connect(db_name, timeout=5) 
    cur = conn.cursor()
    return cur, conn

def extract_dump_data(dump_data_file_path, cur):
    """
    read dump data from .sql file then run queries inside it, if it's already created then return without duplicating data

    Args:
        dump_data_file_path (str): path of .sql file
        cur (cursor):  connection cursor
    """    
    try:
        with open(dump_data_file_path, 'r') as f:
            query=f.read()
        queries=query.split(";")
        for q in queries:
            cur.execute(q)
    except Exception as e:
        print(e)

def process_data(cur):
    """
    the core of pipeline by getting events data group them and call calculations methods, after that do some sorting and renaming

    Args:
        cur (cursor): connection cursor

    Returns:
        pandas.DataFrame: supplier_score_metrics final data ready to be loaded in db
    """    
    cur.execute('''SELECT * FROM MY_TABLE ''')
    raw_data = pd.DataFrame(cur.fetchall(), columns=['id', 'timestamp', 'name', 'data'])
    raw_data.data=raw_data.data.apply(lambda x:json.loads(x))
    raw_events=pd.DataFrame.from_records(raw_data.data.values)
    data=preprocess_data(raw_events)
    groups=data.groupby(by=['hub_id', 'date']).groups
    avg_rating_df=get_average_rating(groups, data)
    acceptance_ratio_df=get_acceptance_ratio(groups, data)
    supplier_score_metrics_df=pd.concat([avg_rating_df, acceptance_ratio_df])
    supplier_score_metrics_df.sort_values(by=['calculated_at', 'supplier_id'], inplace=True)
    supplier_score_metrics_df=supplier_score_metrics_df.reindex(columns=['calculated_at', 'supplier_id', 'metric', 'value'])
    return supplier_score_metrics_df

def preprocess_data(data):
    """
    do some preprocessing casting required for targeted columns

    Args:
        data (pandas.DataFrame): raw data

    Returns:
        pandas.DataFrame: processed data
    """    
    data.timestamp=pd.to_datetime(data.timestamp)
    data['date']=data.timestamp.dt.date
    data['review_value_speed']=data['review_value_speed'].astype('float')
    data['review_value_print_quality']=data['review_value_print_quality'].astype('float')
    return data

def get_average_rating(groups, data):
    """
    calculating average rating for every supplier in each particular day

    Args:
        groups (tuple): pair of (hub_id, date)
        data (pandas.DataFrame): events data

    Returns:
        pandas.DataFrame: metric calculated data grouped by date and supplier_id
    """    
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
    """
    calculating orders acceptance ratio for every supplier in each particular day

    Args:
        groups (tuple): pair of (hub_id, date)
        data (pandas.DataFrame): events data

    Returns:
        pandas.DataFrame: metric calculated data grouped by date and supplier_id
    """    
    acceptance_ratio_data=pd.DataFrame()
    for group in groups:
        hub_day_data=data.iloc[groups[group]]
        accepted_orders=hub_day_data[hub_day_data.event.str.contains('payment')]['order_id'].unique()
        total_orders=hub_day_data['order_id'].unique()
        acceptance_ratio=(len(accepted_orders)/len(total_orders))*100
        acceptance_ratio_data=acceptance_ratio_data.append({"calculated_at":group[1], "supplier_id":group[0], "metric":"acceptance_ratio", "value":int(acceptance_ratio)}, ignore_index=True)
    return acceptance_ratio_data

def load_processed_data_to_db(conn, processed_data):
    """
        load final calculated data to supplier_score_metrics table in DB and replace it if they already exists to avoid duplications 
    Args:
        conn (connection): db connection engine
        processed_data (pandas.DataFrame): supplier_score_metrics final data ready to be loaded in db
    """    
    processed_data.to_sql('supplier_score_metrics', conn, if_exists='replace', index=False)

def main():
    """main ETL pipeline method"""    
    cur, conn = create_database(db_name)
    print(f"connection opened to database {db_name} ...")
    print("ETL Pipeline started")
    extract_dump_data(dump_data_file_path, cur)
    processed_data=process_data(cur)
    load_processed_data_to_db(conn, processed_data)
    print("ETL Pipeline completed")
    conn.commit()
    conn.close()
    print(f"connection closed to database {db_name} ...")


if __name__ == "__main__":
    main()
