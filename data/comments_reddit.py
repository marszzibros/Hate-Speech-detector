import requests
import json
import datetime as dt
import pandas as pd
import sqlite3
import time

# connect to sql database that stores all the values
conn = sqlite3.connect('./reddit_data.db')

cursor = conn.cursor()

# check to make sure it connects to the database
print(cursor.execute("SELECT * FROM submissions LIMIT 10").fetchall())


# function to create a string type output from a list of keywords we want 
def keywords_filter_str(keywords_filter):
    str_kf = ""
    for k in keywords_filter:
        str_kf = str_kf + k + ","
    str_kf = str_kf[:-1]
    return str_kf

# get data

def get_data(conn):

    query = "SELECT * FROM submissions WHERE num_comments < 250 ORDER BY RANDOM() LIMIT 30;"

    df = pd.read_sql_query(query, conn)

    if 'comment_avail' in df.columns:
        pass
    else:
        conn.execute("ALTER TABLE submissions ADD comment_avail INTEGER;")
        conn.commit()

    return df


def get_batchids (df):

    batch = []
    total_comments = 0
    i = 0

    while total_comments < 230:

        if df.iloc[i]['num_comments'] < 230:

            batch.append(df.iloc[i]['id'])
            total_comments += df.iloc[i]['num_comments']

            i += 1
        
        else:

            i += 1
    total_comments -= int(df[df['id'] == batch[-1]]['num_comments'])
    return batch[:-1]

def extract_comments (comments_endpoint, size, comments_keywords_filter, batch, conn):

    time.sleep(2)

    try:
        comments = requests.get(f"{comments_endpoint}?q=*&fields={keywords_filter_str(comments_keywords_filter)}&sort=asc&link_id={keywords_filter_str(batch)}&size={size}")

        if comments.status_code == 200:

            comments_df = pd.DataFrame(json.loads(comments.text)['data'])
            comments_df = comments_df.set_index('id')
            comments_df.to_sql('comments', conn, if_exists='append') # push the data to sql database
        
            for id in batch:
                    query = f'''UPDATE submissions SET comment_avail = 1 WHERE id = "{id}"; '''
                    conn.execute(query)
                    conn.commit()
        
        else:

            error_df = pd.DataFrame([{'code': comments.status_code, 'batch': str(batch)}])
            error_df.to_sql('errors_comments', conn, if_exists='append')
        
    except:

        error_df = pd.DataFrame([{'code': -999, 'batch': str(batch)}])
        error_df.to_sql('errors_comments', conn, if_exists='append')

def main():

    comments_endpoint = "https://api.pushshift.io/reddit/comment/search/"
    size = 250
    comments_keywords_filter = ['id', 'author', 'body', 'created_utc', 'link_id', 'is_submitter', 'score', 'subreddit', 'subreddit_id']
    
    for _ in range(90000):

        reddit_df = get_data(conn)
        batch = get_batchids(reddit_df)
        extract_comments(comments_endpoint, size, comments_keywords_filter, batch, conn)

if __name__ == "__main__":
    main()