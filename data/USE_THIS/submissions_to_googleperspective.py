from googleapiclient import discovery
import re
import time
import sys
import sqlite3
import pandas as pd
from multiprocessing import Pool, cpu_count

def clean_data(df):

    # regex to filter out the url
    url_filter = r'[(http(s)?):\/\/(www\.)?a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)'

    # remove urls
    for i in range(len(df)):
        stringliteral = df['body'][i]
        df['body'][i] = re.sub(url_filter, '', stringliteral)

    # only select those that have letters
    df = df[df["body"].str.contains(r'[a-zA-Z]')].reset_index(drop = False)

    return df

def load_and_format(path):

    # connect to sql database
    conn = sqlite3.connect(path)

    # load a data sample
    query = "SELECT * FROM comments WHERE body is NOT '' AND body is NOT NULL"

    reddit_df = pd.read_sql_query(query, conn)
    reddit_df = reddit_df[reddit_df['body'].notna()].reset_index(drop=False)

    # check if Toxicity is part of the schema
    if "Toxicity" in reddit_df.columns:
        pass
    else:
        # if not, add all the relevant values
        conn.execute("ALTER TABLE comments ADD Toxicity real;")
        conn.execute("ALTER TABLE comments ADD Insult real;")
        conn.execute("ALTER TABLE comments ADD Severe_Toxicity real;")
        conn.execute("ALTER TABLE comments ADD Identity_Attack real;")
        conn.execute("ALTER TABLE comments ADD Profanity real;")

        conn.commit()
    
    return conn

def parse_googlePerspective(path, client, sample):

    conn = sqlite3.connect(path)

    #print(sample)

    sample_text = sample['body']

    # check if the submissions text is within limit for the API

    # if it is larger, then break it down
    if len(sample_text.encode('utf-8')) > 20480:
        try:
            # break it in two:

            # set two headers
            response_header_1 = {
    'comment': { 'text': sample_text[:20000]},
    'requestedAttributes': {'TOXICITY': {}, 'INSULT':{}, 'THREAT':{}, 'SEVERE_TOXICITY': {}, 'IDENTITY_ATTACK': {}, 'PROFANITY': {}}
    }       
            response_header_2 = {
    'comment': { 'text': sample_text[20000:]},
    'requestedAttributes': {'TOXICITY': {}, 'INSULT':{}, 'THREAT':{}, 'SEVERE_TOXICITY': {}, 'IDENTITY_ATTACK': {}, 'PROFANITY': {}}
    }
            
            # get two responses

            response_1 = client.comments().analyze(body=response_header_1).execute()
            response_2 = client.comments().analyze(body=response_header_2).execute()
            
            # add their average response
            sample['Toxicity'] = (response_1['attributeScores']['TOXICITY']['spanScores'][0]['score']['value']
                                    + response_2['attributeScores']['TOXICITY']['spanScores'][0]['score']['value'])/2
            sample['Insult'] = (response_1['attributeScores']['INSULT']['spanScores'][0]['score']['value']/2
                                    + response_2['attributeScores']['INSULT']['spanScores'][0]['score']['value'])
            sample['Severe_Toxicity'] = (response_1['attributeScores']['SEVERE_TOXICITY']['spanScores'][0]['score']['value']
                                    + response_2['attributeScores']['SEVERE_TOXICITY']['spanScores'][0]['score']['value'])/2
            sample['Identity_Attack'] = (response_1['attributeScores']['IDENTITY_ATTACK']['spanScores'][0]['score']['value']
                                    + response_2['attributeScores']['IDENTITY_ATTACK']['spanScores'][0]['score']['value'])/2
            sample['Profanity'] = (response_1['attributeScores']['PROFANITY']['spanScores'][0]['score']['value']
                                    + response_2['attributeScores']['PROFANITY']['spanScores'][0]['score']['value'])/2
            
            # update values
            cols = ['Toxicity', 'Insult', 'Severe_Toxicity', 'Identity_Attack', 'Profanity']

            for col in cols:
                query = f''' UPDATE comments SET {col} = {sample[col]} WHERE id= '{sample['id']}' '''
                conn.execute(query)
                conn.commit()

            
        except:
            try:
                # store errors

                error_df = pd.DataFrame([{'id': sample['id']}])
                error_df.to_sql('errors_perspective', conn, if_exists='append')
            except:
                print(f"Unmanageable error, Id: {sample['id']}")

    else: # if not, don't break it down
        # handle exceptions
        try:
            # set headers
            request_header = {
        'comment': { 'text': sample_text},
        'requestedAttributes': {'TOXICITY': {}, 'INSULT':{}, 'THREAT':{}, 'SEVERE_TOXICITY': {}, 'IDENTITY_ATTACK': {}, 'PROFANITY': {}}
        }
            # get response
            response = client.comments().analyze(body=request_header).execute()

            # adjust values
            sample['Toxicity'] = response['attributeScores']['TOXICITY']['spanScores'][0]['score']['value']
            sample['Insult'] = response['attributeScores']['INSULT']['spanScores'][0]['score']['value']
            sample['Severe_Toxicity'] = response['attributeScores']['SEVERE_TOXICITY']['spanScores'][0]['score']['value']
            sample['Identity_Attack'] = response['attributeScores']['IDENTITY_ATTACK']['spanScores'][0]['score']['value']
            sample['Profanity'] = response['attributeScores']['PROFANITY']['spanScores'][0]['score']['value']

            # update values
            cols = ['Toxicity', 'Insult', 'Severe_Toxicity', 'Identity_Attack', 'Profanity']

            for col in cols:
                query = f''' UPDATE comments SET {col} = {sample[col]} WHERE id= '{sample['id']}' '''
                conn.execute(query)
                conn.commit()

        except:
            # store errors

            error_df = pd.DataFrame([{'id': sample['id']}])
            error_df.to_sql('errors_perspective', conn, if_exists='append')
def main():
    # load and format the data

    path = './reddit_data_comments.db'
    conn = load_and_format(path)

    # parameters for Google Perspective
    api_key = "AIzaSyBBhycm2m3xZTh95Tms50xaYUgXQ0_SoWM"
    client = discovery.build(
    "commentanalyzer",
    "v1alpha1",
    developerKey=api_key,
    discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
    static_discovery=False,
    )
    query = "SELECT * FROM comments WHERE body is NOT '' AND body is not NULL AND Toxicity is NULL;"
    reddit_df = pd.read_sql_query(query, conn)

    print(reddit_df[0:10])
    # run multiple times
    for _ in range(10):
        # select batches where Toxicity is not available and Self Text is

        
        # clean it
        reddit_clean = clean_data(reddit_df[0:10])

        # send batch requests by multiprocessing
        with Pool(processes=cpu_count()) as pl:
            pl.starmap(parse_googlePerspective, [(path, client, reddit_clean.loc[i]) for i in range(len(reddit_clean))])
        
        time.sleep(1)

if __name__ == "__main__":
    main()
