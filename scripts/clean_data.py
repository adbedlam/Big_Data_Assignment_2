
import pandas as pd
import numpy as np
import swifter
import json
import datetime
from tqdm import  tqdm
import multiprocessing as mp
from bson import json_util
import os
import re


campaigns = pd.read_csv('D:/datasets/assigment/campaigns.csv')
client_first_purchase_date = pd.read_csv('D:/datasets/assigment/client_first_purchase_date.csv')
events = pd.read_csv('D:/datasets/assigment/events.csv')
friends = pd.read_csv('D:/datasets/assigment/friends.csv')
messages = pd.read_csv('D:/datasets/assigment/messages.csv',low_memory=False)

path_sql = '../data/cleaned_data_psql/'

def clean_campaigns(campaigns : pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    
    campaigns['total_count'] = campaigns['total_count'].astype('object')
    campaigns['hour_limit'] = campaigns['hour_limit'].astype('object')
    campaigns['subject_length'] = campaigns['subject_length'].astype('object')
    campaigns['position'] = campaigns['position'].astype('object')
    
    for i, row in campaigns.iterrows():
        if not pd.isna(row['total_count']):
            campaigns.at[i,'total_count'] = int(row['total_count'])
        if not pd.isna(row['hour_limit']):
            campaigns.at[i,'hour_limit'] = int(row['hour_limit'])
        if not pd.isna(row['subject_length']):
            campaigns.at[i,'subject_length'] = int(row['subject_length'])
        if not pd.isna(row['position']):
            campaigns.at[i,'position'] = int(row['position'])
        if pd.isna(row['ab_test']):
            campaigns.at[i,'ab_test'] = False

   
    
    return campaigns

def clean_events(events : pd.core.frame.DataFrame):
    
    events['event_time'] = events['event_time'].astype(str).str.replace(" UTC", "", regex=False)
    events['event_time'] = pd.to_datetime(events['event_time'])
    
    events['price'] = pd.to_numeric(events['price'])
    
    products = pd.DataFrame()
    
    products['product_id'] = events['product_id']
    products['category_id'] = events['category_id']
    
    products['brand'] = events['brand']
    products['category_code'] = events['category_code']

    
    d_brand = {}
    d_type = {}
    
    for i, row in products.iterrows():
        if (row['product_id'], row['category_id']) in d_brand and pd.isna(d_brand[(row['product_id'], row['category_id'])]):
            if pd.notna(row['brand']):
                d_brand[ (row['product_id'], row['category_id'])] = row['brand']
        else:
            d_brand[ (row['product_id'], row['category_id'])] = row['brand']
        
        if (row['product_id'], row['category_id']) in d_type and pd.isna(d_type[ (row['product_id'], row['category_id'])]):
            if pd.notna(row['category_code']):
                d_type[ (row['product_id'], row['category_id'])] = row['category_code']
        else:
            d_type[ (row['product_id'], row['category_id'])] = row['category_code']
        
    rows = []
    
    for k in d_type.keys():
        row = {'product_id':k[0], 'category_id':k[1], 'brand': d_brand[k], 'category_code':d_type[k]}
        rows.append(row)
        
    df = pd.DataFrame(rows)
    
    events = events.drop(columns=['category_code','brand'])

        
    return events, df

def clean_messages(messages : pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
      date_columns = [
        'date', 'sent_at', 'opened_first_time_at', 'opened_last_time_at',
        'clicked_first_time_at', 'clicked_last_time_at', 'unsubscribed_at',
        'hard_bounced_at', 'soft_bounced_at', 'complained_at', 'blocked_at',
        'purchased_at', 'created_at', 'updated_at'
      ]
      
      for col in date_columns:
          if col in messages.columns:
              messages[col] = pd.to_datetime(messages[col], errors='coerce')
              
      boolean_cols = [
        'is_opened', 'is_clicked', 'is_unsubscribed', 'is_hard_bounced',
        'is_soft_bounced', 'is_complained', 'is_blocked', 'is_purchased'
      ]
      
      for col in boolean_cols:
          if col in messages.columns:
              messages[col] = messages[col].map({'t': True, 'f': False})
              
      return messages

def clean_friends(friends : pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    
    friends['friend1'] = pd.to_numeric(friends['friend1'])
    friends['friend2'] = pd.to_numeric(friends['friend2'])
    
    return friends

def clean_client_first_purchase_date(cfp_data : pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    
    cfp_data['first_purchase_date'] = pd.to_datetime(cfp_data['first_purchase_date'])
    
    return cfp_data

clean_campaingns_data = clean_campaigns(campaigns)
clean_events_data, clean_products_data = clean_events(events)
clean_messages_data = clean_messages(messages)
clean_friends_data = clean_friends(friends)
clean_client_first_purchase_date_data = clean_client_first_purchase_date(client_first_purchase_date)

clean_campaingns_data.to_csv(f'{path_sql}/Clean_Campaings.csv', index=False)
clean_events_data.to_csv(f'{path_sql}/Clean_Events.csv', index=False)
clean_products_data.to_csv(f'{path_sql}/Clean_Products.csv', index=False)
clean_messages_data.to_csv(f'{path_sql}/Clean_Messages.csv', index=False)
clean_friends_data.to_csv(f'{path_sql}/Clean_Friends.csv', index=False)
client_first_purchase_date.to_csv(f'{path_sql}/Clean_client_first_purchase_date.csv', index=False)



tqdm.pandas()

def processing_nan(value_in_row):
     if not pd.isna(value_in_row):
            return int(value_in_row)
     else:
         return None

def processing_date(dates_in_row):
    return pd.to_datetime(dates_in_row)

def ab_test_proc(value):
    if value != True:
        return False
    else:
        return True

def preproc_campaings(row):

    mongo_row = {
        'id': row['id'],
        'campaign_type': row['campaign_type'],
        'channel': row['channel'],
        'topic': row['topic'],
        'started_at': processing_date(row['started_at']),
        'finished_at': processing_date(row['finished_at']),
        'total_count': processing_nan(row['total_count']),
        'ab_test': ab_test_proc(row['ab_test']),
        'warmup_mode': row['warmup_mode'],
        'subject' : {
            'length' : processing_nan(row['subject_length']),
            'with_personalization' : row['subject_with_personalization'],
            'with_deadline' : row['subject_with_deadline'],
            'with_emoji': row['subject_with_emoji'],
            'with_bonuses': row['subject_with_bonuses'],
            'with_discount' : row['subject_with_discount'],
            'with_saleout' : row['subject_with_saleout'],
        },
        'is_test': row['is_test'],
        'position': processing_nan(row['position']),
    }    

    
    return mongo_row
    

def preproc_event(row):

    mongo_row = {
        'event_time': processing_date(row['event_time']),
        'event_type': row['event_type'],
        'product':{
            'product_id': int(row['product_id']),
            'category_id': int(row['category_id']),
            'category_code': row['category_code'],
            'brand': row['brand'],
            'price': processing_nan(row['price']),
        },
        'user':{
            'user_id': int(row['user_id']),
            'user_session': row['user_session'],
        }
    }
    
    return mongo_row  

def preproc_friends(row):
    
    mongo_row = {
        'friend1' : int(row['friend1']),
        'friend2' : int(row['friend2']),
    }
        
    return mongo_row
        

def preproc_client_first_purchase_date(row):

    mongo_row = {
        'client_id' : int(row['client_id']),
        'first_purchase_date' : processing_date(row['first_purchase_date']),
        'user_id' : int(row['user_id']),
        'user_device_id': int(row['user_device_id']),
    }
    
    return mongo_row

def improve_t_f(value):
    if value == 't':
        return True
    else:
        return False


def preproc_messages(row):
        
    return {
        'message_id': row['message_id'],
        'campaign_id' : int(row['campaign_id']),
        'message_type': row['message_type'],
        'client_id' : int(row['client_id']),
        'channel' : row['channel'],
        'platform' : row['platform'],
        'email_provider' : row['email_provider'],
        'stream' : row['stream'],
        'sent_at': processing_date(row['sent_at']),
        'status':{
            'is_opened' : improve_t_f(row['is_opened']),
            'opened_at' : {
                'first' : processing_date(row['opened_first_time_at']),
                'last' : processing_date(row['opened_last_time_at']),
            },
            'is_clicked':  improve_t_f(row['is_clicked']),
            'clicked_at' : {
                'first' : processing_date(row['clicked_first_time_at']),
                'last' : processing_date(row['clicked_last_time_at']),
            },
            'is_unsubscribed': improve_t_f(row['is_unsubscribed']),
            'unsubscribed_at': processing_date(row['unsubscribed_at']),
            'is_hard_bounced': improve_t_f(row['is_hard_bounced']),
            'hard_bounced_at': processing_date(row['hard_bounced_at']),
            'is_soft_bounced': improve_t_f(row['is_soft_bounced']),
            'soft_bounced_at': processing_date(row['soft_bounced_at']),
            'is_complained' : improve_t_f(row['is_complained']),
            'complained_at' : processing_date(row['complained_at']),
            'is_blocked': improve_t_f(row['is_blocked']),
            'blocked_at': processing_date(row['blocked_at']),
            'is_purchased': improve_t_f(row['is_purchased']),
            'purchased_at' : processing_date(row['purchased_at']),
        },
        'created_at': processing_date(row['created_at']),
        'updated_at': processing_date(row['updated_at']),
        'user_device_id' : int(row['user_device_id']),
        'user_id': int(row['user_id']),
        
    }


campaigns_json_preproc = campaigns.swifter.apply(preproc_campaings, axis=1)

events_json_preproc = events.swifter.apply(preproc_event, axis=1)

friends_json_preproc = friends.swifter.apply(preproc_friends, axis=1)

client_first_purchase_date_json_preproc = client_first_purchase_date.swifter.apply(preproc_client_first_purchase_date, axis=1)


path = '../data/cleaned_MongoDB/'


campaigns_json_preproc.reset_index().to_json(path + 'campaigns.json', orient='records')
events_json_preproc.reset_index().to_json(path + 'events.json', orient='records')
friends_json_preproc.reset_index().to_json(path + 'friends.json', orient='records')
client_first_purchase_date_json_preproc.reset_index().to_json(path + 'cfp_date.json', orient='records')

del(campaigns_json_preproc)
del(events_json_preproc)
del(friends_json_preproc)
del(client_first_purchase_date_json_preproc)

df_split = np.array_split(messages, 5)

i = 1
for df in df_split:
    messages_json_preproc = df.swifter.apply(preproc_messages, axis=1)
    messages_json_preproc.reset_index().to_json(path+f'messages_date{i}.json', orient='records', date_format='iso')
    i+=1


names = ['campaigns.json', 'events.json', 'cfp_date.json', 'messages_date1.json', 'messages_date2.json', 'messages_date3.json', 'messages_date4.json', 'messages_date5.json']


dates_col = ['first_purchase_date', 'event_time']

for file_name in names:
    with open(f'../data/cleaned_MongoDB/{file_name}', 'r+') as f:
        data = json.load(f)
        for val in data:
            for key, value in val.items():
                if (key.endswith('_at')) or (key in dates_col):
                    try:
                        data[key] = {
                    "$date": datetime.datetime.strptime(data[key], "%Y-%m-%d %H:%M:%S UTC").strftime(
                        "%Y-%m-%dT%H:%M:%SZ")}
                    except ValueError:
                        try:
                    # Если формат не соответствует, пробуем другой формат, например ISO
                            dt = datetime.datetime.fromisoformat(value)
                        except ValueError:
                            # Если не удается распознать формат, оставляем значение без изменений
                            continue
                    
        json.dump(data, f)                
        

for name in names:
    with open(f'../data/cleaned_MongoDB/{name}', 'r', encoding='utf-8') as f:
        text = f.read()
    
    text_fixed = re.sub(r'\]\s*\[', ',', text)
    
    if not text_fixed.strip().startswith('['):
        text_fixed = '[' + text_fixed
    if not text_fixed.strip().endswith(']'):
        text_fixed = text_fixed + ']'
    
    try:
        data = json.loads(text_fixed)
    except json.JSONDecodeError as e:
        print("Ошибка декодирования JSON:", e)
        exit(1)
    
    unique_records = {}
    for record in data:
        if "id" in record:
            key = record["id"]
        else:
            key = json.dumps(record, sort_keys=True)
        unique_records[key] = record
    
    unique_list = list(unique_records.values())
    
    with open(f'../data/cleaned_MongoDB/{name}+_fixed.json', 'w', encoding='utf-8') as f:
        json.dump(unique_list, f)





def clean_events_neo(events : pd.core.frame.DataFrame):
    
    events['event_time'] = events['event_time'].astype(str).str.replace(" UTC", "", regex=False)
    events['event_time'] = pd.to_datetime(events['event_time'])
    
    events['price'] = pd.to_numeric(events['price'])
    
    products = pd.DataFrame()
    
    products['product_id'] = events['product_id']
    products['category_id'] = events['category_id']
    products['price'] = events['price']
    products['brand'] = events['brand']
    products['category_code'] = events['category_code']
    
    events = events.drop(columns=['category_code','brand', 'price'])

        
    products['brand'] = products.groupby(['product_id', 'category_id', 'category_code'])['brand'].transform('first')
    
    
    return events, products

events_neo, products_neo = clean_events_neo(events)

events_neo.to_csv('../data/cleaned_data_neo/Clean_Events_Neo.csv', index=False)
products_neo.to_csv(f'../data/cleaned_data_neo/Clean_Products_Neo.csv', index=False)

