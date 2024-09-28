import json
import boto3
import io
import urllib.parse
import numpy as np
import pandas as pd
from io import StringIO

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Load data
        bucket_name = event['bucket_name']
        object_key =  urllib.parse.unquote_plus(event['object_key'], encoding = 'utf-8')
        
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        data = response['Body'].read().decode('utf-8')
        zomato_df = pd.read_csv(io.StringIO(data))
        
        # Drop Unnecessary Columns
        zomato_df.drop(['url','phone','address','reviews_list','menu_item', 'dish_liked'], axis=1, inplace=True)
    
        
        # Data Formatting and Cleaning
        zomato_df['approx_cost(for two people)'] = zomato_df['approx_cost(for two people)'].str.replace(',', '').astype(float)
        
        # Replace all commas in all columns
        zomato_df = zomato_df.applymap(lambda x: x.replace(',', '') if isinstance(x, str) else x)
        
        def convert_rating(rating_str):
            if isinstance(rating_str, str) and '/5' in rating_str:
                return float(rating_str.split('/')[0])
            else:
                return np.NaN

        zomato_df['rate'] = zomato_df['rate'].apply(convert_rating)
        
        zomato_df.replace(['', 'None', 'NA', 'NaN', '-999'], 'None', inplace=True)
        
        zomato_df['online_order'] = zomato_df['online_order'].apply(lambda x: 1 if x == 'Yes' else 0)
        zomato_df['book_table'] = zomato_df['book_table'].apply(lambda x: 1 if x == 'Yes' else 0)
        
        # Handling Duplicate Rows
        zomato_df.drop_duplicates(inplace=True)
        
        # Handling Missing Data
        zomato_df['approx_cost(for two people)'].fillna(zomato_df['approx_cost(for two people)'].mean(), inplace=True)
        zomato_df['rest_type'].fillna(zomato_df['rest_type'].mode()[0], inplace=True)
        zomato_df['cuisines'].fillna(zomato_df['cuisines'].mode()[0], inplace=True)
        zomato_df['location'].fillna(zomato_df['location'].mode()[0], inplace=True)
        
        # Convert DataFrame to Parquet and store in a BytesIO buffer
        parquet_buffer = io.BytesIO()
        
        # Load DataFrame into s3 as CSV
        csv_buffer = StringIO()
        zomato_df.to_csv(csv_buffer, index=False, quotechar = '"')
        
        name = object_key.split('/')
        name[0] = 'data_after_process'
        name[1] = name[1]
        name = '/'.join(name)
        
        s3_client.put_object(Bucket=bucket_name, Key=name, Body=csv_buffer.getvalue())
        
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'File processed and uploaded successfully to {bucket_name}/{name}')
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
