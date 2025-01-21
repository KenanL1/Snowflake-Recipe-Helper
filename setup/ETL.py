import pandas as pd
import json
import os
import streamlit as st
from dotenv import load_dotenv
import snowflake.connector
from snowflake.core import Root
# from snowflake.snowpark import Session
from snowflake.connector.pandas_tools import write_pandas

# Load environment variables
load_dotenv()

# Snowflake connection parameters from environment variables
conn = snowflake.connector.connect(
    user=st.secrets['SNOWFLAKE_USER'],
    password=st.secrets['SNOWFLAKE_PASSWORD'],
    account=st.secrets['SNOWFLAKE_ACCOUNT'],
    # warehouse=st.secrets['SNOWFLAKE_WAREHOUSE'],
    # database=st.secrets['SNOWFLAKE_DATABASE'],
    # schema=st.secrets['SNOWFLAKE_SCHEMA']
)

# Create cursor
cur = conn.cursor()

# Read the JSON file
with open('foundationDownload.json', 'r') as file:
    data = json.load(file)['FoundationFoods']


# Create Food DataFrame
food_data = []
for item in data:
    # print(item)
    food_data.append({
        'FOODCLASS': item.get('foodClass'),
        'DESCRIPTION': item.get('description'),
        'NDBNUMBER': item.get('ndbNumber'),
        'DATATYPE': item.get('dataType'),
        'FOODCATEGORY': item.get('foodCategory', {}).get('description'),
        'FDCID': item.get('fdcId')
    })

food_df = pd.DataFrame(food_data)

# Create Food Nutrients DataFrame
nutrient_data = []
for item in data:
    fdcId = item.get('fdcId')
    for nutrient in item.get('foodNutrients', []):
        if 'nutrient' in nutrient:
            nutrient_data.append({
                'FDCID': fdcId,
                'NUTRIENT_ID': nutrient['nutrient'].get('id'),
                'NUTRIENT_NAME': nutrient['nutrient'].get('name'),
                'UNIT_NAME': nutrient['nutrient'].get('unitName'),
                'AMOUNT': nutrient.get('amount')
            })

nutrients_df = pd.DataFrame(nutrient_data)

# Display the first few rows of each DataFrame
# print("Food DataFrame:")
# print(food_df.head())
# print("\nNutrients DataFrame:")
# print(nutrients_df.head())


try:
    # Create database if not exists
    cur.execute(f"USE WAREHOUSE {st.secrets['SNOWFLAKE_WAREHOUSE']}")
    cur.execute(
        f"CREATE DATABASE IF NOT EXISTS {st.secrets['SNOWFLAKE_DATABASE']}")
    cur.execute(f"USE DATABASE {st.secrets['SNOWFLAKE_DATABASE']}")
    cur.execute(
        f"CREATE SCHEMA IF NOT EXISTS {st.secrets['SNOWFLAKE_SCHEMA']}")
    cur.execute(f"USE SCHEMA {st.secrets['SNOWFLAKE_SCHEMA']}")

    # Create food table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS FOODS (
        FOODCLASS VARCHAR,
        DESCRIPTION VARCHAR,
        NDBNUMBER VARCHAR,
        DATATYPE VARCHAR,
        FOODCATEGORY VARCHAR,
        FDCID NUMBER PRIMARY KEY
    )
    """)

    # Create nutrients table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS FOOD_NUTRIENTS (
        FDCID NUMBER,
        NUTRIENT_ID NUMBER,
        NUTRIENT_NAME VARCHAR,
        UNIT_NAME VARCHAR,
        AMOUNT FLOAT,
        FOREIGN KEY (FDCID) REFERENCES FOODS(FDCID)
    )
    """)

    # Clean DataFrames before loading
    food_df = food_df.fillna('')
    food_df['FDCID'] = food_df['FDCID'].astype(int)

    # nutrients_df = nutrients_df.fillna('')
    nutrients_df['FDCID'] = nutrients_df['FDCID'].astype(int)
    nutrients_df['AMOUNT'] = nutrients_df['AMOUNT'].fillna(0.0)

    # Write DataFrames to Snowflake
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=food_df,
        table_name='FOODS',
        # database=st.secrets['SNOWFLAKE_DATABASE'],
        # schema=st.secrets['SNOWFLAKE_SCHEMA']
    )
    print(f"Loaded {nrows} food records in {nchunks} chunks")

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=nutrients_df,
        table_name='FOOD_NUTRIENTS',
        database=st.secrets['SNOWFLAKE_DATABASE'],
        schema=st.secrets['SNOWFLAKE_SCHEMA']
    )
    print(f"Loaded {nrows} nutrient records in {nchunks} chunks")

    # Verify the data
    cur.execute("SELECT COUNT(*) FROM FOODS")
    print(f"Total foods: {cur.fetchone()[0]}")

    cur.execute("SELECT COUNT(*) FROM FOOD_NUTRIENTS")
    print(f"Total nutrients: {cur.fetchone()[0]}")

except Exception as e:
    print(f"An error occurred: {e}")
    conn.rollback()

finally:
    cur.close()
    conn.close()
