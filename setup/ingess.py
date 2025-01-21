import pandas as pd
import json
import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.core import Root
# from snowflake.snowpark import Session
from snowflake.connector.pandas_tools import write_pandas
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import PyPDFLoader

# Load environment variables
load_dotenv()

# Snowflake connection parameters from environment variables
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
)

# Create cursor
cur = conn.cursor()

# Use the database, schema, and warehouse
cur.execute(f"USE WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')}")
cur.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
cur.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_SCHEMA')}")


def parse_documents():
    docs_dir = os.path.join(os.getcwd(), 'docs')
    chunks_df = pd.DataFrame(columns=['FILE_NAME', 'PAGE_NUMBER', 'CHUNK'])

    # Initialize text splitter (you can reuse your existing settings)
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1024,
        chunk_overlap=256,
        length_function=len
    )

    for file in os.listdir(docs_dir):
        if file.endswith('.pdf'):
            file_path = os.path.join(docs_dir, file)
            print(f'Processing: {file_path}')

            # Load PDF
            loader = PyPDFLoader(file_path)
            pages = loader.load()

            # Split into chunks
            chunks = text_splitter.split_documents(pages)

            # Extract text and metadata
            doc_chunks = [
                {
                    'FILE_NAME': file,
                    'PAGE_NUMBER': chunk.metadata.get('page'),
                    'CHUNK': chunk.page_content
                }
                for chunk in chunks
            ]

            # Add to DataFrame
            doc_df = pd.DataFrame(doc_chunks)
            chunks_df = pd.concat([chunks_df, doc_df], ignore_index=True)

    return chunks_df


# create chunck table
cur.execute(f'''
    create or replace TABLE DOCS_CHUNKS_TABLE ( 
    FILE_NAME VARCHAR(16777216), -- Name of the file
    PAGE_NUMBER VARCHAR(16777216), -- Page number of the file
    CHUNK VARCHAR(16777216) -- Piece of text
);
            ''')

try:
    # Process documents
    chunks_df = parse_documents()

    # Write to Snowflake
    write_pandas(conn, chunks_df, 'DOCS_CHUNKS_TABLE')
    print(f"Processed {len(chunks_df)} chunks")
except Exception as e:
    print(f"Error processing documents: {str(e)}")

# Create cortex search service
cur.execute(f'''
    create or replace CORTEX SEARCH SERVICE NUTRITION_SERVICE
    ON chunk
    warehouse = COMPUTE_WH
    TARGET_LAG = '1 day'
    as (
        select chunk,
            file_name,
            page_number,
        from docs_chunks_table
    );
''')
cur.execute(f'''
    create or replace CORTEX SEARCH SERVICE FOOD_LIST_SERVICE
    ON DESCRIPTION
    warehouse = COMPUTE_WH
    TARGET_LAG = '1 day'
    as (select description, fdcid from FOODS)
''')
