import os
import sqlite3
import pandas as pd
from pathlib import Path

from src.database_utils.db_values.preprocess import make_db_lsh
from src.database_utils.db_catalog.preprocess import make_db_context_vec_db


for db_id in ['customers_and_orders', 'e_commerce', 'real_estate_rentals', 'restaurant_bills']:
    print('Processing database:', db_id)
    path = f'/home/chenjie/projects/chase-sql-on-finch/data/spider/{db_id}'
    conn = sqlite3.connect(os.path.join(path, f'{db_id}.sqlite'))
    cursor = conn.cursor()

    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    metadata = []

    for table in tables:
        table_name = table[0]
        
        # Get column information for each table
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        for column in columns:
            # column tuple: (cid, name, type, notnull, dflt_value, pk)
            column_name = column[1]
            column_type = column[2]
            
            metadata.append({
                'table_name': table_name,
                'column_name': column_name,
                'original_column_name': column_name
            })

    conn.close()

    # Save to CSV
    csv_path = os.path.join(path, 'database_description')
    os.makedirs(csv_path, exist_ok=True)
    df = pd.DataFrame(metadata)
    for table_name in df.table_name.unique():
        table_df = df[df.table_name == table_name].copy()
        output_path = os.path.join(csv_path, f"{table_name}.csv")
        table_df.to_csv(output_path, index=False)


    make_db_lsh(
            path, 
            signature_size=100, 
            n_gram=3, 
            threshold=0.01,
            verbose=True)
    make_db_context_vec_db(
        path, 
        use_value_description=False)

print("Done!")