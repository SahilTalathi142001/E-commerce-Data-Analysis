import os
import pandas as pd
import mysql.connector

def get_sql_type(dtype):

    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

def clean_column_name(name):

    return name.replace(' ', '_').replace('-', '_').replace('.', '_')

def create_table_query(table_name, columns):

    columns_sql = ', '.join([f'`{col}` {get_sql_type(dtype)}' for col, dtype in columns.items()])
    return f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns_sql})'

def insert_data_query(table_name, columns):

    columns_sql = ', '.join([f'`{col}`' for col in columns])
    placeholders = ', '.join(['%s'] * len(columns))
    return f"INSERT INTO `{table_name}` ({columns_sql}) VALUES ({placeholders})"

def process_csv_files(folder_path, csv_files, cursor):

    for csv_file, table_name in csv_files:
        file_path = os.path.join(folder_path, csv_file)

        # Read CSV file
        df = pd.read_csv(file_path)

        # Replace NaN with None
        df = df.where(pd.notnull(df), None)


        # Clean column names
        df.columns = [clean_column_name(col) for col in df.columns]

        # Generate and execute CREATE TABLE query
        columns = {col: df[col].dtype for col in df.columns}
        create_table_sql = create_table_query(table_name, columns)
        cursor.execute(create_table_sql)

        # Generate and execute INSERT INTO query
        insert_sql = insert_data_query(table_name, df.columns)
        for _, row in df.iterrows():
            values = tuple(None if pd.isna(x) else x for x in row)
            cursor.execute(insert_sql, values)


        conn.commit()

# List of CSV files and corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('geolocation.csv', 'geolocation'),
    ('order_items.csv', 'order_items'),
    ('payments.csv', 'payments'),
    ('products.csv', 'products'),
    ('sellers.csv', 'sellers'),
    ('orders.csv', 'orders')
]

# Database connection
conn = mysql.connector.connect(
            host="127.0.0.1",
            port=3306,
            user="username",
            password="password",
            database="database_name"
)

cursor = conn.cursor()

#Folder path containing CSV files
folder_path = 'path'

#Process the CSV files
process_csv_files(folder_path, csv_files, cursor)

cursor.close()
conn.close()
