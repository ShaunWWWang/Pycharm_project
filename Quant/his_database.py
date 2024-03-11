import mysql.connector
from mysql.connector import errorcode
import akshare as ak
from sqlalchemy import create_engine, MetaData, Table, select
import pandas as pd
from datetime import datetime, timedelta

# 函数：从akshare获取股票代码列表
def download_stock_codes():
    return ak.stock_info_a_code_name()

# 函数：从mysql database 获取股票代码列表
def get_stock_codes():
    # Replace these values with your MySQL connection details
    DATABASE_URL = 'mysql+mysqlconnector://root:ImSpiderMan1@localhost/stock_data'

    # Create a SQLAlchemy engine
    engine = create_engine(DATABASE_URL, echo=False)

    # Define the table schema (replace 'stock_codes' and add columns as needed)
    metadata = MetaData()
    stock_codes_table = Table(
        'stock_codes',
        metadata,
        autoload_with=engine
    )

    # Create a connection
    with engine.connect() as connection:
        # Construct a SQLAlchemy SELECT query
        select_query = select(stock_codes_table.c.stock_code)

        # Execute the SELECT query and fetch the results
        result = connection.execute(select_query)
        rows = result.fetchall()

        # Create a DataFrame using pandas
        df = pd.DataFrame(rows, columns=['stock_code'])

    print('Stock codes data downloaded')
    return df

# 函数：下载股票历史行情数据
def download_stock_data(stock_code,start_date):
    data = ak.stock_zh_a_hist(symbol=stock_code,adjust='hfq',start_date=start_date)
    return data

# 函数：将股票历史行情数据存储到MySQL数据库
def save_to_mysql(df, stock_code):
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='ImSpiderMan1',
        database='stock_data'
    )
    cursor = connection.cursor()

    # 假设你已经在数据库中创建了表，表结构需要与股票历史行情数据的列对应
    table_name = f'stock_{stock_code}'
    create_table_query = (
    "CREATE TABLE IF NOT EXISTS {} (Date DATE,OPEN FLOAT,CLOSE FLOAT,HIGH FLOAT,LOW FLOAT,VOLUME INT,TURNOVER FLOAT,AMPLITUDE FLOAT,PERCENTAGE_CAHNGE FLOAT,CHANGE_IN_PRICE FLOAT,TURNOVER_RATE FLOAT);".format(table_name)
    )

    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(create_table_query)
    except mysql.connector.Error as err:
        print(err.msg)

    # 将数据插入数据库表
    for index, row in df.iterrows():
        insert_query = (
        "INSERT INTO {} (Date, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER, AMPLITUDE, PERCENTAGE_CAHNGE, CHANGE_IN_PRICE, TURNOVER_RATE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);".format(table_name))

        cursor.execute(insert_query, tuple(row))

    connection.commit()
    cursor.close()
    connection.close()

# 函数：从mysql database 获取指定股票数据
def get_stock_data(stock_code):
    try:
        # Replace these values with your MySQL connection details
        DATABASE_URL = 'mysql+mysqlconnector://root:ImSpiderMan1@localhost/stock_data'

        # Create a SQLAlchemy engine
        engine = create_engine(DATABASE_URL, echo=False)

        # Define the table schema (replace 'stock_codes' and add columns as needed)
        metadata = MetaData()
        table_name = f'stock_{stock_code}'
        stock_codes_table = Table(
            table_name,
            metadata,
            autoload_with=engine
        )

        # Create a connection
        with engine.connect() as connection:
            # Construct a SQLAlchemy SELECT query
            select_query = select(stock_codes_table)

            # Execute the SELECT query and fetch the results
            result = connection.execute(select_query)
            rows = result.fetchall()

            # Create a DataFrame using pandas
            df = pd.DataFrame(rows, columns=result.keys())

        print('Stock data downloaded successfully')
        return df

    except Exception as e:
        print('Error occurred while fetching stock data:', e)

# 函数：更新mysql database 指定股票数据
def update_mysql(stock_code):
    try:
        df = get_stock_data(stock_code)
        last_date = df['Date'].iloc[-1]
        last_date = last_date + timedelta(days=1)
        last_date = last_date.strftime("%Y%m%d")
        data = download_stock_data(stock_code, last_date)
        if len(data)!=0:
            save_to_mysql(data, stock_code)
            print(f'Data for stock {stock_code} downloaded and saved to MySQL.')
        else:
            print("There's nothing to update.")
    except Exception as e:
        print('Error occurred while fetching stock data:', e)
        data = ak.stock_zh_a_hist(symbol=stock_code, adjust='hfq')
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='ImSpiderMan1',
            database='stock_data'
        )
        cursor = conn.cursor()
        # Construct the SQL query to drop the table
        table_name = f'stock_{stock_code}'
        sql_query = f"DROP TABLE IF EXISTS {table_name}"

        # Execute the SQL query
        cursor.execute(sql_query)

        print(f"Table '{table_name}' dropped successfully")

        # Commit the changes
        conn.commit()

        if cursor:
            cursor.close()
        if conn:
            conn.close()
        save_to_mysql(data, stock_code)






# 主函数
def main():
    stock_info_a_code_name_df = get_stock_codes()
    codes = list(stock_info_a_code_name_df['stock_code'])
    print(len(codes))
    idx=codes.index('002107')
    codes=codes[idx:]
    for stock_code in codes:
        print(f'Updating data for stock {stock_code}...')
        update_mysql(stock_code)
        if stock_code == '873833':
            print('ALL stock done.')

if __name__ == "__main__":
    main()
