import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select

def MA(price_line,n):
    # price_line:价格序列 (list,array,series)
    # n:周期参数 (int>0)
    # return list
    df=pd.Series(price_line)
    ma=df.rolling(window=n).mean()
    ma.fillna(value=df.expanding(min_periods=1).mean(),inplace=True)
    return list(ma)

def RSI(price_line,n1=6,n2=12,n3=1):
    # price_line: 价格序列(list, array, series)
    # n1:短周期参数 (int>0)
    # n2:长周期参数 (int>0)
    # return list [短周期RSI，长周期RSI]
    price=pd.Series(price_line)
    u=price.diff(1).fillna(0)
    d=u.copy()
    u[u<0]=0
    d[d>0]=0
    d=abs(d)
    df_short=pd.DataFrame({'u':MA(u,n1),'d':MA(d,n1)})
    rs_short=df_short['u']/df_short['d']
    df_long = pd.DataFrame({'u': MA(u, n2), 'd': MA(d, n2)})
    rs_long = df_long['u'] / df_long['d']
    return [list(100-100/(1+rs_short)),list(100-100/(1+rs_long))]

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

        print(f'Data for stock {stock_code} downloaded successfully')
        return df

    except Exception as e:
        print('Error occurred while fetching stock data:', e)

def form_dataframe(stock_codes_list,start_date,end_date):

    if len(stock_codes_list)==0:
        return 'Stock_codes_list is empty'

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    df_1= get_stock_data(stock_codes_list[0])
    df_1 = df_1[['Date','CLOSE']]
    df_1['Date'] = pd.to_datetime(df_1['Date'])
    df_1 = df_1[df_1['Date'] >= start_date]
    df_1 = df_1[df_1['Date'] <= end_date]

    if len(stock_codes_list)==1:
        return df_1
    else:
        df_2 = get_stock_data(stock_codes_list[1])
        df_2 = df_2[['Date', 'CLOSE']]
        df_2['Date'] = pd.to_datetime(df_2['Date'])
        df_2 = df_2[df_2['Date'] >= start_date]
        df_2 = df_2[df_2['Date'] <= end_date]

        df = pd.merge(df_1, df_2, on='Date', how='outer')
        df = df.rename(columns={'CLOSE_x': stock_codes_list[0], 'CLOSE_y': stock_codes_list[1]})

        if len(stock_codes_list)==2:
            df = df.sort_values(by='Date')
            df = df.ffill()
            return df
        else:
            for i in stock_codes_list[2:]:
                df_3 = get_stock_data(i)
                df_3 = df_3[['Date', 'CLOSE']]
                df_3['Date'] = pd.to_datetime(df_3['Date'])
                df_3 = df_3[df_3['Date'] >= start_date]
                df_3 = df_3[df_3['Date'] <= end_date]

                df = pd.merge(df, df_3, on='Date', how='outer')
                df = df.rename(columns={'CLOSE': i})

            df = df.sort_values(by='Date')
            df = df.ffill()
            return df

stock_codes_list = get_stock_codes()
stock_codes_list = list(stock_codes_list['stock_code'])
print(stock_codes_list)
start_date = '2000-01-01'
end_date = '2024-02-11'
df=form_dataframe(stock_codes_list,start_date,end_date)
print(df.info)