import pandas as pd
from sqlalchemy import create_engine
import akshare as ak

# List of stock codes
stock_info_a_code_name_df = ak.stock_info_a_code_name()
codes = list(stock_info_a_code_name_df['code'])

# Create a DataFrame from the list
df = pd.DataFrame({'stock_code': codes})

# MySQL database connection parameters
# Replace with your own database connection information
db_params = {
        'host':'localhost',
        'user':'root',
        'password':'ImSpiderMan1',
        'database':'stock_data',
}

# Create a connection to the database
engine = create_engine(f"mysql+pymysql://{db_params['user']}:{db_params['password']}@{db_params['host']}/{db_params['database']}")

# Write the DataFrame to a MySQL table (replace 'stocks' with your table name)
df.to_sql(name='stock_codes', con=engine, if_exists='replace', index=False)

# Close the database connection
engine.dispose()

