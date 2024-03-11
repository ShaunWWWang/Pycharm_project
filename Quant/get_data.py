import yfinance as yf
NVDA = yf.Ticker('NVDA')
info = NVDA.info
# for k,v in info.items():
#     print(k,':',v)
his_data = NVDA.history(period='5y')
# print(his_data)
ann_financial = NVDA.financials
quart_financial = NVDA.quarterly_financials
ann_balance = NVDA.balance_sheet
quart_balance = NVDA.quarterly_balance_sheet
ann_income = NVDA.income_stmt
quart_incom =NVDA.quarterly_income_stmt
ann_cash = NVDA.cash_flow
quart_cash = NVDA.quarterly_cash_flow

closing_price = his_data['Close']
daily_return = closing_price.pct_change().dropna()
print(daily_return)
# import json
# import requests
# def pretty_print(data:dict):
#     print(json.dumps(data,indent=4))
# api_key = 'NNQP4D58TM3NDXB4'
# def retrieve_data(function:str,symbol:str,api_key:str):
#     url=f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={api_key}'
#     response=requests.get(url)
#     data=response.text
#     parsed = json.loads(data)
#     return parsed
# pretty_print(retrieve_data('balance_sheet','NVDA',api_key))
