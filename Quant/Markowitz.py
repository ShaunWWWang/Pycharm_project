import yfinance as yf
import yahoo_fin.stock_info as si
from yahoo_fin.stock_info import get_data
import pandas as pd
dow_list = si.tickers_dow()
dow_list.remove('DOW')
historical_datas={}
mu=[]
for ticker in dow_list:
    historical_datas[ticker]=get_data(ticker, start_date='2023-01-04')
    historical_datas[ticker]=historical_datas[ticker]['adjclose'].pct_change().dropna()
    mu.append(sum(historical_datas[ticker]))
    # print(historical_datas[ticker]['adjclose'].info)
df=pd.DataFrame.from_dict(historical_datas)
# print(mu)


from scipy.optimize import minimize
import numpy as np
covariance_matrix = df.cov()
# print(covariance_matrix)


# Define constraints (e.g., weights sum to 1)
constraints = {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}

# Define bounds (weights between 0 and 1)
bounds = tuple((0, 1) for asset in range(len(historical_datas)))

# Define objective function (negative of Sharpe ratio)
def objective(w, Sigma):

    portfolio_volatility = np.sqrt(w.T @ Sigma @ w)

    return portfolio_volatility

# Initial guess for weights
initial_weights = np.ones(len(historical_datas)) / len(historical_datas)
returns=[]
volatilities=[]
# Perform optimization
for r in np.linspace(min(mu), max(mu), num=100):
    # Constraint: Portfolio return equals the specified return (r)
    constraints_r = {'type': 'eq', 'fun': lambda w: np.sum(w * mu) - r}
    result = minimize(objective, initial_weights, args=(covariance_matrix),method='SLSQP', bounds=bounds, constraints=[constraints,constraints_r])
    returns.append(r)
    volatilities.append(result.fun)
    # Extract optimized weights
    # optimized_weights = result.x
    # print("Optimized Portfolio Weights:")
    # print(optimized_weights)


import matplotlib.pyplot as plt
# Plot the efficient frontier
plt.figure(figsize=(10, 6))
plt.scatter(volatilities, returns, marker='o')
plt.title('Efficient Frontier')
plt.xlabel('Volatility (Risk)')
plt.ylabel('Return')
plt.show()