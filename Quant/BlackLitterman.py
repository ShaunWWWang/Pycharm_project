import numpy as np

# 准备数据
Sigma_market = np.array([[0.01, 0.005], [0.005, 0.02]])  # 市场均衡的协方差矩阵
Pi_market = np.array([0.02, 0.01]).reshape(-1,1)  # 市场均衡的预期收益
P = np.array([[1, -1], [0, 1]])  # picking matrix
Q = np.array([0.2, 0.3]).reshape(-1,1)  # 投资者观点的矩阵

# Black-Litterman 模型的参数
tau = 0.05  # 缩放参数
Omega = tau * P @ Sigma_market @ P.T
# 计算后验协方差矩阵
Sigma_posterior = np.linalg.inv(np.linalg.inv(tau * np.linalg.inv(Sigma_market)) + P.T @ np.linalg.inv(Omega) @ P)

# 计算后验均值
mu_posterior = Sigma_posterior @ (tau * np.linalg.inv(Sigma_market) @ Pi_market + P.T @ np.linalg.inv(Omega) @ Q)

# 输出结果
print("后验协方差矩阵：")
print(Sigma_posterior)

print("\n后验均值：")
print(mu_posterior)

from scipy.optimize import minimize

# Define risk aversion parameter (subjective parameter)
delta = 2

# Define constraints (e.g., weights sum to 1)
constraints = ({'type': 'eq', 'fun': lambda w: np.sum(w) - 1})

# Define bounds (weights between 0 and 1)
bounds = tuple((0, 1) for asset in range(len(mu_posterior)))

# Define objective function (negative of Sharpe ratio)
def objective(w, mu, Sigma, delta):
    portfolio_return = np.dot(w, mu)
    portfolio_volatility = np.sqrt(w.T @ Sigma @ w)
    sharpe_ratio = -portfolio_return / portfolio_volatility
    return sharpe_ratio

# Initial guess for weights
initial_weights = np.ones(len(mu_posterior)) / len(mu_posterior)

# Perform optimization
result = minimize(objective, initial_weights, args=(mu_posterior, Sigma_posterior, delta),
                  method='SLSQP', bounds=bounds, constraints=constraints)

# Extract optimized weights
optimized_weights = result.x

print("Optimized Portfolio Weights:")
print(optimized_weights)
