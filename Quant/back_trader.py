import backtrader as bt
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector
# 自定义交易策略
class TestStrategy(bt.Strategy):
    params = (
        ("short_period", 20),
        ("long_period", 50),
    )

    def __init__(self):
        self.short_ma = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.short_period)
        self.long_ma = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.long_period)

    def next(self):
        if self.short_ma > self.long_ma:
            # Buy signal
            self.buy()
        elif self.short_ma < self.long_ma:
            # Sell signal
            self.sell()

# 获取股票数据（示例数据）
def get_stock_data_from_mysql(stock_code: object, start_date: object, end_date: object) -> object:
    # 连接到 MySQL 数据库
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='ImSpiderMan1',
        database='stock_data'
    )
    cursor = connection.cursor()
    table_name = f'stock_{stock_code}'
    # 构造 SQL 查询语句
    query = f"SELECT Date, Close FROM {table_name} WHERE Date BETWEEN '{start_date}' AND '{end_date}'"

    # 使用 pandas 读取数据
    df = pd.read_sql(query, connection, index_col='Date', parse_dates=['Date'])
    print(f'Data for stock {stock_code} downloaded')
    # Closing the database connection
    connection.close()
    return df

stock_codes = ['000001','000002']
start_date = '2022-01-01'
end_date = '2024-01-10'
for stock_code in stock_codes:
    df = get_stock_data_from_mysql(stock_code, start_date, end_date)
    print(df)
    # 将数据加载到 Backtrader 中
    data_feed = bt.feeds.PandasData(dataname=df)

    # 初始化 Backtrader 引擎
    cerebro = bt.Cerebro()

    # 添加数据到引擎
    cerebro.adddata(data_feed, name=stock_code)

    # 初始资金 100,000,000
    cerebro.broker.setcash(100000000.0)
    # 佣金，双边各 0.0003
    cerebro.broker.setcommission(commission=0.0003)
    # 滑点：双边各 0.0001
    cerebro.broker.set_slippage_perc(perc=0.005)

    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='pnl')  # 返回收益率时序数据
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')  # 年化收益率
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio')  # 夏普比率
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')  # 回撤

    # 将编写的策略添加给大脑，别忘了 ！
    cerebro.addstrategy(TestStrategy)

    # 运行回测
    result=cerebro.run()
    # 从返回的 result 中提取回测结果
    strat = result[0]
    # 返回日度收益率序列
    daily_return = pd.Series(strat.analyzers.pnl.get_analysis())
    # 打印评价指标
    print("--------------- AnnualReturn -----------------")
    print(strat.analyzers._AnnualReturn.get_analysis())
    print("--------------- SharpeRatio -----------------")
    print(strat.analyzers._SharpeRatio.get_analysis())
    print("--------------- DrawDown -----------------")
    print(strat.analyzers._DrawDown.get_analysis())
    # 获取回测结果
    final_portfolio_value = cerebro.broker.getvalue()
    max_drawdown = cerebro.strats.stats.drawdown.drawdown()

    # 获取交易日志
    trade_log_analyzer = cerebro.analyzers.trade_log_analyzer
    trade_log = trade_log_analyzer.trade_log

    # 可视化回测结果
    cerebro.plot(style='candlestick', iplot=False)

    # 打印回测结果和交易日志
    print(f"Final Portfolio Value: {final_portfolio_value}")
    print(f"Max Drawdown: {max_drawdown}")

    print("\nTrade Log:")
    for trade_info in trade_log:
        print(trade_info)

    plt.show()
