# 导入必要的模块
from scipy.stats import rankdata
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import pyquantfin.nav_calc as cnav
import akshare as ak
import mysql.connector


plt.rcParams['font.sans-serif'] = ['SimHei']    # 解决中文显示问题
plt.rcParams['axes.unicode_minus'] = False    # 解决负号显示问题


# 定义一个函数，根据市盈率PE Ratio对原始数据进行分组
def group_1d(arr, nog):
    """
    根据某条件对一维数据进行分组
    :param arr: 分组前的原始数据
    :param nog: 组数，要把原始数据分成几组
    :return: 以Numpy数组形式返回分组的结果
    """
    # 创建一个全零数组，用于存放最终的分组结果
    results = np.zeros_like(arr, dtype=int)
    # 接受一个数组，返回该数组中元素对应的顺序编号，如果元素相同，则返回该元素对应顺序的平均值
    ranks = rankdata(arr)

    total_num = len(arr)  # 数据总量
    nan_num = np.isnan(arr).sum()  # nan数据量
    no_nan_num = total_num - nan_num  # 非空数据量
    # 下面计算每个分组中有多少个元素，考虑到实际意义，在此四舍五入
    d = round(no_nan_num / nog)

    # 下面开始分组
    # 除了最后一组，其余分组中包含的全部元素个数是相同的
    for i in range(1, nog):
        rank1 = 1 + (i - 1) * d  # 当前分组开始元素对应的顺序编号值
        rank2 = rank1 + d  # 当前分组结束元素对应的顺序编号值
        results[(ranks >= rank1) & (ranks < rank2)] = i  # 写入对应元素的分组结果

    # 最后一组的分组情况
    rank1 = 1 + (nog - 1) * d
    rank2 = no_nan_num
    results[(ranks >= rank1) & (ranks < rank2)] = nog  # 剩下的全部是最后一组

    return results  # 返回分组结果


def grouping(arr2d, nog):
    """
    对二维数据进行分组
    :arr2d：传入需要分组的二维数据集
    :nog：组数
    :return：返回分组的结果
    """
    results = np.zeros_like(arr2d, dtype=int)  # 创建一个全零数组，用于保存分组的结果
    # 分组
    for i in range(len(results)):  # 对第i行数据进行分组
        results[i] = group_1d(arr2d[i], nog)  # 调用函数，对某一行数据进行分组

    return results  # 返回二维数据的分组结果


def getting_weight(group, group_id):
    """
    根据分组结果和组号生成该组股票对应的仓位权重，核心思路是等权重持有pe最低的股票
    :param group：传入分组之后的二维数组
    :param group_id：需要创建仓位的分组组号，在此仅仅对pe最低的一组创建仓位
    :return：返回仓位权重
    """
    w = (group == group_id).astype(int)  # 得到仅含有0和1的二维数组w

    w_sum = w.sum(axis=1)  # 1代表按行求和
    w_sum[w_sum == 0] = 1  # 为避免除零错误，在此人为将全零行权重和设置为1

    w = w / w_sum.reshape((-1, 1))  # 重构权重矩阵

    return w

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
    df = pd.read_sql(query, connection)
    print(f'Data for stock {stock_code} downloaded')
    # Closing the database connection
    connection.close()
    return df

trade_dates=[]
trade_dates_pe=[]
stock_info_a_code_name_df = ak.stock_info_a_code_name()
codes = list(stock_info_a_code_name_df['code'])
bank_stock_pool = codes[:10]
start_date = '2022-01-01'
end_date = '2024-01-10'

import yfinance as yf

# Specify the ticker symbol for the CSI 300 index
csi_300_ticker = '000300.SS'

# Download historical data
csi_300_data = yf.download(csi_300_ticker, start=start_date, end=end_date)
csi_300_data['Date'] = csi_300_data.index
csi_300_data['Date'] = csi_300_data['Date'].astype('str')
csi_300_data['Date'] = csi_300_data['Date'].apply(
        lambda x: datetime.strptime(x, '%Y-%m-%d').strftime('%Y%m%d'))
df_csi_300 = pd.DataFrame(columns=['Adj_Close'])
df_csi_300['Adj_Close'] = pd.Series(csi_300_data['Adj Close'].values, index=csi_300_data['Date'])


for stock_code in bank_stock_pool:
    bank_stock_daily_basic = ak.stock_financial_abstract_ths(stock_code,indicator='按报告期')
    bank_stock_daily = get_stock_data_from_mysql(stock_code, start_date, end_date)

    bank_stock_daily_basic = bank_stock_daily_basic[bank_stock_daily_basic['报告期'] >= start_date]
    bank_stock_daily_basic['报告期'] = bank_stock_daily_basic['报告期'].apply(
        lambda x: datetime.strptime(x, '%Y-%m-%d').strftime('%Y%m%d'))
    # print(bank_stock_daily_basic)
    bank_stock_daily['Date'] = bank_stock_daily['Date'].astype('str')
    bank_stock_daily['Date'] = bank_stock_daily['Date'].apply(
        lambda x: datetime.strptime(x, '%Y-%m-%d').strftime('%Y%m%d'))
    # print(bank_stock_daily)

    trade_dates.extend(bank_stock_daily['Date'])
    trade_dates=list(np.sort(list(set(trade_dates))))
    trade_dates_pe.extend(bank_stock_daily_basic['报告期'])
    trade_dates_pe =list(np.sort(list(set(trade_dates_pe))))

df_prz = pd.DataFrame(columns=bank_stock_pool,index=trade_dates)
df_eps = pd.DataFrame(columns=bank_stock_pool,index=trade_dates_pe)

for stock_code in bank_stock_pool:
    bank_stock_daily_basic = ak.stock_financial_abstract_ths(stock_code, indicator='按报告期')
    bank_stock_daily = get_stock_data_from_mysql(stock_code, start_date, end_date)

    bank_stock_daily_basic = bank_stock_daily_basic[bank_stock_daily_basic['报告期'] >= start_date]
    bank_stock_daily_basic['报告期'] = bank_stock_daily_basic['报告期'].apply(
        lambda x: datetime.strptime(x, '%Y-%m-%d').strftime('%Y%m%d'))
    # print(bank_stock_daily_basic)
    bank_stock_daily['Date'] = bank_stock_daily['Date'].astype('str')
    bank_stock_daily['Date'] = bank_stock_daily['Date'].apply(
        lambda x: datetime.strptime(x, '%Y-%m-%d').strftime('%Y%m%d'))
    df_prz[stock_code] = pd.Series(bank_stock_daily['Close'].values, index=bank_stock_daily['Date'].values)    # 将个股后复权收盘价填入对应位置
    df_eps[stock_code] = pd.Series(bank_stock_daily_basic['基本每股收益'].values, index=bank_stock_daily_basic['报告期'].values)    # 将个股后复权收盘价填入对应位置


df_eps = df_eps.fillna(method='ffill')
df_prz = df_prz.fillna(method='ffill')

l=[]
for i in range(len(df_eps.index)):
    search_index = df_eps.index[i]
    nearest_index = df_prz.index.asof(search_index)
    l.append(nearest_index)

df=df_prz.loc[l]
df_eps = df_eps.apply(pd.to_numeric, errors='coerce')
df.reset_index(drop=True , inplace = True)
df1 = df_eps.reset_index(drop = True)
df_pe = df / df1
df_pe['date'] = l
df_pe.set_index('date', inplace=True)


pe_group = grouping(df_pe.values, 3)    # 分成三组

bk_rst = {}    # 创建一个空字典，用于存放策略回测后的结果

for i, j in zip(['low_pe', 'mid_pe', 'high_pe'], [1, 2, 3]):
    # zip函数用于产生两个可迭代对象的一对一元组
    print(f"Backtesting the {i} group...", end='')    # 打印回测结果提示信息
    weight = getting_weight(group=pe_group, group_id=j)    # 根据分组的结果和对应的分组序号，产生该分组对应的权重，得到一个二维数组
    weight_df = pd.DataFrame(weight, index=df_pe.index, columns=df_pe.columns)    # 将权重二维数组转换成DataFrame形式
    print(weight_df)
    bk_rst[i] = cnav.multi_assets(df_prz, weight_df, weight_df.index.values, fee=2.5/10000)    # 根据历史行情和投资组合权重对策略进行回测
    """
        multi_assets:
        参数一：传入行情数据df；
        参数二：传入投资组织标的权重df；
        参数三：再平衡日（在该项目中指定为每月的最后一天）；
        参数四：交易费率
    """
    print("done.")
nav = {}  # 创建一个空字典，用于存放每个分组回测的净值结果

for i, df in bk_rst.items():
    nav[i] = df.nav

# 将nav回测结果转换为df
nav_df = pd.DataFrame(nav)
nav_df['Index_HS300'] = csi_300_data['Adj Close']
print(nav_df)

nav_df = nav_df / nav_df.iloc[0]
nav_df.plot()
plt.xticks(rotation=45)    # 横轴旋转45度，解决数据重叠显示问题
plt.show()

nav_excess = nav_df.low_pe / nav_df.high_pe
nav_excess.plot()
plt.xticks(rotation=45)    # 横轴旋转45度，解决数据重叠显示问题
plt.show()