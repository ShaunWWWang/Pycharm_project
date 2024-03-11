# 导入解析包
from bs4 import BeautifulSoup
import requests
import pandas as pd

url = 'https://finance.yahoo.com/quote/TSLA/history?p=TSLA'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}
html = requests.get(url, headers=headers)
# 创建beautifulsoup解析对象


soup = BeautifulSoup(html.text,
                     'lxml')  # html_doc 表示要解析的文档，而 html.parser 表示解析文档时所用的解析器，此处的解析器也可以是 'lxml' 或者 'html5lib'

obj = soup.find('tbody')
results = obj.find_all('span')
rows = []
rows.append(['Date', 'Open',
             'High', 'Low',
             'Close', 'Adj_close',
             'Volume']
             )
print(results)
print(len(results))
l=[]
for i in range(len(results)):
    if i%7 == 0:
        if i!=0:
            rows.append(l)
        l=[]
    result = results[i].getText()
    l.append(result)
print(rows)
df = pd.DataFrame(rows,columns=rows[0])
print(df.info)
# prettify()用于格式化输出html/xml文档
# print(soup.prettify())

# 查找第一个符合条件的标签
# print(soup.find('span', class_='year'))
# # 查找所有符合条件的标签
# for item in soup.find_all('span', class_='pl'):
#     print(item.text)

html_doc = """
<html>
<head>
    <title>电影列表</title>
</head>
<body>
    <h1>电影列表</h1>
    <div class="movie">
        <h2>黑白迷宫</h2>
        <p>导演：张艺谋</p>
        <p>主演：李连杰、章子怡</p>
        <p>评分：<span class="rating">8.9</span></p>
    </div>
    <div class="movie">
        <h2>西游记之大圣归来</h2>
        <p>导演：田晓鹏</p>
        <p>主演：张磊、石磊、杨晓婧</p>
        <p>评分：<span class="rating">9.2</span></p>
    </div>
</body>
</html>
"""
soup = BeautifulSoup(html_doc, 'html.parser')
# 查找所有div标签
# div_list = soup.find_all('div')
# print(div_list)
# # 查找class属性值为"movie"的div标签
# movie_list = soup.find_all('div', class_='movie')
# print(movie_list)
# # 查找评分大于9.0的电影
# rating_list = soup.find_all('span', class_='rating', string=lambda x: float(x) > 9.0)
# print(rating_list)
# 查找第一个电影的导演名
# director = soup.find('div', class_='movie').find('p')
# print(director)