项目背景：某高校图书馆积累了多年的借阅数据（borrow_log_sample.csv），这些数据以结构化文件的形式存储于HDFS，
记录了学生对图书的借阅与归还行为。为了提高图书管理的智能化水平，图书馆希望建立一个基于Spark的借阅行为分析系统，
帮助管理员了解学生借阅习惯、热门书籍等。

项目目标1.	从HDFS中读取图书借阅数据；
2.	利用Spark进行数据分析，包括：
	每个学生的借阅总数与平均借阅周期；
	热门图书排行（按借阅次数）；
	每月借阅趋势分析；
3.	将分析结果写入HBase，供其他系统实时查询；
•4.输出结果可选写入CSV文件便于进一步可视化。

设计思路清晰：1.采用RPC技术进行服务拆分。Python程序通过RPC和HBase等通信
分析逻辑：
项目结构图


SparkProjects/
├── artifacts/
│   └── spark-xxx/        # 一些 Spark 运行相关的临时文件夹
├── data/
│   └── borrow_log_sample.csv       # 源数据文件
├── output/
│   ├── hot_books.csv               # 热门图书分析结果
│   ├── monthly_trend.csv          # 月度借阅趋势结果
│   └── student_borrow.csv         # 学生借阅统计结果
├── hbase_writer.py                # 写入 HBase 的封装模块
├── main.py                        # 主程序入口
├── spark_analysis.py             # Spark 分析任务逻辑
├──  hotStudent.py                 #学生分析
├──book_plot.py                     #可视化
├── requirements.txt              #依赖库
├── README.md                      # 项目说明


技术选型：hadoop-3.3.6 ,hbase-2.4.18,spark-3.5.0 jdk1.8
运行环境 :CentOS7 ，Windows11
Python依赖库：
Package         Version
--------------- -----------
contourpy       1.3.2
cycler          0.12.1
Cython          3.1.2
fonttools       4.58.4
happybase       1.2.0
kiwisolver      1.4.8
matplotlib      3.10.3
numpy           2.3.0
packaging       25.0
pandas          2.3.0
pillow          11.2.1
pip             25.1.1
ply             3.11
py4j            0.10.9.9
pyarrow         20.0.0
pyparsing       3.2.3
pyspark         4.0.0
python-dateutil 2.9.0.post0
pytz            2025.2
setuptools      65.5.1
six             1.17.0
thriftpy2       0.5.2
tzdata          2025.2
wheel           0.38.4


架构图：
项目流程架构图
============

1. 借阅日志文件（CSV）
   ↓
2. 存储至 HDFS
   ↓
3. Spark 程序运行 Driver
   ↓
4. 数据清洗与聚合分析
   ↓ ↓ ↓
 ┌──────────┬──────────┬──────────┐
 │ 学生分析 │ 图书分析 │ 阅读趋势 │
 └──────────┴──────────┴──────────┘
              ↓
           写入 HBase


