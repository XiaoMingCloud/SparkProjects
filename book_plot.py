from pyspark.sql.functions import to_date, date_format
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ActiveStudentAnalysis").getOrCreate()
# 生成年月字段
borrow_df = spark.read.csv("hdfs://100.75.29.25:9001/data/library_borrow_logs/borrow_log.csv", header=True, inferSchema=True)
borrow_df = borrow_df.withColumn("month", date_format(to_date(borrow_df["borrow_time"]), "yyyy-MM"))

# 按月聚合统计
monthly_trend = borrow_df.groupBy("month").count().orderBy("month")
monthly_pd = monthly_trend.toPandas()

# matplotlib 绘图
plt.figure(figsize=(10, 6))
plt.plot(monthly_pd["month"], monthly_pd["count"], marker='o')
plt.title("Monthly Borrowing Trend")
plt.xlabel("Month")
plt.ylabel("Borrow Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("monthly_trend.png")

