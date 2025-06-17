
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, datediff, substring
# 读取与清洗数据
def load_and_clean_data(spark, hdfs_path):
    df = spark.read.csv(hdfs_path, header=True)
    df = df.withColumn("borrow_time", to_date("borrow_time", "yyyy-MM-dd")) \
           .withColumn("return_time", to_date("return_time", "yyyy-MM-dd")) \
           .filter(col("borrow_time").isNotNull() & col("return_time").isNotNull()) \
           .withColumn("borrow_duration", datediff("return_time", "borrow_time")) \
           .withColumn("month", substring("borrow_time", 1, 7))
    return df


# A. 学生借阅统计
def analyze_student_borrow(df):

    student_borrow=df.groupBy("student_id") \
             .agg({"book_id": "count", "borrow_duration": "avg"}) \
             .withColumnRenamed("count(book_id)", "borrow_count") \
             .withColumnRenamed("avg(borrow_duration)", "avg_duration")
    student_borrow.toPandas().to_csv("output/student_borrow.csv", index=False)
    return student_borrow


# 热门图书统计（Top 10）
def analyze_hot_books(df):
    hot_books = df.groupBy("book_id") \
                  .count() \
                  .withColumnRenamed("count", "borrow_times") \
                  .orderBy("borrow_times", ascending=False) \
                  .limit(10)
    hot_books.toPandas().to_csv("output/hot_books.csv", index=False)
    return hot_books

# 月度借阅趋势
def analyze_monthly_trend(df):
    monthly_trend=df.groupBy("month") \
             .count() \
             .withColumnRenamed("count", "total_borrowed")
    monthly_trend.toPandas().to_csv("output/monthly_trend.csv", index=False)
    return monthly_trend

