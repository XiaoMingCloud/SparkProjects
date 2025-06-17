from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ActiveStudentAnalysis").getOrCreate()


borrow_df = spark.read.csv("hdfs://100.75.29.25:9001/data/library_borrow_logs/borrow_log.csv", header=True, inferSchema=True)
borrow_df.createOrReplaceTempView("borrow_log")

# 查询借阅次数最多的前 10 名学生
active_students = spark.sql("""
    SELECT student_id, COUNT(*) AS borrow_count
    FROM borrow_log
    GROUP BY student_id
    ORDER BY borrow_count DESC
    LIMIT 10
""")

active_students.show()

