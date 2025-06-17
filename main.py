from pyspark.sql import SparkSession
from spark_analysis import load_and_clean_data, analyze_student_borrow, analyze_hot_books, analyze_monthly_trend
from hbase_writer import write_to_hbase


def main():
    spark = SparkSession.builder.appName("LibraryBorrowAnalysis").getOrCreate()

    hdfs_path = "hdfs://100.75.29.25:9001/data/library_borrow_logs/borrow_log.csv"
    df = load_and_clean_data(spark, hdfs_path)

    student_df = analyze_student_borrow(df)
    hot_books_df = analyze_hot_books(df)
    monthly_df = analyze_monthly_trend(df)

    write_to_hbase("borrow_statistics", "student", student_df, ["borrow_count", "avg_duration"])
    write_to_hbase("borrow_statistics", "book", hot_books_df, ["borrow_times"])
    write_to_hbase("borrow_statistics", "month", monthly_df, ["total_borrowed"])

    spark.stop()


if __name__ == "__main__":
    main()
