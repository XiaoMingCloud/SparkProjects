
import happybase
# 写入 HBase
def write_to_hbase(table_name, rowkey_prefix, df, mapping):
    connection = happybase.Connection('100.75.29.25',port=9090)  # HBase IP
    table = connection.table(table_name)
    for row in df.collect():
        rowkey = f"{rowkey_prefix}:{row[0]}"
        data = {f"info:{col}": str(row[i+1]) for i, col in enumerate(mapping)}
        table.put(rowkey, data)
    connection.close()
