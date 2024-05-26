import duckdb

table = duckdb.read_csv("../stream_data/shop_activity.csv")
table.show()
