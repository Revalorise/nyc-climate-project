import duckdb

table = duckdb.read_csv("../data/shop_activity.csv")
table.show()
