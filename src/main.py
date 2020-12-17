from pyspark.sql import SparkSession
from dbconnect import _check_is_databricks, _get_dbutils, _get_display


def create_dataframe():
    spark = SparkSession.builder.appName("Dbconnect test").getOrCreate()

    df = spark.createDataFrame([('Fiji Apple', 'Red', 3.5),
                                ('Banana', 'Yellow', 1.0),
                                ('Green Grape', 'Green', 2.0),
                                ('Red Grape', 'Red', 2.0),
                                ('Peach', 'Yellow', 3.0),
                                ('Orange', 'Orange', 2.0),
                                ('Green Apple', 'Green', 2.5)],
                               ['Fruit', 'Color', 'Price'])
    #df.show()
    return df


if __name__ == "__main__":
    if _check_is_databricks():
        print("Databricks")
    else:
        print("No Databricks")
    item = _get_dbutils(
        SparkSession.builder.appName("Dbconnect test").getOrCreate())
    print(item)
    items = item.fs.ls("/")
    print(items)
    mydf = create_dataframe()
    display = _get_display()
    display(mydf)
    print("Done!")