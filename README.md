# PYSPARK
![img1]([https://fr.web.img6.acsta.net/r_654_368/newsv7/17/11/07/17/59/1720477.jpg](https://www.filmsnewsfeed.com/images/article/will-billy-hargrove-return-in-stranger-things-season-4-main.webp))
## Load data from PostgreSQL in Spark

```py
import psycopg2
import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

appName = "PySpark PostgreSQL Example - via psycopg2"
master = "local"

spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

engine = create_engine(
    "postgresql+psycopg2://postgres:password@localhost/testdb?client_encoding=utf8")
pdf = pd.read_sql('select * from test_table', engine)

# Convert Pandas dataframe to spark DataFrame
df = spark.createDataFrame(pdf)
print(df.schema)
df.show()
```


### Create pd_temp
pd_temp = pd.DataFrame(np.random.random(10))

### Create spark_temp from pd_temp
spark_temp = spark.createDataFrame(pd_temp)

### Examine the tables in the catalog
print(spark.catalog.listTables())

### Add spark_temp to the catalog
spark_temp.createOrReplaceTempView("temp")

### Examine the tables in the catalog again
print(spark.catalog.listTables())
