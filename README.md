# PYSPARK
![img1](https://i.gaw.to/vehicles/photos/40/28/402841-2022-chevrolet-spark.jpg?640x400)
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


Create pd_temp
```
pd_temp = pd.DataFrame(np.random.random(10))
```
Create spark_temp from pd_temp
```
spark_temp = spark.createDataFrame(pd_temp)
```
Examine the tables in the catalog
```
print(spark.catalog.listTables())
```
Add spark_temp to the catalog
```
spark_temp.createOrReplaceTempView("temp")
```
Examine the tables in the catalog again
```
print(spark.catalog.listTables())
```

![image2](https://user-images.githubusercontent.com/73175706/183451095-e4a49937-0263-4366-9505-a1383ac6633e.png)

