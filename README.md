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
#### Another way

Download the pilote JDBC PostgreSQL : https://jdbc.postgresql.org/download.html

```py
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/path_to_postgresDriver/postgresql-42.2.5.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/databasename") \
    .option("dbtable", "tablename") \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()
```

Create pd_temp
```py
pd_temp = pd.DataFrame(np.random.random(10))
```
Create spark_temp from pd_temp
```py
spark_temp = spark.createDataFrame(pd_temp)
```
Examine the tables in the catalog
```py
print(spark.catalog.listTables())
```
Add spark_temp to the catalog
```py
spark_temp.createOrReplaceTempView("temp")
```
Examine the tables in the catalog again
```py
print(spark.catalog.listTables())
```
### Example

```py
# Don't change this file path
file_path = "/usr/local/share/datasets/players.csv"

# Read in the airports data
players = spark.read.csv(file_path, header=True)

# Show the data
players.show()

# Create the DataFrame flights
flights = spark.table("flights")

# Show the head
flights.show()

# Add duration_hrs
flights = flights.withColumn("duration_hrs", flights.air_time/60)
```
### Filter 

```py
# Filter flights by passing a string
long_flights1 = flights.filter("distance > 1000")

# Filter flights by passing a column of boolean values
long_flights2 = flights.filter(flights.distance >1000)

# Print the data to check they're equal
long_flights1.show()
long_flights2.show()
```
### Filter Select 
```py
# Select the first set of columns
selected1 = flights.select("tailnum", "origin", "dest")

# Select the second set of columns
temp = flights.select(flights.origin, flights.dest, flights.carrier)

# Define first filter
filterA = flights.origin == "SEA"

# Define second filter
filterB = flights.dest == "PDX"

# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)
```

### Alias Select

```py
# Define avg_speed
avg_speed = (flights.distance/(flights.air_time/60)).alias("avg_speed")

# Select the correct columns
speed1 = flights.select("origin", "dest", "tailnum", avg_speed)

# Create the same table using a SQL expression
speed2 = flights.selectExpr("origin", "dest", "tailnum", "distance/(air_time/60) as avg_speed")
```

### Filter GroupBy 

```py
# Find the shortest flight from PDX in terms of distance
flights.filter(flights.origin == "PDX").groupBy().min("distance").show()

# Find the longest flight from SEA in terms of air time
flights.filter(flights.origin == "SEA").groupBy().max("air_time").show()
```

### Count

```py
# Group by tailnum
by_plane = flights.groupBy("tailnum")

# Number of flights each plane made
by_plane.count().show()

# Group by origin
by_origin = flights.groupBy("origin")

# Average duration of flights from PDX and SEA
by_origin.avg("air_time").show()
```

### F.stddev

```py
# Import pyspark.sql.functions as F
import pyspark.sql.functions as F

# Group by month and dest
by_month_dest = flights.groupBy("month","dest")

# Average departure delay by month and destination
by_month_dest.avg("dep_delay").show()

# Standard deviation of departure delay
by_month_dest.agg(F.stddev("dep_delay")).show()
```

### Join

```py
# Examine the data
airports.show()

# Rename the faa column
airports = airports.withColumnRenamed("faa", "dest")

# Join the DataFrames
flights_with_airports = flights.join(airports, on="dest", how="leftouter")

# Examine the new DataFrame
flights_with_airports.show()
```


### Convert

```py
# Create is_late
model_data = model_data.withColumn("is_late", model_data.arr_delay > 0)

# Convert to an integer
model_data = model_data.withColumn("label", model_data.is_late.cast("integer"))

# Remove missing values
model_data = model_data.filter("arr_delay is not NULL and dep_delay is not NULL and air_time is not NULL and plane_year is not NULL")

```

```py
# Create a StringIndexer
carr_indexer = StringIndexer(outputCol="carrier_index",inputCol="carrier")

# Create a OneHotEncoder
carr_encoder = OneHotEncoder(outputCol="carrier_fact",inputCol="carrier_index")
```

```py
# Create a StringIndexer
dest_indexer = StringIndexer(inputCol="dest",outputCol="dest_index")

# Create a OneHotEncoder
dest_encoder = OneHotEncoder(inputCol="dest_index",outputCol="dest_fact")
```

```py
# Make a VectorAssembler
vec_assembler = VectorAssembler(inputCols=["month", "air_time", "carrier_fact", "dest_fact", "plane_age"], outputCol="features")
```

## Create the pipeline

```py
# Import Pipeline
from pyspark.ml import Pipeline

# Make the pipeline
flights_pipe = Pipeline(stages=[dest_indexer, dest_encoder, carr_indexer, carr_encoder, vec_assembler])
```

## Transform the data

```py
# Fit and transform the data
piped_data = flights_pipe.fit(model_data).transform(model_data)
```

## Split the data

```py
# Split the data into training and test sets
training, test = piped_data.randomSplit([.6, .4])
```

## Create the modeler
```py
# Import LogisticRegression
from pyspark.ml.classification import LogisticRegression

# Create a LogisticRegression Estimator
lr = LogisticRegression()
```
## Cross validation

### Create the evaluator

```py
# Import the evaluation submodule
import pyspark.ml.evaluation as evals

# Create a BinaryClassificationEvaluator
evaluator = evals.BinaryClassificationEvaluator(metricName="areaUnderROC")
```

### Make a grid

```py
# Import the tuning submodule
import pyspark.ml.tuning as tune

# Create the parameter grid
grid = tune.ParamGridBuilder()

# Add the hyperparameter
grid = grid.addGrid(lr.regParam, np.arange(0, .1, .01))
grid = grid.addGrid(lr.elasticNetParam, [0, 1])

# Build the grid
grid = grid.build()
```

### Make the validator

```py
# Create the CrossValidator
cv = tune.CrossValidator(estimator=lr,
               estimatorParamMaps=grid,
               evaluator=evaluator
               )
```

### Fit the model(s)

```py
# Fit cross validation models
models = cv.fit(training)

# Extract the best model
best_lr = models.bestModel

# Call lr.fit()
best_lr = lr.fit(training)

# Print best_lr
print(best_lr)
```

### Evaluating binary classifiers(ROC)

### Evaluate the model

```py
# Use the model to predict the test set
test_results = best_lr.transform(test)

# Evaluate the predictions
print(evaluator.evaluate(test_results))
```

```py
# Print the version of SparkContext
print("The version of Spark Context in the PySpark shell is", sc.version)

# Print the Python version of SparkContext
print("The Python version of Spark Context in the PySpark shell is", sc.pythonVer)

# Print the master of SparkContext
print("The master of Spark Context in the PySpark shell is", sc.master)
```

```py
# Create a Python list of numbers from 1 to 100 
numb = range(1,101)

# Load the list into PySpark  
spark_data = sc.parallelize(numb)
```

```py
# Load a local file into PySpark shell
lines = sc.textFile(file_path)
```
