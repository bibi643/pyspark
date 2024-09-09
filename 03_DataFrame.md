# Introduction

RDD is not optimised to work on columns or perform machine learning tasks. That is why we use the structure **Data Frame**. This structure is similar to dataframes in Pandas.


# Spark SQL

Spark SQL is the module to work on **structured datas**. Spark SQL is added to Spark and introduce a new element called **SparkSession** which was at the beginning the entry point to Spark SQL but now it is unified to Spark.

```python
# Import de Spark Session et SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext

# Définition d'un SparkContext
SparkContext.getOrCreate() 

# Définition d'une SparkSession
spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Introduction au DataFrame") \
    .getOrCreate()
    
spark
```
- appName : Gives a name to the Session
- getOrCreate: to use an already open session or to create a new one.


# Create a Data Frame Spark.

We can create a df from 
- RDD
- csv file

1- From a RDD
