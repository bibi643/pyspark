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

## From a RDD
If we have a RDD with 2 elements on each line.
- name
- age

We can create a dataframe 
```python
rdd_row = rdd.map(lambda line : ROW(name = line[0],age = line[1])
df = spark.createDataFrame(rdd_row)
```

Lets go back to the flights rdd.

```python
from pyspark.sql import Row

 #Chargement du fichier '2008_raw.csv'
rdd = sc.textFile('2008_raw.csv').map(lambda line: line.split(","))

rdd_row = rdd.map(lambda line: Row(annee = line[0],
                                   mois = line[1],
                                   jours=line[2],
                                   flightNum=line[5]))

df=spark.createDataFrame(rdd_row)
df.show(5)
df.take(5)

+-----+----+-----+---------+
|annee|mois|jours|flightNum|
+-----+----+-----+---------+
| 2008|   1|    1|      324|
| 2008|   1|    1|      572|
| 2008|   1|    1|      511|
| 2008|   1|    1|      376|
| 2008|   1|    1|      729|
+-----+----+-----+---------+
only showing top 5 rows

[Row(annee='2008', mois='1', jours='1', flightNum='324'),
 Row(annee='2008', mois='1', jours='1', flightNum='572'),
 Row(annee='2008', mois='1', jours='1', flightNum='511'),
 Row(annee='2008', mois='1', jours='1', flightNum='376'),
 Row(annee='2008', mois='1', jours='1', flightNum='729')]

```

To display properly a dataframe in Spark it is better to use **df.show(n)**


## Creation from a csv
That is the most common option. 
_Note_: we use spark everywhere becasue this is the name of the session we choose first.
**spark.read.csv('file.ext' ,header=True)**

```python
raw_df = spark.read.csv('2008.csv',header=True)
```
To know the name of each column and the type of the datas, we can use **.printSchema()**. It is like info() in python.

```python
raw_df.printSchema()
>>>
root
 |-- annee: string (nullable = true)
 |-- mois: string (nullable = true)
 |-- jours: string (nullable = true)
 |-- heure: string (nullable = true)
 |-- uniqueCarrier: string (nullable = true)
 |-- flightNum: string (nullable = true)
 |-- tailNum: string (nullable = true)
 |-- origin: string (nullable = true)
 |-- dest: string (nullable = true)
 |-- distance: string (nullable = true)
 |-- canceled: string (nullable = true)
 |-- cancellationCode: string (nullable = true)
 |-- diverted: string (nullable = true)
 |-- carrierDelay: string (nullable = true)
 |-- weatherDelay: string (nullable = true)
 |-- nasDelay: string (nullable = true)
 |-- securityDelay: string (nullable = true)
 |-- lateAircraftDelay: string (nullable = true)

```
**WARNING**: printSchema is bs as you can see. It does not recognize properly the type of the data so we need to change it manually.




