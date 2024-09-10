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


# Explore and Manipulate DataFrame
**Recall that the dataframe struture is coming from Spark SQL** so that is why eveything what is coming next is similar to SQL!!

## Select variable/columns

- new_df = df.select('name','age')

  ```python
  flights1 = raw_df.select('annee','mois','jours','flightNum','origin','dest','distance','canceled','cancellationCode','carrierDelay')
  flights1.take(20)
  flights.show(20)

  >>>
  +-----+----+-----+---------+------+----+--------+--------+----------------+------------+
|annee|mois|jours|flightNum|origin|dest|distance|canceled|cancellationCode|carrierDelay|
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+
| 2008|   1|    1|      324|   SEA| SJC|     697|       0|            null|          NA|
| 2008|   1|    1|      572|   SEA| PSP|     987|       0|            null|          NA|
| 2008|   1|    1|      511|   SAN| SEA|    1050|       0|            null|           0|
| 2008|   1|    1|      376|   SEA| GEG|     224|       0|            null|          NA|
| 2008|   1|    1|      729|   TUS| SEA|    1216|       0|            null|          NA|
| 2008|   1|    1|      283|   LAX| SEA|     954|       0|            null|          NA|
| 2008|   1|    1|      211|   LAX| SEA|     954|       0|            null|          NA|
| 2008|   1|    1|      100|   ANC| PDX|    1542|       0|            null|           0|
| 2008|   1|    1|      665|   LAS| SEA|     866|       0|            null|          NA|
| 2008|   1|    1|      531|   SJC| SEA|     697|       0|            null|          NA|
| 2008|   1|    1|      571|   SEA| DEN|    1024|       0|            null|          22|
| 2008|   1|    1|      154|   ANC| SEA|    1449|       1|               A|          NA|
| 2008|   1|    1|      728|   SEA| TUS|    1216|       0|            null|          NA|
| 2008|   1|    1|      518|   SEA| SAN|    1050|       0|            null|          92|
| 2008|   1|    1|      580|   SEA| SAN|    1050|       0|            null|          21|
| 2008|   1|    1|       85|   SEA| ANC|    1449|       0|            null|          NA|
| 2008|   1|    1|      640|   SEA| LAS|     866|       0|            null|          NA|
| 2008|   1|    1|      292|   SEA| LAX|     954|       0|            null|           0|
| 2008|   1|    1|      478|   SEA| PSP|     987|       0|            null|          15|
| 2008|   1|    1|      485|   LAX| SEA|     954|       0|            null|          NA|
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+
  '''
  
- The **attribute** columns returns a list of the variables/columns.
```python
print(flights.columns)
>>>
['annee',
 'mois',
 'jours',
 'flightNum',
 'origin',
 'dest',
 'distance',
 'canceled',
 'cancellationCode',
 'carrierDelay']
```


- The **structure** Columns is obtained typing the name of the column as an attribute.
```python
flights.annee
flights.jours

new_df = flights.select(flights.origin, flights.dest)
```
This way to select columns is interesting specially for the _method_ cast of the Column _structure. With **cast** we can easliy change the column data type, as we saw earlier that spark is lazy and does not put the right type to the columns.
- Example
```python
new_df = df.select(df.name.cast("string"),df.age.cast("int"))
```

```python
# Création d'un DataFrame en spécifiant le type des colonnes
flights = raw_df.select(raw_df.annee.cast("int"),
                        raw_df.mois.cast("int"),
                        raw_df.jours.cast("int"),
                        raw_df.flightNum.cast("int"),
                        raw_df.origin.cast("string"),
                        raw_df.dest.cast("string"),
                        raw_df.distance.cast("int"),
                        raw_df.canceled.cast("boolean"),
                        raw_df.cancellationCode.cast("string"),
                        raw_df.carrierDelay.cast("int"))

# Affichage de 20 premières lignes
flights.show()
>>>
-----+----+-----+---------+------+----+--------+--------+----------------+------------+
|annee|mois|jours|flightNum|origin|dest|distance|canceled|cancellationCode|carrierDelay|
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+
| 2008|   1|    1|      324|   SEA| SJC|     697|   false|            null|        null|
| 2008|   1|    1|      572|   SEA| PSP|     987|   false|            null|        null|
| 2008|   1|    1|      511|   SAN| SEA|    1050|   false|            null|           0|
| 2008|   1|    1|      376|   SEA| GEG|     224|   false|            null|        null|
| 2008|   1|    1|      729|   TUS| SEA|    1216|   false|            null|        null|
| 2008|   1|    1|      283|   LAX| SEA|     954|   false|            null|        null|
| 2008|   1|    1|      211|   LAX| SEA|     954|   false|            null|        null|
| 2008|   1|    1|      100|   ANC| PDX|    1542|   false|            null|           0|
| 2008|   1|    1|      665|   LAS| SEA|     866|   false|            null|        null|
| 2008|   1|    1|      531|   SJC| SEA|     697|   false|            null|        null|
| 2008|   1|    1|      571|   SEA| DEN|    1024|   false|            null|          22|
| 2008|   1|    1|      154|   ANC| SEA|    1449|    true|               A|        null|
| 2008|   1|    1|      728|   SEA| TUS|    1216|   false|            null|        null|
| 2008|   1|    1|      518|   SEA| SAN|    1050|   false|            null|          92|
| 2008|   1|    1|      580|   SEA| SAN|    1050|   false|            null|          21|
| 2008|   1|    1|       85|   SEA| ANC|    1449|   false|            null|        null|
| 2008|   1|    1|      640|   SEA| LAS|     866|   false|            null|        null|
| 2008|   1|    1|      292|   SEA| LAX|     954|   false|            null|           0|
| 2008|   1|    1|      478|   SEA| PSP|     987|   false|            null|          15|
| 2008|   1|    1|      485|   LAX| SEA|     954|   false|            null|        null|
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+
only showing top 20 rows
```
## Count and distinct
Like in SQL, we can combine count and distint to count the number of lines and filter duplicated results.
- count(): count the number of lines
- distinct(): applied to a variable filters the duplicates.
Here for example we will count the number of different flights.

```python
flights.select('flightNum').distinct().count()
```

## Describe
Return a summary of the dataframe. we need to use **show()** to display the df. **show()** has an _option_ truncate=n, where n is the nth characters to truncate.

```python
flights.describe().show(truncate=3)

>>>
|summary|annee|mois|jours|flightNum|origin|dest|distance|cancellationCode|carrierDelay|
+-------+-----+----+-----+---------+------+----+--------+----------------+------------+
|    cou|  151| 151|  151|      151|   151| 151|     151|             213|         301|
|    mea|  200| 6.4|  15.|      336|   NUL| NUL|     957|             NUL|         15.|
|    std|  0.0| 3.3|  8.7|      235|   NUL| NUL|     598|             NUL|         39.|
|    min|  200|   1|    1|        1|   ADK| ADK|      31|               A|           0|
|    max|  200|  12|   31|      997|   YAK| YAK|     284|               C|         947|
+-------+-----+----+-----+---------+------+----+--------+----------------+------------+
```
**Tips**: for this kind of small dataframe we can use the method **toPandas** to obtain a pandas type df, so we don't spend so much time on truncate. On big df it is better **NOT TO** use toPandas() as it can affect the distribution of the datas.

```python
flights.describe().toPandas()

```
## Categorical data
We saw that the column 'cancelled' is not returned in describe. Moreover, describe gives few information about categorical datas.
As a summary for this type of datas it is better to display the frequence of each modality, using **groupBY**.
**groupby** can group data by variables and then apply to it a function/transformation like count().

```python
flights.groupBy('cancellationCode').count().show()

>>> +----------------+------+
|CancellationCode| count|
+----------------+------+
|            NULL|148963|
|               B|   865|
|               C|    52|
|               A|  1222|

```

```python
flights.groupBy('cancellationCode','canceled').count().show()

>>>
+----------------+--------+------+
|cancellationCode|canceled| count|
+----------------+--------+------+
|               A|    true|  1222|
|               C|    true|    52|
|               B|    true|   865|
|            NULL|   false|148963|
+----------------+--------+------+

```
## Filter
Another way to select variables is to use the **method** filter().
**filter()** can select the data follwing a condition.
example:
df.filter(condition)
df.filter(df.age<20) will return a df containing only people younger than 20.

```python
flights.filter(flights.cancellationCode == 'C').show()
>>>
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+
|annee|mois|jours|flightNum|origin|dest|distance|canceled|cancellationCode|carrierDelay|
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+
| 2008|   1|    5|      345|   SFO| PDX|     550|    true|               C|        NULL|
| 2008|   1|    8|      345|   SFO| PDX|     550|    true|               C|        NULL|
| 2008|   1|    8|      526|   SEA| SFO|     679|    true|               C|        NULL|
| 2008|   1|   10|      526|   SEA| SFO|     679|    true|               C|        NULL|
| 2008|   1|   10|      345|   SFO| PDX|     550|    true|               C|        NULL|
| 2008|   1|   11|      345|   SFO| PDX|     550|    true|               C|        NULL|
| 2008|   1|   11|      526|   SEA| SFO|     679|    true|               C|        NULL|
| 2008|   1|   22|      526|   SEA| SFO|     679|    true|               C|        NULL|
| 2008|   1|   22|      345|   SFO| PDX|     550|    true|               C|        NULL|
| 2008|   1|   25|      341|   SFO| PDX|     550|    true|               C|        NULL|
| 2008|   1|   25|      378|   SFO| PSP|     421|    true|               C|        NULL|
| 2008|   1|   25|      310|   PDX| SFO|     550|    true|               C|        NULL|
| 2008|   1|   29|      341|   SFO| PDX|     550|    true|               C|        NULL|
| 2008|   1|   29|       20|   SEA| ORD|    1721|    true|               C|        NULL|
| 2008|   1|   29|       23|   ORD| SEA|    1721|    true|               C|        NULL|
| 2008|   1|   31|      345|   SFO| PDX|     550|    true|               C|        NULL|
| 2008|   1|   31|       22|   SEA| ORD|    1721|    true|               C|        NULL|
| 2008|   1|   31|      131|   ORD| ANC|    2846|    true|               C|        NULL|
| 2008|   1|   31|       28|   SEA| ORD|    1721|    true|               C|        NULL|
| 2008|   1|   31|       29|   ORD| SEA|    1721|    true|               C|        NULL|
+-----+----+-----+---------+------

```
Example:
What is the month with more cancellations?
```python
flights.filter(flights.cancelled == True).groupBy('mois').count().show()


mois|count|
+----+-----+
|  12|  627|
|   1|  355|
|   6|  104|
|   3|   85|
|   5|  127|
|   9|   67|
|   4|  158|
|   8|  154|
|   7|   98|
|  10|   93|
|  11|   65|
|   2|  206|
+----+-----+
```

# Creation and aggregation of variables.

The _method_ **withColumn** creates a new column.

**General Syntax and Examples**:
- df.withColumn('New_column_name', operation_to_do)
- df.withColumn('ageInMonth', df.age *12)
- df.withColumn('isMinor', df.age <18) # Booleen showing if a person is minor or not.

Let's create a new column returning true if the flight is longer than 1000 miles.
```python
flights.withColumn('IsLong', flights.distance > 1000)
flight.show(10)
>>>
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+------------+
|annee|mois|jours|flightNum|origin|dest|distance|canceled|cancellationCode|carrierDelay|isLongFlight|
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+------------+
| 2008|   1|    1|      324|   SEA| SJC|     697|   false|            NULL|        NULL|       false|
| 2008|   1|    1|      572|   SEA| PSP|     987|   false|            NULL|        NULL|       false|
| 2008|   1|    1|      511|   SAN| SEA|    1050|   false|            NULL|           0|        true|
| 2008|   1|    1|      376|   SEA| GEG|     224|   false|            NULL|        NULL|       false|
| 2008|   1|    1|      729|   TUS| SEA|    1216|   false|            NULL|        NULL|        true|
| 2008|   1|    1|      283|   LAX| SEA|     954|   false|            NULL|        NULL|       false|
| 2008|   1|    1|      211|   LAX| SEA|     954|   false|            NULL|        NULL|       false|
| 2008|   1|    1|      100|   ANC| PDX|    1542|   false|            NULL|           0|        true|
| 2008|   1|    1|      665|   LAS| SEA|     866|   false|            NULL|        NULL|       false|
| 2008|   1|    1|      531|   SJC| SEA|     697|   false|            NULL|        NULL|       false|
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+-------

```
 > L'enregistrement de la nouvelle colonne ne s'effectue nulle part. A cause du caractère immuable, aucune modification ne se fait par remplacement (_in place_). Pour enregistrer une nouvelle variable, il faut créer un nouvel objet ou de la créer dès la création du DataFrame. 

 
# Missing Values

Like in Pandas we have function like **dropna and fillna**. Missing valies in spark are called **null**.

**General Syntax and exmaples**
- df.fillna(newvalue, 'column_Name')
- df.fillna('unknown', 'name')
- df.fillna('23,'age')

Let's subsitute the missing values in carrierDelay by 0.

```python
flights.fillna(0, 'carrierDelay').show(10)

>>>
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+
|annee|mois|jours|flightNum|origin|dest|distance|canceled|cancellationCode|carrierDelay|
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+
| 2008|   1|    1|      324|   SEA| SJC|     697|   false|            NULL|           0|
| 2008|   1|    1|      572|   SEA| PSP|     987|   false|            NULL|           0|
| 2008|   1|    1|      511|   SAN| SEA|    1050|   false|            NULL|           0|
| 2008|   1|    1|      376|   SEA| GEG|     224|   false|            NULL|           0|
| 2008|   1|    1|      729|   TUS| SEA|    1216|   false|            NULL|           0|
| 2008|   1|    1|      283|   LAX| SEA|     954|   false|            NULL|           0|
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+
only showing top 6 rows

```
# Replace
Replacing any value by a new one is also possible using the method **replace()**.

**Syntax and examples**
- df.replace(oldValue, newValue)

- df.replace(oldvalue, newValue, ColumnName)
- df.replace([oldvalue1, oldvalue2],[newvalue1, newvalue2],'columnName')

Let's replace A, B, C by 1, 2, 3 in the cancellation code column.

```python
flights.replace(['A','B','C'],['1','2,','3'],'cancellatioCode')
>>>
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+
|annee|mois|jours|flightNum|origin|dest|distance|canceled|cancellationCode|carrierDelay|
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+
| 2008|   1|    1|      324|   SEA| SJC|     697|   false|            NULL|        NULL|
| 2008|   1|    1|      572|   SEA| PSP|     987|   false|            NULL|        NULL|
| 2008|   1|    1|      511|   SAN| SEA|    1050|   false|            NULL|           0|
| 2008|   1|    1|      376|   SEA| GEG|     224|   false|            NULL|        NULL|
| 2008|   1|    1|      729|   TUS| SEA|    1216|   false|            NULL|        NULL|
| 2008|   1|    1|      283|   LAX| SEA|     954|   false|            NULL|        NULL|
| 2008|   1|    1|      211|   LAX| SEA|     954|   false|            NULL|        NULL|
| 2008|   1|    1|      100|   ANC| PDX|    1542|   false|            NULL|           0|
| 2008|   1|    1|      665|   LAS| SEA|     866|   false|            NULL|        NULL|
| 2008|   1|    1|      531|   SJC| SEA|     697|   false|            NULL|        NULL|
| 2008|   1|    1|      571|   SEA| DEN|    1024|   false|            NULL|          22|
| 2008|   1|    1|      154|   ANC| SEA|    1449|    true|               1|        NULL|
| 2008|   1|    1|      728|   SEA| TUS|    1216|   false|            NULL|        NULL|
| 2008|   1|    1|      518|   SEA| SAN|    1050|   false|            NULL|          92|
| 2008|   1|    1|      580|   SEA| SAN|    1050|   false|            NULL|          21|
| 2008|   1|    1|       85|   SEA| ANC|    1449|   false|            NULL|        NULL|
| 2008|   1|    1|      640|   SEA| LAS|     866|   false|            NULL|        NULL|
| 2008|   1|    1|      292|   SEA| LAX|     954|   false|            NULL|           0|
| 2008|   1|    1|      478|   SEA| PSP|     987|   false|            NULL|          15|
| 2008|   1|    1|      485|   LAX| SEA|     954|   false|            NULL|        NULL|
+-----+----+-----+---------+------+----+--------+--------+----------------+------------+

```

# Order by
Orderby works like the SQL function. Applied to a df, it sorts the values of one of the variable/Column.

**General Syntax and Exmaples**

df.orderBy(df.age)

df.orderBy(df.age.desc())

```python
flights.orderBy(flights.flightNum.desc()).show()
```

# SQL Queries
We can use sql queries, but it wouild be much more longer and so we lost the thing of Sparks!!


    La première étape consiste à créer une vue SQL (SQL view), référencée dans le code SQL grâce à la méthode createOrReplaceTempView.

    Exemple : Création d'une vue et utilisation de la méthode SQL pour envoyer une requête

        df.createOrReplaceTempView("people")
        sqlDF = spark.sql("SELECT * FROM people")

    (a) Créer une vue SQL de flights que l'on appellera "flightsView".

    (b) Créer un DataFrame appelé sqlDF contenant uniquement la variable carrierDelay grâce à une requête SQL.

    (c) Afficher les premières lignes de sqlDF.

    ```python
    flights.createOrReplaceTempView('flightsView')
    sqlDF = spark.sql(" SELECT carrierDelay FROM flightsView')
    sqlDF.show()
    ```
    

# Sample and tips

We saw earlier methods to display like
- take(n)
- show(n)
- toPandas()
each ones having advantages and inconvenients.

**sample(withReplacement= _Booleen_, fraction = fraction_to_keep, seed = make ramdoness reproducible)**

df.sample(False, 0.01, seed =1234)

```python
flights.sample(False, .001, seed= 1234).toPandas()
```

Note: It can be combined with toPandas().



# Close

As always close the spark session
**spark.stop()**
