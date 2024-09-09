# Introduction

Open source framework to work on massive datasets due to a distribution of the datas on different clusters dealing in parallel with a sample of the data.

# SparkContext

SparkContext is the object managing the connexions with the cluster Spark.

```python
# Import de SparkContext du module pyspark
from pyspark import SparkContext

# Définiton d'un SparkContext en local
sc = SparkContext('local')
sc

```

If we would like to send our code to production, simply change the SparkContext wich won't be local anymore.

> Use preferably  **SparkContext.getOrCreate()** because the use of sparkcontext twice generate an error.

# RDD (Resilient Distributed Data)

RDD is a table of datas where we can store tuples, lists, dictionnaries...
it is optimised for lines operations.

## Import data

To import/load data, we can use sc.textFile('path_to_file.ext')
```python
raw_rdd = sc.textFile('2008_raw.csv')
```

The efficency in Sparks is due to **lazy evaluation**, meaning it is not returning or performing any operations until it is necessary.
To see data we have to use methods such as take(n) to return n lines.

```python
print(raw_rdd.take(5))
>>>
['2008,1,1,2057,AS,324,N306AS,SEA,SJC,697,0,,0,NA,NA,NA,NA,NA', '2008,1,1,703,AS,572,N302AS,SEA,PSP,987,0,,0,NA,NA,NA,NA,NA', '2008,1,1,2011,AS,511,N564AS,SAN,SEA,1050,0,,0,0,0,0,0,63', '2008,1,1,2301,AS,376,N309AS,SEA,GEG,224,0,,0,NA,NA,NA,NA,NA', '2008,1,1,1221,AS,729,N317AS,TUS,SEA,1216,0,,0,NA,NA,NA,NA,NA']
```

We can see that the columns in RDD are not named. This is because RDD are for lines and not for columns data.
```python
raw_rdd.count() # to count the number of lines.
```

# Map and Reduce
Create a new rdd where each line is a list made of the element of each line.

```python
airplane_rdd= raw_rdd.map(lambda line:line.split(','))
airplane_rdd.take(1)

>>>
[['2008',
  '1',
  '1',
  '2057',
  'AS',
  '324',
  'N306AS',
  'SEA',
  'SJC',
  '697',
  '0',
  '',
  '0',
  'NA',
  'NA',
  'NA',
  'NA',
  'NA']]
```
Now we want to count how many flights we have on each airports -> key:value (airport:flights)
```python
# Insérez votre code ici 
hist_rdd= airplane_rdd.map(lambda line: (line[7],1)).reduceByKey(lambda x,y:x+y)

hist_rdd.take(5)
>>>
# Insérez votre code ici 

hist_rdd= airplane_rdd.map(lambda line: (line[7],1)).reduceByKey(lambda x,y:x+y)

​

hist_rdd.take(5)

>>>
[('SEA', 48134), ('SAN', 3958), ('TUS', 444), ('LAX', 7150), ('ANC', 15340)]

```
> De façon plus générale, les techniques de map et reduce permettent de résumer les données et s'effectuent généralement de la façon suivante :
• Créer un couple (key, value) sur chaque ligne grâce à map.
• Regrouper les clés grâce à reduceByKey en effectuant l'opération de notre choix sur les valeurs

# Collect Method to force evaluation

We saw that spark use lazy evaluation and we need function such as take(n) to show some results.
To show the whole variable we use **collect()**

```python
# Insérez votre code ici 

hist=hist_rdd.collect()
hist
>>>
[('SEA', 48134),
 ('SAN', 3958),
 ('TUS', 444),
 ('LAX', 7150),
 ('ANC', 15340),
 ('LAS', 4022),
 ('SJC', 3109),
 ('DFW', 1083),
 ('DEN', 2221),
 ('SFO', 5062),
 ('GEG', 1704),
 ('SMF', 2019),
 ('JNU', 4410),
 ('PDX', 11262),
 ('KTN', 2380),
 ('SNA', 4838),
 ('ONT', 1403),
 ('PSP', 1755),
 ('ADQ', 706),
 ('FAI', 4537),
 ('SIT', 1332),
 ('OAK', 3342),
 ('ORD', 1459),
 ('BUR', 1450),
 ('LGB', 1071),
 ('BOI', 75),
 ('PHX', 3252),
 ('MCO', 968),
 ('HNL', 602),
 ('LIH', 366),
 ('BOS', 984),
 ('RNO', 112),
 ('PSG', 727),
 ('WRG', 727),
 ('EWR', 731),
 ('SCC', 727),
 ('BET', 1035),
 ('CDV', 725),
 ('OME', 1090),
 ('YAK', 725),
 ('BRW', 728),
 ('OTZ', 1086),
 ('DCA', 1093),
 ('MIA', 366),
 ('ADK', 102),
 ('DLG', 116),
 ('AKN', 116),
 ('GST', 85),
 ('OGG', 195),
 ('MSP', 133),
 ('KOA', 45)]
```

# Sort and Filter a RDD

We will sort the list by the value to have a better idea which aiport has more flights, using **sorted(list= list_to_sort, keys = list_to_use_as_ref_to_sort, reverse = 0 or 1)
ex:
**sorted(hist_rdd.collect(), key=lambda x: x[0])** -> sorts the airpots by alphabetic order.

```python
sorted(hist,keys=lambda x:x[1], reverse = 1)
```

We can also filter a rdd using filter. Now we will count by airport the number of flights cancelled.
- filter
- map and reduced by keys
- sort

```python
cancelled_rdd= airplane_rdd \
    .filter(lambda x:x[10]=='1') \
    .map(lambda line: (line[8],1)) \
    .reduceByKey(lambda x,y :x+y) \
    .collect()



sorted(cancelled_rdd,
      key=lambda x:x[1],
      reverse=1)
>>>
​

sorted(cancelled_rdd,

      key=lambda x:x[1],

      reverse=1)

[('SEA', 647),
 ('ANC', 334),
 ('PDX', 162),
 ('OAK', 90),
 ('SNA', 77),
 ('JNU', 67),
 ('LAX', 64),
 ('SFO', 58),
 ('FAI', 52),
 ('SJC', 48),
 ('ADQ', 39),
 ('KTN', 36),
 ('LAS', 31),
 ('SIT', 29),
 ('GEG', 28),
 ('SAN', 28),
 ('OTZ', 27),
 ('OME', 27),
 ('WRG', 26),
 ('BET', 26),
 ('PSG', 23),
 ('SMF', 21),
 ('YAK', 20),
 ('DEN', 18),
 ('PHX', 18),
 ('ORD', 15),
 ('PSP', 14),
 ('BUR', 14),
 ('ONT', 10),
 ('LGB', 10),
 ('CDV', 10),
 ('ADK', 10),
 ('SCC', 9),
 ('BRW', 9),
 ('DFW', 7),
 ('MCO', 6),
 ('EWR', 5),
 ('BOS', 4),
 ('DLG', 4),
 ('AKN', 2),
 ('GST', 2),
 ('MIA', 2),
 ('MSP', 2),
 ('DCA', 2),
 ('BOI', 1),
 ('TUS', 1),
 ('LIH', 1),
 ('KOA', 1),
 ('HNL', 1),
 ('OGG', 1)]
```



> **General Syntax**
>    Easier to read
    rdd \
      .filter(fun1) \
      .map(fun2) \
      .reduceByKey(fun3) \
      .collect()


# Close SparkContext

**sc.stop()**
