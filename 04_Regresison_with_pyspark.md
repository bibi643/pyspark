# Introduction

Before we saw Spark SQL for dealing with dataframe structure, now let's dive into **Spark ML**. Spark ML can execute the majority of the ML algorithm but in a distributed way, so we gain in efficency.
First we will see a simple LR algorithm and later on more complex example.

# Building the SparkSession

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext

# Definition of sparkcontext locally

sc = SparkContext.getOrCreate()

# Building Spark Session

spark = SparkSession \
    .builder \
    .appName("Introduction to Spark ML") \
    .getOrCreate()

spark

```


# Import the data Base


The data base here is Year Prediction MSD. It contains characteristics of 515345 songs released between 1922 and 2011. It contains 91 variables/columns.
- One variable is the release year
- 12 variables is a proejction in 12D of the audio.
- 78 variables covariance of the audio.

The objectif is to predict the release year based on audio characteristics. For this we will implement a LR on the informations of the audio to predict the release year.


```python

df_raw = spark.read.csv('YearPredictionMSD.txt')
df_raw.show(5)

#or 
df_raw.sample(False, .00001, seed=124).toPandas()

```

Observe how .show() is horrible to read in this case due to the number of variables. That is why it is usually better to combine .sample().toPandas().


Also remember that the parsing (data type assignation) by pyspark is terrible. To check it use .printSchema().

```python
df_raw.printSchema()
```

To modify the type of the columns we could use the following:
```python
df_raw.select(df_raw._c0.cast('double'),
              df_raw._c1.cast('double'),
              df_raw._c2.cast(´double'),
              ...)
```

This can be super painful if the columns to change is huge like here.

Thanks to the function **col()** we can automate this. col() is from pyspark.sql.functions and needs to be imported.

- exprs = [col(c).cast('double') for c in df_raws.columns]
df = df_raw.select(*exprs)


```python
from pyspark.sql.functions import col
exprs = [col(c).cast('double') for c in df_raws.columns[1:91]]
df = df_raw.select(df_raw._c0.cast('int'),*exprs)
```

**Note**: Notice here how we combine the example when we were modifying all columns one by one. Here we change the first column into in, and the others to double.

To check we can perform a df.printSchema()



> **Checking the correct type** of the datas and **dealing with missing values** are super important tasks when the datas will be put into a ML algorithm.



# svmlib format

Previously one good practice is to **place the variable to predict as the first column of the df**.

To be used in sparkML the data base should be a dataframe made of 2 columns:
- label: variable to predict
- features: containing explicative variables

The function **DenseVector()** from the package pyspark.ml.linalg can group multiple variables into one, exactly what we want to do with the column features made of multiple ones.

To use DenseVector() we need to:
1- transform the df into rdd
2- use the method map()

_Example_:
```python
rdd_ml = df.rdd.map(lambda x: (x[0],DenseVector(x[1:]))), if we put the label column in first position as mentionned earlier.
df_ml = spark.createDataFrame(rdd_ml, ['label','features'])
```

```python
from pyspark.ml.linalg import DenseVector
rdd_ml = df.rdd.map(lambda x:(x[0], DenseVector(x[1:])))
df_ml = spark.createDataFrame(rdd_ml, ['label','features'])
df_ml.show()

+-----+--------------------+
|label|            features|
+-----+--------------------+
| 2001|[49.94357,21.4711...|
| 2001|[48.73215,18.4293...|
| 2001|[50.95714,31.8560...|
| 2001|[48.2475,-1.89837...|
| 2001|[50.9702,42.20998...|
| 2001|[50.54767,0.31568...|
| 2001|[50.57546,33.1784...|
| 2001|[48.26892,8.97526...|
| 2001|[49.75468,33.9958...|
| 2007|[45.17809,46.3423...|
+-----+--------------------+
```

# Split the data into train and test

To evaluate the performance of the model we need to separate some datas to certify the quality of the model once trained. We need to split into train and test the datas.

Usually the test set is between 15-30% of the set. To perform the split we use the method randomSplit().
```python
train,test = df.randomSplit([.7,.3], seed=122)
```

```python
train, test = df_ml.randomSplit([.8,.2], seed= 124)
```

# Lineal Regression


It can be function linear regression in the module **pyspark.ml.regression** named as _LinearRegression_. This function performs a linear regression distributed on different clusters in the SparkSession.

- Create the function and the parameters.
- Use the method fit to apply it to the datas.

_example_:

lr = LinearRegression(labelCol= 'label', featuresCol = 'features', maxIter = 10, regParam = 0.3)


```python
#Import the function
from pyspark.ml.regression import LinearRegression

# Create the model
lr= LinearRegression(labelCol = 'label', featuresCol = 'features', maxIter =10 , regParam = 0.3)

# Apply the model to the train set.
linearModel= lr.fit(train)
```


Once we trained the model on the train set, we can do **some Predictions** on the test set.
To this purpose, Spark ML has a method called **transform()** to make some predictions taking as argument the test set.

```python
predicted = linearModel.transform(test)
predicted.show()

+-----+--------------------+------------------+
|label|            features|        prediction|
+-----+--------------------+------------------+
| 1922|[40.96435,64.5129...| 1995.680624609109|
| 1922|[46.15136,66.0833...| 1998.014087259435|
| 1928|[31.04544,-226.47...|1995.2753028083687|
| 1928|[34.74756,-60.680...| 1986.909272322613|
| 1928|[35.49673,-230.03...|1998.2821083988053|
| 1928|[36.89361,-74.548...|1985.9837356850257|
| 1928|[37.09369,-119.24...|1997.3250174376587|
| 1929|[32.29746,-108.55...|1987.6390339731618|
| 1929|[37.26627,-54.121...|1986.9385433844573|
| 1929|[38.37679,-79.578...| 1990.594613818516|
| 1930|[20.82134,-105.06...|1979.1137062923792|
| 1930|[35.55615,-56.582...|1979.0963819341628|
| 1930|[35.57837,-73.831...|1980.5385668017377|
| 1930|[36.57854,-142.67...|1987.6212950448803|
| 1930|[39.41802,-73.499...|1984.8435178157122|
| 1931|[36.8584,-91.0008...|1984.9371174379403|
| 1932|[32.2233,-59.1997...|1984.7245233771953|
| 1932|[33.20162,-143.14...|1989.7056878717538|
| 1933|[35.52117,-173.08...| 1991.302754672393|
| 1936|[29.81987,-138.43...| 1994.535671682062|
+-----+--------------------+------------------+

```

# Evaluating the model

To evaluate the model, we can have a look to the attribut summary of the model.

**Type linearModel.summary then TAB to pop up all the different attributes of summary.**

```python
# Calcul et affichage du RMSE
print("RMSE:", linearModel.summary.rootMeanSquaredError)

# Calcul et affichage du R2
print("R2:  ", linearModel.summary.r2)
RMSE: 9.548411838648486
R2:   0.2363424607632092


```
R2 is bad so the Linear model is bad to indicate the decennies in which the song has been released.

To **optimise** a model in general we can:
- change the explicative variables
- change the whole model
- change the model parameters.


Once the model is fairly good we can access to the coefficients and intercept through those attributes.

```python
pprint(linearModel.coefficients)
pprint(linearModel.intercept)

``

# Close Session

**spark.stop()



# Summurize

   Aller plus loin - Autres algorithmes de régression

Maintenant que vous avez appris à programmer une régression linéaire en utilisant Spark ML, vous n'êtes qu'à quelques pas de maîtriser tout algorithme de régression distribué sous Spark. Pour vous aider à retenir l'essentiel, en voici un aperçu :

• 1. Transformer la base de données en format svmlib :
  • Sélectionner les variables numériques à utiliser pour la régression.
  • Placer la variable à expliquer en première position.
  • Mapper un couple (label, vecteur de features) dans un RDD.
  • Convertir ce RDD en DataFrame et nommer les variables 'label' et 'features'.
• 2. Séparer la base de données en deux échantillons train et test.
• 3. Appliquer un modèle de classification.
• 4. Evaluer le modèle.



# More Linear regressors




   Spark est en constante amélioration et possède aujourd'hui quelques régresseurs notables. Ils sont utilisables de la même façon en important ces fonction depuis pyspark.ml.regression. Vous êtes invité à consulter la documentation pour observer les différents paramètres à prendre en compte pour optimiser ces algorithmes :

  • LinearRegression() pour effectuer une régression linéaire lorsque le label est présupposé suivre une loi normale.

  • GeneralizedLinearRegression() pour effectuer une régression linéaire généralisée lorsque le label est présupposé suivre une autre loi que l'on spécifie dans le paramètre family (gaussian, binomial, poisson, gamma).

  • AFTSurvivalRegression() pour effectuer une analyse de survie.

Il est également possible d'utiliser les algorithmes, qui gèrent également les variables catégorielles, détaillés dans l'exercice suivant :

  • DecisionTreeRegressor() pour un arbre de décision.

  • RandomForestRegressor() pour une forêt aléatoire d'arbres de décision.

  • GBTRegressor() pour une forêt d'arbres gradient-boosted.












