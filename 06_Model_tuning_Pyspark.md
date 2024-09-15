# Introduction

There are multiple methods to tune the parameters of a model in order to **avoid overfitting.**

The most common method is cross validation. In this case the learning process is made in 4 steps:
- Selection of the model to optimise.
- Creation of a grid of parameters.
- Selection of an evaluation metric
- CrossValidator


```python
# Import de SparkSession et de SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext

# Création d'un SparkContext
sc = SparkContext.getOrCreate()

# Création d'une session Spark
spark = SparkSession \
    .builder \
    .appName("ML Tuning") \
    .getOrCreate()
        
spark
```


# Import the dataBase.

We will work here on database called Year Prediction MSD. Robustness in ML is how efficient is an algorithm when facing new datas. 

For time purposes we will consider only the 12th first variables and 10% of the dataBase. Obviously we will obtain worst results but in a faster way.

## Loading the sample of the dataBase.

```python
# Chargement de la base de données brute
df_full = spark.read.csv('YearPredictionMSD.txt', header=False)

# On infère les bons types des colonnes
from pyspark.sql.functions import col
exprs = [col(c).cast("double") for c in df_full.columns[1:13]]

df_casted = df_full.select(df_full._c0.cast('int'),
                           *exprs)

# Enfin, par soucis de rapidité des calculs,
# on ne traitera qu'un extrait de la base de données 
df = df_casted.sample(False, .1, seed = 222)

df.sample(False, .001, seed = 222).toPandas()
```



## Converting to svmlib format
Now we will convert the df into svlib format -> label column and features vector column
```python
from pyspark.ml.linalg import DenseVector
rdd_ml = df.rdd.map(lambda x: (x[0], DenseVector(x[1:])))
df_ml = spark.createDataFrame(rdd_ml, ['label','features'])


```

Next it is important to **keep a test sample** that will never be used by the algorithm. It will only be used to **measure the performance of the model**.

```python
train,test = df_ml.randomSplit([.8,.2],seed=1234)

```

First we will instanciate a model LR.

```python
from pyspark.ml.regression import LinearRegression

lr = LinearRegression(labelCol= 'label', featuresCol= 'features')
```


# Creation of the GridParameter

The goal is to **find the set of parameters which make the algorithm as robust as possible**.

The function **ParamGridBuilder()** from pyspark.ml.tuning is dedicated to parameters tuning. It is used as:
- Call the function ParamGridBuilder() without any argument.
- Add the parameters using the method addGrid() applied to the constructor.
- Build the grid using the method Build()

_Example_:
```python
# Création d'une grille contenant les valeurs 0 et 1
# pour les paramètres regParam et elasticNetParam
param_grid = ParamGridBuilder().addGrid(lr.regParam,[0,1])
    .addGrid(lr.elasticnetParam, [0,1])
    .build()

``


```python
# Créer une grille de paramètres, appelée param_grid, contenant les valeurs 0, 0.5 et 1 pour les paramètres regParam et elasticNetParam.

# Insérez votre code ici
from pyspark.ml.tuning import ParamGridBuilder
param_grid=ParamGridBuilder().\
    addGrid(lr.regParam,[0,0.5,1]).\
    addGrid(lr.elasticNetParam,[0,0.5,1]).\
    build()
```


# Choosing an evaluation metric

To evaluate models, an evaluator is defined to optimise the parameters. It is possible to minimse this metric to chose the best parameters.

For each algorithm, there is one evaluator inside the package **pyspark.ml.evaluation**. For example, for a regression there is the _RegressionEvaluator_ to use the metric R2 or RMSE.

This evaluator is defined specifying the name of the label columns and prediction and the metric to use.

_Example_:
ev = RegressionEvaluator(predictionCol= 'prediction', labelCol='label',metricName = 'rmse')


```python
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol= 'prediction',labelCol='label',metricName='r2')
```


# Parameter tuning using cross validation

It is possible to change the parameters in this grid to obtain the best parameters possible, following different methods. Usually the method used is cross validation.

**k-folds cross validation** is super efficient to improve robustness of a model.
- split the data in k samples.
- Selection of one k sample (hold out) as a validation set. The K-1 other samples are used for the training.
- Error estimation doing a test, a metric, or score on test set.
- We repeat all the above selecting another hold out sample.

This operation is repeated k times, so each k sample is used as a hold out (validation). 

Finally **the average of the k scores is performed to estimate the error in the prediction.**

The package **pyspark.ml.tuning has a function CrossValidator** having the following arguments:
- estimator: estimator to use (linear regression, random forest, etc...)
- estimatorParamMaps: Param grid
- evaluator: evaluation metric
- numFolds: k. Number of folds to create from dataset.


```python
from pyspark.ml.tuning import CrossValidator
cv= CrossValidator(estimator = lr,estimatorParamGrid=param_grid,evaluator = evaluator, numFolds = 3)
```


# Application to the model

Let's train the model using fit().
We need to instanciate again lr, ev, cv.


```python
lr = LinearRegression(featuresCol = 'features', labelCol = 'label')
ev = RegressionEvaluator(predictionCol='prediction', labelCol='label', metricName='r2')
cv = CrossValidator(estimator = lr, estimatorParamMaps = param_grid, evaluator = ev, numFolds = 3)

cv_model = cv.fit(train)


```

With this model let's predict the release year in train and in test sets.

```python
# Calcul des prédictions des données d'entraînement
pred_train = cv_model.transform(train)

# Calcul des prédictions des données de test
pred_test  = cv_model.transform(test)

```

Once done, we can obtain the scores to evaluate the model.


To obtain the score we can apply 2 methods successively.
- setMetricName to specify the metric to apply.
- evaluate to specify the dataset we want to have the score.
ev.setMetricName('r2').evaluate(pred_train), to obtain the r2 on the train set

Let's calculate the score on the test set.

```python
ev.setMetricName('r2').evaluate(pred_test)
```


# Use of the results

The global informations is availabel in the argument **bestModel**.

```python
cv_model.bestModel.coefficients
```

 Les paramètres de la grille obtenus pour le meilleur modèle ne sont pas accessibles directement, mais sont stockés au sein de l'objet java :

cv_model.bestModel._java_obj.getRegParam()
cv_model.bestModel._java_obj.getElasticNetParam()

 Cette information est utile pour vérifier que notre grille est bien adaptée au modèle : il faut éviter que le paramètre choisi soit sur un bord de notre intervalle. Il faut ici passer par l'objet java parce que cette option n'est pas encore disponible directement dans PySpark. 





