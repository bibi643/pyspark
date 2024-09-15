# Introduction

Previously we saw how to manipulate a df, and the structure svmlib of a database to apply **regression** algortihm.

Here we will see how to deal with **classification** algorithm.

We need to understand how 
- pyspark deal with categorical data
- to use a ML Pipeline.


**ML Pipelines** consists in combining estimators and transformators to define a ML process.


# Build SparSession

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

spark = SparkSession \
    .builder \
    .appName("Pipelines Spark ML")
    .getOrCreate()


spark
```



# Categorical Variables

**WARNING: svmlib does not work with categorical variable.**

We need to use an indexor, which convert a categorical variable into index series.

We will work here on a data set containing both types of variables (Human Resources Analytics)



```python

hr = spark.read.csv('HR_comma_sep.csv')


# Affichage d'un extrait du DataFrame
hr.sample(False, 0.001, seed=222).toPandas()
```

The column to predict is the column left which indicate if an employee left voluntarly (1) or not (0). We need to place the left column in first.

```python
# Ordonner les variables pour avoir le label en première colonne
hr = hr.select( 'left',
               'satisfaction_level',
               'last_evaluation',
               'number_project',
               'average_montly_hours',
               'time_spend_company',
               'Work_accident',
               'promotion_last_5years',
               'sales',
               'salary')

# Affichage d'une description des variables
hr.describe().toPandas()

```

DenseVector cannot handle strings, so we need to convert categorical into indexes. 

The function **StringIndexer** from the package pyspark.ml.feature can index variables in function of the frequence of their modality. The most frequent modality will be indexed as 0.0 and the next one 1.0. StringIndexer take in input a variable and return an indexed variable.

2 steps:
- Create an indexer, specifying the input and ouptut columns( inputCol and outputCol)and search the modalities in the database using fit().
- apply the indexer to the data base using transform().



_example_: To index the variable name in a dataframe df.
- nameindexer = StringIndexer(inputCol ='name', outputCol ='indexedName').fit(df)
- indexed = nameindexer.transform(df)


```python
from pyspark.ml.fetaure import StringIndexer
SalesIndexer = StringIndexer(inputCol = 'sales',outputCol = 'indexeSales').fit(hr)
hrSalesIndexed = SalesIndexer.transform(hr)

hrSalesIndexed.sample(False, .0001,seed= 122).toPandas()


+------------------+---------------+--------------+--------------------+------------------+-------------+----+---------------------+-----+------+------------+
|satisfaction_level|last_evaluation|number_project|average_montly_hours|time_spend_company|Work_accident|left|promotion_last_5years|sales|salary|indexedSales|
+------------------+---------------+--------------+--------------------+------------------+-------------+----+---------------------+-----+------+------------+
|              0.38|           0.53|             2|                 157|                 3|            0|   1|                    0|sales|   low|         0.0|
|               0.8|           0.86|             5|                 262|                 6|            0|   1|                    0|sales|medium|         0.0|
|              0.11|           0.88|             7|                 272|                 4|            0|   1|                    0|sales|medium|         0.0|
|              0.72|           0.87|             5|                 223|                 5|            0|   1|                    0|sales|   low|         0.0|
|              0.37|           0.52|             2|                 159|                 3|            0|   1|                    0|sales|   low|         0.0|
|              0.41|            0.5|             2|                 153|                 3|            0|   1|                    0|sales|   low|         0.0|
|               0.1|           0.77|             6|                 247|                 4|            0|   1|                    0|sales|   low|         0.0|
|              0.92|           0.85|             5|                 259|                 5|            0|   1|                    0|sales|   low|         0.0|
|              0.89|              1|             5|                 224|                 5|            0|   1|                    0|sales|   low|         0.0|
|              0.42|           0.53|             2|                 142|                 3|            0|   1|                    0|sales|   low|         0.0|
|              0.45|           0.54|             2|                 135|                 3|            0|   1|                    0|sales|   low|         0.0|
|              0.11|           0.81|             6|                 305|                 4|            0|   1|                    0|sales|   low|         0.0|
|              0.84|           0.92|             4|                 234|                 5|            0|   1|                    0|sales|   low|         0.0|
|              0.41|           0.55|             2|                 148|                 3|            0|   1|                    0|sales|   low|         0.0|
|              0.36|           0.56|             2|                 137|                 3|            0|   1|                    0|sales|   low|         0.0|
|              0.38|           0.54|             2|                 143|                 3|            0|   1|                    0|sales|   low|         0.0|
|              0.45|           0.47|             2|                 160|                 3|            0|   1|                    0|sales|   low|         0.0|
|              0.78|           0.99|             4|                 255|                 6|            0|   1|                    0|sales|   low|         0.0|
|              0.45|           0.51|             2|                 160|                 3|            1|   1|                    1|sales|   low|         0.0|
|              0.76|           0.89|             5|                 262|                 5|            0|   1|                    0|sales|   low|         0.0|
+------------------+---------------+--------------+--------------------+------------------
```


To **reverse** the process, fiding the categorical variable from the indexed one, we can use **IndexToString**

- inputCol: name of the indexed column
- outputCol: name of the output column
-labels: labels from the indexer **previously created**

**IndexToString does not need the fit part. we just need the transform part.**


_example_:
nameReconstructor = IndextoString(inputCol= 'indexedName', outputCol = 'nameReconstructed', labels = nameindexer.labels)


```python
from pyspark.ml.feature import IndexToString

salesReconstructor= IndexToString(inputCol = 'indexedSales',outputCol = 'salesReconstructed', labels = SalesIndexer.labels)
hrSalesReconstructed = salesReconstructor.transform(hrSalesIndexed)
```


# Pipelines


To apply in an elegant way a serie of estimators or transformers, SparkML has the **function** Pipeline(). **Pipelines** are defined as a serie of estimators or transformers called stages.
Pipeline(stages=[estimator1, estimator2, estimator3,...])


```python
from pyspark.ml import Pipeline
SalesIndexer = StringIndexer(inputCol = 'sales', outputCol='indexedSales')
SalaryIndexer = StringIndexer(inputCol = 'salary', outputCol = 'indexedSalary')
indexer = Pipeline(stages= [SalesIndexer, SalaryIndexer])

hrIndexed = indexer.fit(hr).transform(hr)
hrIndexer.sample(False, 0.01, seed=123).toPandas()
```

**Note**: Here we put fit and transform at the end.



# Transformation into svmlib format

Now the variables are in numeric format. It can be transformed to svmlib format, but we need fisrt to exclude non Indexed variables.


```python
# Import de DenseVector du package pyspark.ml.linalg
from pyspark.ml.linalg import DenseVector


# Selection of Indexed and numeric variables excluding Non Indexed variables.
hrNumeric=hrIndexed.select('left',
                           'satisfaction_level','last_evaluation',
                           'number_project',
                           'average_montly_hours',
                           'time_spend_company',
                           'Work_accident',
                           'promotion_last_5years',
                           'indexedSales',
                           'indexedSalary')

# We separate the label from the features, and combining all together the features using the RDD format

hdRDD= hrNumeric.rdd.map(lambda x: (x[0], DenseVector[1:])))


# Now we create the df and rename the columns
hrsvlim = spark.createDataFrame(hrRDD,['label','fetaures'])


# Display of the df
hrlibsvm.sample(False, .001, seed =222).toPandas()

```

**Summary: transformation of a databe to svmlib**
- Import the function DenseVector
- Transform each line in a couple containing the label and a vector of feature using rdd.map()
- Transform this rdd to a dataframe renaming the first column label and the second one features.



# Application of a SparkML Classifier

The database is now into a svmlib format. We just need to specify that
- variable label is categorical
- some features are categorical and others are continue.



To classify our label we need to create an IndexedLabel variable.
labelIndexer = StringIndexer(inputCol ='label', outputCol= 'IndexedLabel').fit(hr_ml)

For the features we will use the function VectorIndexer(inputCol='features', outputCol= 'indexedFeatures', maxCategories = 5).fit(hr_ml).
This function will indexed variable with a specified number of modalities (maxcategories). So we need to know how many modalities categorical variables have.

```python
hrNumeric.describe().toPandas()
```

We have 2 categorical variables (IndexedSales and IndexedSalary). 0->9 modalities means we have 10 modalities. So maxcategories will be 10.

```python
from pyspark.ml.feature import VectorIndexer

featureIndexer = VectorIndexer(inputCol='features', outputCol= 'indexedFeatures',maxCategories = 10).fit(hrlibsvm)
```

We will use here **Random Forest** as a classifier. This algorithm use different sets of data from the big set of data to perform some decision trees and make them vote separatly.

**RandomForestClassifier()** from the package **pyspark.ml.classifiaction** creates a classifier with the following arguments:
- labelCol: name of the column to use as a label
- featuresCol: name of the column to use with the features
- predictionCol: name of the column with the predictions (by default = predictions).
- seed
- more parameters to tune the analysis

_Example_:
rf= RandomForestClassifier(labelCol='IndexedLabel', featuresCol='indexedFeatures', predictionCol= 'prediction', seed=222)



```python 
from pyspark.ml.classification import RandomForestClassifier

# Transformer creation
labelIndexer = StringIndexer(inputCol ='label', outputCol= 'IndexedLabel').fit(hrlibsvm)
featureIndexer = VectorIndexer(inputCol ='features', outputCol='IndexedFeatures', maxCategories=10).fit(hrlibsvm)

# Creation of random forest object working on the indexed labels and features
rf = RandomForestClassifier(labelCol= 'IndexedLabel',outputCol= 'IndexedFeatures', predictionCol= 'prediction',seed= 123)


# With IndexToString recover the labels from the predictions

labelConverter = IndexToString(inputCol ='prediction', outputCol= 'prediction_reconstructed',labels = labelIndexer.labels)


# Creation of the pipeline with the 4 estimators/transformers we just created.

pipeline = Pipeline(stages = [labelIndexer, fetaureIndexer, rf,labelConverter])


# Split the datas into train and test datasets.
train, test = hrLibsvm.ramdomSplit([.7, .3], seed = 1234)

#creation of a model using the Pipeline on train sets
model = pipeline.fit(train)


# Predictions dataframe
predictions = model.transform(test)

predictions.sample(False, .001, seed=1234).toPandas()


```

# Model Evaluation

Once we built a model, it is important to check how good it is (Accuracy of the predictions) to compare it with other models or optimise the paramaters.
We have a module called **pyspark.ml.evaluation** with all the metrics to evaluate properly. For instance the the function **MulticlassClassificationEvaluator** to evaluate classification models.

It has 3 main arguments:
- metricName: typically 'accuracy'
- labelCol: name of the column to predict
- predictionCol: name of the prediction column

The created evaluator possesses an evaluate method to apply it to a sample.



```python
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Creation of the evaluator
evaluator = MulticlassClassificationEvaluator(metricName= 'accuracy', labelCol = labels, predictionCol= features)

accuracy = evaluator.evaluate(test)

print(accuracy)




# Conclusion

     La métrique accuracy correspond au nombre de prédictions correctes divisé par le nombre de prédictions effectuées. Elle est donc comprise entre 0 et 1 ; une accuracy de 0 correspond à des prédictions toutes fausses et une accuracy de 1 correspond à l'absence d'erreur dans la prédiction.
      Aller plus loin — Autres algorithmes de classification

    Vous avez maintenant tous les outils en main pour effectuer tout type de classification en Spark ML. Pour résumer de façon concise, une classification s'effectue de la façon suivante :

    • 1. Transformer toutes les variables en variables numériques.
    • 2. Transformer la base en format svmlib.
    • 3. Créer une Pipeline contenant :
      • La transformation de la variable label en catégorie.
      • La transformation des features catégorielles.
      • Un modèle de classification.
      • Un transformateur inverse de l'indexation pour les prédictions créées.
    • 4. Evaluer le modèle.

      Spark est en constante amélioration et possède aujourd'hui quelques classifieurs notables que vous pouvez utiliser de la même façon en important ces fonctions depuis le package pyspark.ml.classification. Vous êtes invités à consulter la documentation pour observer les différents paramètres à prendre en compte pour optimiser ces algorithmes :

      • LogisticRegression() pour effectuer une régression logistique.
      • DecisionTreeClassifier() pour un arbre de régression simple.
      • RandomForestClassifier() pour une forêt aléatoire.
      • GBTClassifier() pour une forêt d'arbres gradient-boosted.
      • LinearSVC() pour un SVM de régression à noyau linéaire.
      • NaiveBayes() pour une classification naïve bayesienne.

    Pour une liste complète des algorithmes de Spark ML, vous pouvez également consulter la documentation générale de pyspark.ml; la liste des algorithmes disponibles ne cesse de grandir au fur et à mesure du développement de Spark. 





