# Introduction

In the previous lesson it has been studied the structure RDD, ideal to work on lines. But when we deal with data bases organized in columns, we need somethings like dataFrames. 
Nevertheless **RDD are extremely efficient on non structed datas such as text.**

It will be used here on the Miserables to count the number of occurences of each word to go deeper in the use of map/reduce.

# SparkContext

```python
# Import de SparkContext du module pyspark
from pyspark import SparkContext

# Défintion d'un SparkContext
sc = SparkContext.getOrCreate()
```

# Import the database (Les Miserables)

- Import the file LesMiserables dans un RDD.
- Show the fisrt 10 lines.



```python
miserables = sc.textfile('miserables.txt')
miserables.take(10)

>>>
['Les Misérables Tome 1 - Fantine',
 'Livre Premier - Un Juste',
 'Chapitre I - Monsieur Myriel',
 'En 1815, M. Charles-François-Bienvenu Myriel était évêque de Digne. C’était un vieillard d’environ soixante-quinze ans ; il occupait le siège de Digne depuis 1806.',
 'Quoique ce détail ne touche en aucune manière au fond même de ce que nous avons à raconter, il n’est peut-être pas inutile, ne fût-ce que pour être exact en tout, d’indiquer ici les bruits et les propos qui avaient couru sur son compte au moment où il était arrivé dans le diocèse. Vrai ou faux, ce qu’on dit des hommes tient souvent autant de place dans leur vie et surtout dans leur destinée que ce qu’ils font. M. Myriel était fils d’un conseiller au parlement d’Aix ; noblesse de robe. On contait de lui que son père, le réservant pour hériter de sa charge, l’avait marié de fort bonne heure, à dix-huit ou vingt ans, suivant un usage assez répandu dans les familles parlementaires. Charles Myriel, nonobstant ce mariage, avait, disait-on, beaucoup fait parler de lui. Il était bien fait de sa personne, quoique d’assez petite taille, élégant, gracieux, spirituel ; toute la première partie de sa vie avait été donnée au monde et aux galanteries. La révolution survint, les événements se précipitèrent, les familles parlementaires décimées, chassées, traquées, se dispersèrent. M. Charles Myriel, dès les premiers jours de la révolution, émigra en Italie. Sa femme y mourut d’une maladie de poitrine dont elle était atteinte depuis longtemps. Ils n’avaient point d’enfants. Que se passa-t-il ensuite dans la destinée de M. Myriel ? L’écroulement de l’ancienne société française, la chute de sa propre famille, les tragiques spectacles de 93, plus effrayants encore peut-être pour les émigrés qui les voyaient de loin avec le grossissement de l’épouvante, firent-ils germer en lui des idées de renoncement et de solitude ?',
 'Fut-il, au milieu d’une de ces distractions et de ces affections qui occupaient sa vie, subitement atteint d’un de ces coups mystérieux et terribles qui viennent quelquefois renverser, en le frappant au cœur, l’homme que les catastrophes publiques n’ébranleraient pas en le frappant dans son existence et dans sa fortune ? Nul n’aurait pu le dire ; tout ce qu’on savait, c’est que, lorsqu’il revint d’Italie, il était prêtre.',
 'En 1804, M. Myriel était curé de Brignolles. Il était déjà vieux, et vivait dans une retraite profonde.',
 'Vers l’époque du couronnement, une petite affaire de sa cure, on ne sait plus trop quoi, l’amena à Paris. Entre autres personnes puissantes, il alla solliciter pour ses paroissiens M. le cardinal Fesch. Un jour que l’empereur était venu faire visite à son oncle, le digne curé, qui attendait dans l’antichambre, se trouva sur le passage de sa majesté. Napoléon, se voyant regardé avec une certaine curiosité par ce vieillard, se retourna, et dit brusquement :',
 '— Quel est ce bonhomme qui me regarde ?',
 '— Sire, dit M. Myriel, vous regardez un bonhomme, et moi je regarde un grand homme. Chacun de nous peut profiter.']
```

# Shape the dataBase

We want to count each occurence of each word so we need to:
- put every wrods in the same format. We will use the method **lower()**.
- eliminate all punctuations and replace it by space using **replace()**.

```python
# Création d'un RDD nettoyé
miserables_clean = miserables.map(lambda x : x.lower().replace(',', ' ').replace('.', ' ').replace('-', ' ').replace('’', ' '))
miserables_clean.take(3)
>>>
miserables_clean.take(3)

['les misérables tome 1   fantine',
 'livre premier   un juste',
 'chapitre i   monsieur myriel']

```

Now we can separate words from each lines using split. If we use map we will create list of list. So we will use **flatMap** to create one dimension list. It is like ravel() in python.

```python
# Insérez votre code ici 

miserables_flat= miserables_clean.flatMap(lambda line: line.split(' '))
miserables_flat.take(10)

>>>
['les', 'misérables', 'tome', '1', '', '', 'fantine', 'livre', 'premier', '']

```


# Map Reduce

Using miserables_flat, let's count the number of occurences of each words.
```python
# création d'un RDD contenant l'ensemble des couples (mot, nb_occurences) 
mots = miserables_flat.map(lambda x : (x,1)) \
                      .reduceByKey(lambda x,y : x + y)
```


```python
### Première méthode de tri

# Tri en utilisant la fonction 'sorted' des RDD
mots_sorted  = sorted(mots.collect(),
                     key= lambda x: x[1],
                     reverse= 0)

### Deuxième méthode de tri

# Tri en utilisant la fonction 'sortBy' des RDD puis convertir en liste en utilisant collect
mots_sorted_2 = mots.sortBy(lambda couple: couple[1], ascending = True) \
                    .collect()
```



# method combo
We can combine all those method in one

```python

# Création d'une liste à partir du fichier texte
mots_sorted_3 = sc.textFile("miserables_full.txt") \
                  .map(lambda x : x.lower().replace(',', ' ').replace('.', ' ').replace('-', ' ').replace('’', ' ')) \
                  .flatMap(lambda line: line.split(" ")) \
                  .map(lambda x : (x,1)) \
                  .reduceByKey(lambda x,y : x + y) \
                  .sortBy(lambda couple: couple[1], ascending = True) \
                  .collect()
                
mots_sorted_3

>>>
[('hériter', 1),
 ('galanteries', 1),
 ('décimées', 1),
 ('traquées', 1),
 ('ébranleraient', 1),
 ('1804', 1),
 ('brignolles', 1),
 ('palabres', 1),
 ('racontages', 1)...
```


# Close SparkContext

**sc.stop()**
