# ESGIFootballApp

## Prérequis 
⋅⋅* Apache Maven 3.6.3
⋅⋅* Spark 2.4.5

## Lancement du projet : 

Tout d'abord, il faut build le projet grâce à maven.
Pour cela il faut se placer à la racine du projet et effectuer le commande suivante :
```
mvn -DskipTests clean package
```

Pour lancer le projet, utiliser la commande suivante : 
```
spark-submit --class esgi.exo.FootballApp --master local[*] ./target/esgi-1.0-SNAPSHOT.jar "path_to.csv"
```

Le dernier argument path_to.csv correspond au chemin du CSV que vous souhaitez utiliser.

Un dossier data sera alors créé à la racine du projet. Il contiendra les dossier stats.parquet puis result.parquet.

## Lancement des tests
Pour lancer les tests, utilisez la commande suivante : 
```
mvn test
```
