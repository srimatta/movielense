# Movielense

MovieLense Application calculates the minimum, maximum and average ratings for the given movies 
and also calculates the top rated movies by the user. 

#### Generating Uber Jar

`sbt  assembly` will generate 'movielense-assembly-1.0.jar' under 'target/scala-2.12' folder



`application.conf` provides the config details for the Application. 

```
movielense {
       master = "local"
       appName = "MovieLenseApp"
       topRatedMovies = 3
       movies {
           dataPath = "ml-1m/movies.dat"
           dataFormat = "csv"
           separator = "::"
       }
       ratings{
           dataPath = "ml-1m/ratings.dat"
           dataFormat = "csv"
           separator = "::"
       }
}
```

#### Running Application

Spark submit command :  

`spark-submit target/scala-2.12/movielense-assembly-1.0.jar --files application.conf  --class org.example.MovieLenseApp`
  will generate the results like below.


```
calculating Movie Min, Max, Avg, Ratings
Movies with Min, Max ,Average Ratings
+-------+--------------------+--------------------+----------+----------+------------------+
|MovieID|               Title|              Genres|min_rating|max_rating|        avg_rating|
+-------+--------------------+--------------------+----------+----------+------------------+
|      1|    Toy Story (1995)|Animation|Childre...|         3|         5|               4.0|
|      3|Grumpier Old Men ...|      Comedy|Romance|         3|         5|3.6666666666666665|
|      5|Father of the Bri...|              Comedy|         1|         5|3.3333333333333335|
|      9| Sudden Death (1995)|              Action|         1|         3|               2.0|
|      4|Waiting to Exhale...|        Comedy|Drama|         3|         3|               3.0|
|      8| Tom and Huck (1995)|Adventure|Children's|         4|         4|               4.0|
|      7|      Sabrina (1995)|      Comedy|Romance|         3|         4|               3.5|
|      2|      Jumanji (1995)|Adventure|Childre...|         4|         5| 4.666666666666667|
+-------+--------------------+--------------------+----------+----------+------------------+

calculating User top rated movies
User 3 Top rated movies 
+------+-------+------+---------+--------------------+--------------------+
|UserID|MovieID|Rating|Timestamp|               Title|              Genres|
+------+-------+------+---------+--------------------+--------------------+
|     1|      2|     5|978300760|      Jumanji (1995)|Adventure|Childre...|
|     1|      5|     4|978300275|Father of the Bri...|              Comedy|
|     1|      3|     3|978302109|Grumpier Old Men ...|      Comedy|Romance|
|     6|      2|     5|978302109|      Jumanji (1995)|Adventure|Childre...|
|     6|      5|     5|978298814|Father of the Bri...|              Comedy|
|     6|      1|     3|978300760|    Toy Story (1995)|Animation|Childre...|
|     3|      9|     2|978246162| Sudden Death (1995)|              Action|
|     5|      1|     5|978244808|    Toy Story (1995)|Animation|Childre...|
|     5|      2|     4|978244759|      Jumanji (1995)|Adventure|Childre...|
|     5|      8|     4|978244177| Tom and Huck (1995)|Adventure|Children's|
|     2|      3|     5|978298814|Grumpier Old Men ...|      Comedy|Romance|
|     2|      7|     4|978299773|      Sabrina (1995)|      Comedy|Romance|
|     2|      4|     3|978300051|Waiting to Exhale...|        Comedy|Drama|
+------+-------+------+---------+--------------------+--------------------+

```







