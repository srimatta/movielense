# movielense

MovieLense Application Calculates the Minimum, Maximum and Average Ratings for the given Movies 
And also calculates the top rated movies by the User. 

Generating Uber Jar
`sbt  assembly` will generate 'movielense-assembly-1.0.jar' under 'target/scala-2.12' folder

Running Application

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

Spark submit command, make sure `application.conf` in same folder.

spark-submit target/scala-2.12/movielense-assembly-1.0.jar --class org.example.MovieLenseApp







