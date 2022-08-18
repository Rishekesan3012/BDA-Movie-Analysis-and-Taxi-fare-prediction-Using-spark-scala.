scala> val Movies_Rdd = sc.textFile("/Users/avinashdora/Downloads/movies.txt")
/*
Movies_Rdd: org.apache.spark.rdd.RDD[String] = /Users/avinashdora/Downloads/movies.txt MapPartitionsRDD[1] at textFile at <console>:24
*/
scala> Movies_Rdd.take(10).foreach(println)
/*
1::Toy Story (1995)::Animation|Children's|Comedy                                
2::Jumanji (1995)::Adventure|Children's|Fantasy
3::Grumpier Old Men (1995)::Comedy|Romance
4::Waiting to Exhale (1995)::Comedy|Drama
5::Father of the Bride Part II (1995)::Comedy
6::Heat (1995)::Action|Crime|Thriller
7::Sabrina (1995)::Comedy|Romance
8::Tom and Huck (1995)::Adventure|Children's
9::Sudden Death (1995)::Action
10::GoldenEye (1995)::Action|Adventure|Thriller
*/

scala> val moviesschema = Movies_Rdd.map{l =>
     | val str0 = l.split("::")
     | val id = str0(0)
     | val year = l.substring(l.lastIndexOf("(")+1,l.lastIndexOf(")"))
     | val movie_title = (l.split("::")(1)).split("\\(")(0)
     | val genres = l.split("::")(2)
     | (id,movie_title,year,genres)}
/*
moviesschema: org.apache.spark.rdd.RDD[(String, String, String, String)] = MapPartitionsRDD[100] at map at <console>:25
*/

scala> val Movie_DF = moviesschema.toDF("Movie ID","Movie Title","Year","Genre")
/*
Movie_DF: org.apache.spark.sql.DataFrame = [Movie ID: string, Movie Title: string ... 2 more fields]
*/

scala> Movie_DF.show()
/*
+--------+--------------------+----+--------------------+
|Movie ID|         Movie Title|Year|               Genre|
+--------+--------------------+----+--------------------+
|       1|          Toy Story |1995|Animation|Childre...|
|       2|            Jumanji |1995|Adventure|Childre...|
|       3|   Grumpier Old Men |1995|      Comedy|Romance|
|       4|  Waiting to Exhale |1995|        Comedy|Drama|
|       5|Father of the Bri...|1995|              Comedy|
|       6|               Heat |1995|Action|Crime|Thri...|
|       7|            Sabrina |1995|      Comedy|Romance|
|       8|       Tom and Huck |1995|Adventure|Children's|
|       9|       Sudden Death |1995|              Action|
|      10|          GoldenEye |1995|Action|Adventure|...|
|      11|American Presiden...|1995|Comedy|Drama|Romance|
|      12|Dracula: Dead and...|1995|       Comedy|Horror|
|      13|              Balto |1995|Animation|Children's|
|      14|              Nixon |1995|               Drama|
|      15|   Cutthroat Island |1995|Action|Adventure|...|
|      16|             Casino |1995|      Drama|Thriller|
|      17|Sense and Sensibi...|1995|       Drama|Romance|
|      18|         Four Rooms |1995|            Thriller|
|      19|Ace Ventura: When...|1995|              Comedy|
|      20|        Money Train |1995|              Action|
+--------+--------------------+----+--------------------+
only showing top 20 rows
*/

scala> Movie_DF.filter("Genre == 'Action'").show(false)
/*
+--------+------------------------------+----+------+
|Movie ID|Movie Title                   |Year|Genre |
+--------+------------------------------+----+------+
|9       |Sudden Death                  |1995|Action|
|20      |Money Train                   |1995|Action|
|71      |Fair Game                     |1995|Action|
|145     |Bad Boys                      |1995|Action|
|204     |Under Siege 2: Dark Territory |1995|Action|
|227     |Drop Zone                     |1994|Action|
|251     |Hunted, The                   |1995|Action|
|315     |Specialist, The               |1994|Action|
|384     |Bad Company                   |1995|Action|
|393     |Street Fighter                |1994|Action|
|394     |Coldblooded                   |1995|Action|
|459     |Getaway, The                  |1994|Action|
|479     |Judgment Night                |1993|Action|
|533     |Shadow, The                   |1994|Action|
|544     |Striking Distance             |1993|Action|
|548     |Terminal Velocity             |1994|Action|
|667     |Bloodsport 2                  |1995|Action|
|694     |Substitute, The               |1996|Action|
|876     |Police Story 4: Project S     |1993|Action|
|886     |Bulletproof                   |1996|Action|
+--------+------------------------------+----+------+
only showing top 20 rows
*/

scala> Movie_DF.filter("Genre == 'Drama|Thriller'").show(false)
/*
+--------+---------------------------------+----+--------------+
|Movie ID|Movie Title                      |Year|Genre         |
+--------+---------------------------------+----+--------------+
|16      |Casino                           |1995|Drama|Thriller|
|61      |Eye for an Eye                   |1996|Drama|Thriller|
|79      |Juror, The                       |1996|Drama|Thriller|
|92      |Mary Reilly                      |1996|Drama|Thriller|
|100     |City Hall                        |1996|Drama|Thriller|
|111     |Taxi Driver                      |1976|Drama|Thriller|
|217     |Babysitter, The                  |1995|Drama|Thriller|
|225     |Disclosure                       |1994|Drama|Thriller|
|229     |Death and the Maiden             |1994|Drama|Thriller|
|230     |Dolores Claiborne                |1994|Drama|Thriller|
|280     |Murder in the First              |1995|Drama|Thriller|
|436     |Color of Night                   |1994|Drama|Thriller|
|454     |Firm, The                        |1993|Drama|Thriller|
|481     |Kalifornia                       |1993|Drama|Thriller|
|593     |Silence of the Lambs, The        |1991|Drama|Thriller|
|614     |Loaded                           |1994|Drama|Thriller|
|627     |Last Supper, The                 |1995|Drama|Thriller|
|628     |Primal Fear                      |1996|Drama|Thriller|
|640     |Diabolique                       |1996|Drama|Thriller|
|678     |Some Folks Call It a Sling Blade |1993|Drama|Thriller|
+--------+---------------------------------+----+--------------+
only showing top 20 rows
*/

scala> val Genres_Rdd = Movies_Rdd.map(l => l.split("::")(2))
/*
Genres_Rdd: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[3] at map at <console>:25
*/

scala> val Genres_Distinct = (Genres_Rdd.flatMap(l => l.split('|'))).distinct().sortBy(_(0))
/*
Genres_Distinct: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[24] at sortBy at <console>:25
*/

scala> Genres_Distinct.count
/*
res10: Long = 18
*/

scala> Genres_Distinct.toDF("Distinct Genres").show(false)
/*
+---------------+
|Distinct Genres|
+---------------+
|Animation      |
|Adventure      |
|Action         |
|Crime          |
|Comedy         |
|Children's     |
|Documentary    |
|Drama          |
|Fantasy        |
|Film-Noir      |
|Horror         |
|Musical        |
|Mystery        |
|Romance        |
|Sci-Fi         |
|Thriller       |
|War            |
|Western        |
+---------------+
*/

scala> val Genre_Count = (Genres_Rdd.flatMap(l => l.split('|')).map(k => (k,1))).reduceByKey(_+_)
/*
Genre_Count: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[55] at reduceByKey at <console>:25
*/

scala> Genre_Count.toDF("Genres","No. of movies").show(false)
/*
+-----------+-------------+
|Genres     |No. of movies|
+-----------+-------------+
|War        |143          |
|Fantasy    |68           |
|Western    |68           |
|Musical    |114          |
|Horror     |343          |
|Crime      |211          |
|Animation  |105          |
|Thriller   |492          |
|Adventure  |283          |
|Action     |503          |
|Sci-Fi     |276          |
|Comedy     |1200         |
|Documentary|127          |
|Mystery    |106          |
|Romance    |471          |
|Drama      |1603         |
|Children's |251          |
|Film-Noir  |44           |
+-----------+-------------+
*/

scala> val year_Rdd = Movies_Rdd.map(l => l.split("::")(1))
/*
year_Rdd: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[59] at map at <console>:25
*/

scala> val year_Rdd_updated = year_Rdd.map(l => l.substring(l.lastIndexOf("(")+1,l.lastIndexOf(")")))
/*
year_Rdd_updated: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[60] at map at <console>:25
*/

scala> val latest_year = year_Rdd_updated.max
/*
latest_year: String = 2000
*/

scala> val latest_movies = year_Rdd.filter(l => l.contains("("+latest_year+")"))
/*
latest_movies: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[61] at filter at <console>:27
*/

scala> latest_movies.count
/*
res17: Long = 156
*/

scala> //So there are 156 that released in the year 2000

scala> //Printing the first 20 movies

scala> latest_movies.take(20).foreach(println)
/*
Supernova (2000)
Down to You (2000)
Isn't She Great? (2000)
Scream 3 (2000)
Gun Shy (2000)
Beach, The (2000)
Snow Day (2000)
Tigger Movie, The (2000)
Trois (2000)
Boiler Room (2000)
Hanging Up (2000)
Pitch Black (2000)
Whole Nine Yards, The (2000)
Reindeer Games (2000)
Wonder Boys (2000)
Waiting Game, The (2000)
3 Strikes (2000)
Chain of Fools (2000)
Drowning Mona (2000)
Next Best Thing, The (2000)
*/











------------------------------RATINGS TXT----------------------------------------










scala> val ratings_Rdd = sc.textFile("/Users/avinashdora/Downloads/ratings.txt")
/*
ratings_Rdd: org.apache.spark.rdd.RDD[String] = /Users/avinashdora/Downloads/ratings.txt MapPartitionsRDD[3] at textFile at <console>:24
*/

scala> ratings_Rdd.take(10).foreach(println)
/*
1::1193::5::978300760
1::661::3::978302109
1::914::3::978301968
1::3408::4::978300275
1::2355::5::978824291
1::1197::3::978302268
1::1287::5::978302039
1::2804::5::978300719
1::594::4::978302268
1::919::4::978301368
*/

scala> val ratings_Rdd_updated = ratings_Rdd.map{l =>
     | val user = l.split("::")(0)
     | val movie = l.split("::")(1)
     | val rating = l.split("::")(2)
     | val timestamp = l.split("::")(3)
     | (user,movie,rating,timestamp)}
/*
ratings_Rdd_updated: org.apache.spark.rdd.RDD[(String, String, String, String)] = MapPartitionsRDD[37] at map at <console>:25
*/

scala> val Ratings_DF = ratings_Rdd_updated.toDF("User ID","Movie ID","Rating","Timestamp")
/*
Ratings_DF: org.apache.spark.sql.DataFrame = [User ID: string, Movie ID: string ... 2 more fields]
*/

scala> Ratings_DF.show()
/*
+-------+--------+------+---------+
|User ID|Movie ID|Rating|Timestamp|
+-------+--------+------+---------+
|      1|    1193|     5|978300760|
|      1|     661|     3|978302109|
|      1|     914|     3|978301968|
|      1|    3408|     4|978300275|
|      1|    2355|     5|978824291|
|      1|    1197|     3|978302268|
|      1|    1287|     5|978302039|
|      1|    2804|     5|978300719|
|      1|     594|     4|978302268|
|      1|     919|     4|978301368|
|      1|     595|     5|978824268|
|      1|     938|     4|978301752|
|      1|    2398|     4|978302281|
|      1|    2918|     4|978302124|
|      1|    1035|     5|978301753|
|      1|    2791|     4|978302188|
|      1|    2687|     3|978824268|
|      1|    2018|     4|978301777|
|      1|    3105|     5|978301713|
|      1|    2797|     4|978302039|
+-------+--------+------+---------+
only showing top 20 rows

*/
scala> val num_users = (ratings_Rdd.map(l => l.split("::"))).map(l => l(0)).distinct().count()
/*
num_users: Long = 6040 
*/

scala> val movie_id_list = Ratings_DF.select("Movie ID").map(f=>f.getString(0)).collect.toList
/*
movie_id_list: List[String] = List(1193, 661, 914, 3408, 2355, 1197, 1287, 2804, 594, 919, 595, 938, 2398, 2918, 1035, 2791, 2687, 2018, 3105, 2797, 2321, 720, 1270, 527, 2340, 48, 1097, 1721, 1545, 745, 2294, 3186, 1566, 588, 1907, 783, 1836, 1022, 2762, 150, 1, 1961, 1962, 2692, 260, 1028, 1029, 1207, 2028, 531, 3114, 608, 1246, 1357, 3068, 1537, 647, 2194, 648, 2268, 2628, 1103, 2916, 3468, 1210, 1792, 1687, 1213, 3578, 2881, 3030, 1217, 3105, 434, 2126, 3107, 3108, 3035, 1253, 1610, 292, 2236, 3071, 902, 368, 1259, 3147, 1544, 1293, 1188, 3255, 3256, 3257, 110, 2278, 2490, 1834, 3471, 589, 1690, 3654, 2852, 1945, 982, 1873, 2858, 1225, 2028, 515, 442, 2312, 265, 1408, 1084, 3699, 480, 1442, 2067, 1265, 1370, 1193, 1801, 1372, 2353, 3334, 2427, 590, 1196, 15...
*/
scala> val Movie_ID = sc.parallelize(movie_id_list)
/*
Movie_ID: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[80] at parallelize at <console>:26
*/

scala> val Movie_Count = Movie_ID.map(k => (k,1)).reduceByKey(_+_)
/*
Movie_Count: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[24] at reduceByKey at <console>:25
*/

scala> Movie_Count.take(10).foreach(println)
/*
(2828,121)
(2350,19)
(3492,14)
(736,1110)
(3638,652)
(1245,358)
(312,112)
(3319,28)
(3283,15)
(2329,640)
*/

scala> val Movie_Count_sorted = Movie_Count.sortBy(l => l._2 , false)
/*
Movie_Count_sorted: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[29] at sortBy at <console>:25
*/

scala> Movie_Count_sorted.take(10).foreach(println)
/*
(2858,3428)
(260,2991)
(1196,2990)
(1210,2883)
(480,2672)
(2028,2653)
(589,2649)
(2571,2590)
(1270,2583)
(593,2578)
*/

scala> val movie_names = Movies_Rdd.map(l=>(l.split("::")(0),l.split("::")(1)))
/*
movie_names: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[94] at map at <console>:25
*/

scala> val Watch_count = movie_names.join(Movie_Count_sorted)
/*
Watch_count: org.apache.spark.rdd.RDD[(String, (String, Int))] = MapPartitionsRDD[97] at join at <console>:27
*/

scala> Watch_count.sortBy(l => l._2._2,false).toDF("Movie ID","Movie Title and Watch Count").show(false)
/*
+--------+-------------------------------------------------------------+
|Movie ID|Movie Title and Watch Count                                  |
+--------+-------------------------------------------------------------+
|2858    |{American Beauty (1999), 3428}                               |
|260     |{Star Wars: Episode IV - A New Hope (1977), 2991}            |
|1196    |{Star Wars: Episode V - The Empire Strikes Back (1980), 2990}|
|1210    |{Star Wars: Episode VI - Return of the Jedi (1983), 2883}    |
|480     |{Jurassic Park (1993), 2672}                                 |
|2028    |{Saving Private Ryan (1998), 2653}                           |
|589     |{Terminator 2: Judgment Day (1991), 2649}                    |
|2571    |{Matrix, The (1999), 2590}                                   |
|1270    |{Back to the Future (1985), 2583}                            |
|593     |{Silence of the Lambs, The (1991), 2578}                     |
|1580    |{Men in Black (1997), 2538}                                  |
|1198    |{Raiders of the Lost Ark (1981), 2514}                       |
|608     |{Fargo (1996), 2513}                                         |
|2762    |{Sixth Sense, The (1999), 2459}                              |
|110     |{Braveheart (1995), 2443}                                    |
|2396    |{Shakespeare in Love (1998), 2369}                           |
|1197    |{Princess Bride, The (1987), 2318}                           |
|527     |{Schindler's List (1993), 2304}                              |
|1617    |{L.A. Confidential (1997), 2288}                             |
|1265    |{Groundhog Day (1993), 2278}                                 |
+--------+-------------------------------------------------------------+
only showing top 20 rows
*/

scala> val qualifying_Movies = Ratings_DF.groupBy("Movie ID").agg(avg("Rating"), count("Rating")).filter("count(Rating) >= 1000")
/*
qualifying_Movies: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [Movie ID: string, avg(Rating): double ... 1 more field]
*/

scala> qualifying_Movies.show(false)
/*
+--------+------------------+-------------+                                     
|Movie ID|avg(Rating)       |count(Rating)|
+--------+------------------+-------------+
|296     |4.278212805158913 |2171         |
|1090    |4.090113735783027 |1143         |
|1372    |3.4095904095904097|1001         |
|1394    |4.02092050209205  |1434         |
|1265    |3.953028972783143 |2278         |
|919     |4.247962747380675 |1718         |
|2700    |3.760441292356186 |1269         |
|1500    |3.813380281690141 |1136         |
|3408    |3.863878326996198 |1315         |
|2628    |3.409777777777778 |2250         |
|1304    |4.215644820295983 |1419         |
|1377    |2.9767216294859358|1031         |
|2762    |4.406262708418057 |2459         |
|2115    |3.676131322094055 |1127         |
|2012    |3.2421602787456445|1148         |
|1259    |4.0969187675070025|1785         |
|3578    |4.106029106029106 |1924         |
|11      |3.7938044530493706|1033         |
|2683    |3.388423988842399 |1434         |
|595     |3.8858490566037736|1060         |
+--------+------------------+-------------+
only showing top 20 rows
*/

scala> val top_rated_movies = Movie_DF.join(qualifying_Movies, Movie_DF.col("Movie ID").equalTo(qualifying_Movies.col("Movie ID"))).select("avg(Rating)", "Movie Title").orderBy(col("avg(Rating)").desc)
/*
top_rated_movies: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [avg(Rating): double, Movie and Year: string]
*/

scala> top_rated_movies.show(false)
/*
+------------------+---------------------------------------------------------------------------+
|avg(Rating)       |Movie and Year                                                             |
+------------------+---------------------------------------------------------------------------+
|4.554557700942973 |Shawshank Redemption, The (1994)                                           |
|4.524966261808367 |Godfather, The (1972)                                                      |
|4.517106001121705 |Usual Suspects, The (1995)                                                 |
|4.510416666666667 |Schindler's List (1993)                                                    |
|4.477724741447892 |Raiders of the Lost Ark (1981)                                             |
|4.476190476190476 |Rear Window (1954)                                                         |
|4.453694416583082 |Star Wars: Episode IV - A New Hope (1977)                                  |
|4.4498902706656915|Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb (1963)|
|4.412822049131217 |Casablanca (1942)                                                          |
|4.406262708418057 |Sixth Sense, The (1999)                                                    |
|4.395973154362416 |Maltese Falcon, The (1941)                                                 |
|4.390724637681159 |One Flew Over the Cuckoo's Nest (1975)                                     |
|4.388888888888889 |Citizen Kane (1941)                                                        |
|4.38403041825095  |North by Northwest (1959)                                                  |
|4.357565011820331 |Godfather: Part II, The (1974)                                             |
|4.3518231186966645|Silence of the Lambs, The (1991)                                           |
|4.339240506329114 |Chinatown (1974)                                                           |
|4.337353938937053 |Saving Private Ryan (1998)                                                 |
|4.335209505941213 |Monty Python and the Holy Grail (1974)                                     |
|4.329861111111111 |Life Is Beautiful (La Vita è bella) (1997)                                 |
+------------------+---------------------------------------------------------------------------+
only showing top 20 rows
*/

scala> val least_rated_movies = Movies_DF.join(qualifying_Movies, Movies_DF.col("Movie ID").equalTo(qualifying_Movies.col("Movie ID"))).select("avg(Rating)", "Movie Title").orderBy(col("avg(Rating)").asc)
/*
least_rated_movies: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [avg(Rating): double, Movie and Year: string]
*/

scala> least_rated_movies.show(false)
/*
+------------------+------------------------------------------------+           
|avg(Rating)       |Movie and Year                                  |
+------------------+------------------------------------------------+
|2.9003724394785846|Mars Attacks! (1996)                            |
|2.9330143540669855|Honey, I Shrunk the Kids (1989)                 |
|2.9767216294859358|Batman Returns (1992)                           |
|3.0029261155815656|Arachnophobia (1990)                            |
|3.0315278900565885|Blair Witch Project, The (1999)                 |
|3.036653386454183 |Lost World: Jurassic Park, The (1997)           |
|3.1332760103181427|Starship Troopers (1997)                        |
|3.1738738738738737|Twister (1996)                                  |
|3.191891891891892 |Armageddon (1998)                               |
|3.1957349581111956|Mission: Impossible 2 (2000)                    |
|3.2421602787456445|Back to the Future Part III (1990)              |
|3.279050042408821 |Mask, The (1994)                                |
|3.291159772911598 |Rocky Horror Picture Show, The (1975)           |
|3.3217477656405165|Mummy, The (1999)                               |
|3.3436960276338517|Back to the Future Part II (1989)               |
|3.351431391905232 |Perfect Storm, The (2000)                       |
|3.388423988842399 |Austin Powers: The Spy Who Shagged Me (1999)    |
|3.4011259676284307|Face/Off (1997)                                 |
|3.4095904095904097|Star Trek VI: The Undiscovered Country (1991)   |
|3.409777777777778 |Star Wars: Episode I - The Phantom Menace (1999)|
+------------------+------------------------------------------------+
only showing top 20 rows
*/

scala> val Genre_Average_Rating = Movie_DF.withColumn("Genre",split(col("Genre"), "\\|")).select(col("Movie ID"), explode(col("Genre"))).join(Ratings_DF, Movie_DF.col("Movie ID").equalTo(Ratings_DF.col("Movie ID"))).groupBy(col("col")).agg(avg("Rating")).withColumnRenamed("col", "Genre").withColumnRenamed("avg(rating)", "Average Rating").orderBy(col("Average Rating").desc).select("Average Rating", "Genre")
/*
Genre_Average_Rating: org.apache.spark.sql.DataFrame = [Average Rating: double, Genre: string]
*/

scala> Genre_Average_Rating.show(false)
/*
+------------------+-----------+                                                
|Average Rating    |Genre      |
+------------------+-----------+
|4.075187558184108 |Film-Noir  |
|3.933122629582807 |Documentary|
|3.893326717935996 |War        |
|3.766332232342065 |Drama      |
|3.708678543141273 |Crime      |
|3.684868223500335 |Animation  |
|3.6681019463387923|Mystery    |
|3.6655189849035708|Musical    |
|3.6377701493980563|Western    |
|3.607464598740535 |Romance    |
|3.5704660480809784|Thriller   |
|3.522098827752538 |Comedy     |
|3.4911849357368414|Action     |
|3.477256948332624 |Adventure  |
|3.466521291339784 |Sci-Fi     |
|3.447370595851354 |Fantasy    |
|3.422034743579087 |Children's |
|3.215013222318226 |Horror     |
+------------------+-----------+
*/

scala> val qualifying_Users = Ratings_DF.groupBy("User ID").count.filter("count >= 300").withColumnRenamed("User ID", "UserId")
/*
qualifying_Users: org.apache.spark.sql.DataFrame = [UserId: string, count: bigint]
*/

scala> qualifying_Users.show(false)
/*
+------+-----+                                                                  
|UserId|count|
+------+-----+
|2088  |441  |
|3441  |320  |
|3650  |785  |
|5523  |376  |
|919   |326  |
|1265  |343  |
|1746  |326  |
|3526  |962  |
|3836  |510  |
|3312  |665  |
|2888  |376  |
|2895  |513  |
|1897  |657  |
|3491  |727  |
|3993  |351  |
|5450  |321  |
|3299  |302  |
|4736  |318  |
|5543  |341  |
|169   |552  |
+------+-----+
only showing top 20 rows
*/

scala> val Qualified_Users_Ratings = Ratings_DF.join(qualifying_Users,Ratings_DF.col("User ID").equalTo(qualifying_Users.col("UserId"))).select("User ID","Movie ID","Rating").withColumnRenamed("Movie ID", "MovieID")
/*
Qualified_Users_Ratings: org.apache.spark.sql.DataFrame = [User ID: string, MovieID: string ... 1 more field]
*/

scala> Qualified_Users_Ratings.show(false)
/*
+-------+-------+------+                                                        
|User ID|MovieID|Rating|
+-------+-------+------+
|2088   |3789   |5     |
|2088   |2987   |5     |
|2088   |1249   |5     |
|2088   |718    |5     |
|2088   |3931   |2     |
|2088   |3932   |5     |
|2088   |3933   |3     |
|2088   |3936   |3     |
|2088   |3937   |3     |
|2088   |2053   |4     |
|2088   |2054   |5     |
|2088   |1253   |5     |
|2088   |2993   |5     |
|2088   |1254   |5     |
|2088   |1259   |4     |
|2088   |589    |5     |
|2088   |2      |4     |
|2088   |1262   |5     |
|2088   |1264   |3     |
|2088   |2067   |4     |
+-------+-------+------+
only showing top 20 rows
*/

scala> val Qualified_Users_Avg_Rating_per_Genre = Movie_DF.join(Qualified_Users_Ratings, Movie_DF.col("Movie ID").equalTo(Qualified_Users_Ratings.col("MovieID"))).withColumn("Genre", split(col("Genre"), "\\|")).select(col("User ID"), col("Movie ID"), col("Rating"), explode(col("Genre"))).withColumnRenamed("col", "genre").groupBy("User ID", "genre").agg(avg("Rating")).orderBy(col("User ID").asc).select("User ID", "avg(rating)", "Genre").withColumnRenamed("avg(rating)", "Average score")
/*
Qualified_Users_Avg_Rating_per_Genre: org.apache.spark.sql.DataFrame = [User ID: string, Average score: double ... 1 more field]
*/

scala> Qualified_Users_Avg_Rating_per_Genre.filter(Qualified_Users_Avg_Rating_per_Genre("User ID") === "1001").show(false)
/*
+-------+------------------+-----------+                                        
|User ID|Average score     |Genre      |
+-------+------------------+-----------+
|1001   |4.5               |Film-Noir  |
|1001   |4.0               |Western    |
|1001   |3.235294117647059 |Adventure  |
|1001   |3.6842105263157894|Children's |
|1001   |2.8               |Fantasy    |
|1001   |3.5               |Horror     |
|1001   |3.9               |Animation  |
|1001   |3.4615384615384617|War        |
|1001   |4.333333333333333 |Documentary|
|1001   |3.367088607594937 |Romance    |
|1001   |3.547008547008547 |Comedy     |
|1001   |3.116279069767442 |Action     |
|1001   |3.6538461538461537|Crime      |
|1001   |3.6               |Mystery    |
|1001   |3.888888888888889 |Musical    |
|1001   |3.457627118644068 |Thriller   |
|1001   |3.8106796116504853|Drama      |
|1001   |3.9444444444444446|Sci-Fi     |
+-------+------------------+-----------+
*/

scala> Qualified_Users_Avg_Rating_per_Genre.filter(Qualified_Users_Avg_Rating_per_Genre("User ID") === "1010").show(false)
/*
+-------+------------------+-----------+                                        
|User ID|Average score     |Genre      |
+-------+------------------+-----------+
|1010   |1.6911764705882353|Children's |
|1010   |2.0294117647058822|Animation  |
|1010   |2.4318181818181817|Sci-Fi     |
|1010   |2.764705882352941 |Western    |
|1010   |2.106382978723404 |Horror     |
|1010   |2.376146788990826 |Romance    |
|1010   |3.230769230769231 |Film-Noir  |
|1010   |2.9107692307692306|Drama      |
|1010   |2.911111111111111 |Crime      |
|1010   |2.4594594594594597|Adventure  |
|1010   |3.0               |Documentary|
|1010   |1.875             |Musical    |
|1010   |2.297872340425532 |Action     |
|1010   |2.4383561643835616|Thriller   |
|1010   |2.6923076923076925|Mystery    |
|1010   |2.0789473684210527|Fantasy    |
|1010   |2.891304347826087 |War        |
|1010   |2.322314049586777 |Comedy     |
+-------+------------------+-----------+
*/












