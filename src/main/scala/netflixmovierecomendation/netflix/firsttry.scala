package netflixmovierecomendation.netflix

import org.apache.spark.sql.SparkSession
import scala.Tuple2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import org.apache.spark.mllib.recommendation.ALS

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

import org.apache.spark.mllib.recommendation.Rating



object firsttry {
  
  def main(Args:Array[String]){
    
    
    val sc=SparkSession.builder()
    .appName("netflix")
    .master("local")
    .getOrCreate()
    
    sc.sparkContext.setLogLevel("ERROR")
    
    val df=sc.read
    .option("header",true)
    .csv("resources/ratings.csv")
    
    df.show()
    val ratingsdf=df.select("userId","movieId","rating","timestamp")
    
    
    ratingsdf.show()
    
    
    val df1=sc.read
    .option("header",true)
    .csv("resources/imdbs.csv")
    
    df1.show()
    
    val moviesdf=df1.select("Rank","Title","Genre")
    
    
    moviesdf.show()
    
    ratingsdf.createOrReplaceTempView("ratings")

    moviesdf.createOrReplaceTempView("movies")
    
    
    
    val noofusers=ratingsdf.select("userId").distinct().count()
    
    val noofratings=ratingsdf.count()
    
    val noofmovies=ratingsdf.select("movieId").distinct().count()
    
    //println("no of users are ::" + noofusers)
    
    
    //println("no of ratings given are: "+ noofratings)

   // println("n of movies are :" +noofmovies)
    
    //println("Got " + noofratings + " ratings from " + noofusers + " users on " + noofmovies+ " movies.")
    
    

    val results = sc.sql("select movies.Title, movierates.maxr, movierates.minr, movierates.cntu "

+ "from(SELECT ratings.movieId,max(ratings.rating) as maxr,"

+ "min(ratings.rating) as minr,count(distinct userId) as cntu "

+ "FROM ratings group by ratings.movieId) movierates "

+ "join movies on movierates.movieId=movies.Rank "

+ "order by movierates.cntu desc") 

//results.show()

val mostActiveUsersSchemaRDD = sc.sql("SELECT ratings.userId, count(*) as ct from ratings "+ "group by ratings.userId order by ct desc limit 10")
//mostActiveUsersSchemaRDD.show() //438 is active more
val results2 = sc.sql("SELECT ratings.userId, ratings.movieId,"

+ "ratings.rating, movies.Title FROM ratings , movies "

+ "where movies.Rank=ratings.movieId"

+ " and ratings.userId=438 and ratings.rating > 4") 

//results2.show()

val splits = ratingsdf.randomSplit(Array(0.75, 0.25), seed = 12345L)

val (trainingData, testData) = (splits(0), splits(1))

val numTraining = trainingData.count()

val numTest = testData.count()

//println("Training: " + numTraining + " test: " + numTest)

val ratingsRDD = trainingData.rdd.map(row => {

val userId = row.getString(0)

val movieId = row.getString(1)

val ratings = row.getString(2)


Rating(userId.toInt, movieId.toInt, ratings.toDouble)

})

val testRDD = testData.rdd.map(row => {

val userId = row.getString(0)

val movieId = row.getString(1)

val ratings = row.getString(2)

Rating(userId.toInt, movieId.toInt, ratings.toDouble)
})

val rank = 20

val numIterations = 15

val lambda = 0.10

val alpha = 1.00 
val block = -1

val seed = 12345L

val implicitPrefs = false

val model = new ALS().setIterations(numIterations) .setBlocks(block).setAlpha(alpha)

.setLambda(lambda)

.setRank(rank) .setSeed(seed)

.setImplicitPrefs(implicitPrefs)

.run(ratingsRDD)

println("Rating:(UserID, MovieID, Rating)")

println("----------------------------------")

val topRecsForUser = model.recommendProducts(438, 600) 
for (rating <- topRecsForUser) 
{ println(rating.toString()) } 
    println("----------------------------------")

 

    
  }
  
}