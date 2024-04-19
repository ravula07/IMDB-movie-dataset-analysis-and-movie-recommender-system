
    import org.apache.spark.sql.{SaveMode, SparkSession, functions}
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.Window

    import java.util.Properties

    // Starting a Spark session
    val spark = SparkSession.builder
      .appName("PreprocessAndSave")
      .config("spark.master", "local")
      .getOrCreate()

    // Importing the dataset
    val input_path = "D:/SEM - 5/DBMS/PROJECT/BDMS PROJECT/src/main/scala/movies.csv"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    // Finding attributes with null values
    val columnsWithNull = df.columns.filter(colName => df.filter(col(colName).isNull).count() > 0)
    columnsWithNull.foreach(println)

    // Removing samples with null values in specific attributes
    val columnsToCheck = Seq(
      "title", "rating", "genre", "released",
      "score", "votes", "director", "star",
      "country", "budget", "gross", "company", "runtime"
    )
    val cleanedDataNull = df.na.drop("any", columnsToCheck)

    // Checking for duplicate values
    val duplicateCount = cleanedDataNull.count() - df.dropDuplicates(df.columns).count()
    if (duplicateCount > 0) {
      println(s"Number of duplicate values: $duplicateCount")
    } else {
      println("No duplicate values found.")
    }

    // Identify attributes with zero values
    val columnsWithZeroValues = cleanedDataNull.columns.filter(colName => cleanedDataNull.filter(col(colName) === "0").count() > 0)
    columnsWithZeroValues.foreach(println)

    // Remove samples with zero values in identified attributes
    val dfWithoutZeroValues = columnsWithZeroValues.foldLeft(cleanedDataNull) { (df, colName) =>
        df.filter(col(colName) =!= "0")
    }

    val TransformedData = dfWithoutZeroValues

    //-----------------------------ANALYSIS------------------------------------------//

    // 1. Top 10 movies based on IMDB scores //
    val result1 = TransformedData
      .select("title", "score")
      .filter(expr("score >= 8.0"))
      .orderBy(col("score").desc)
      .limit(10)

   result1.show()

    // 2. Top 10 genres based on peoples votes
    val result2 = TransformedData
    .select("title", "votes")
    .orderBy(col("votes").desc)
    .limit(10)

    result2.show()

    // 3.Top 10 liked genres based on the average of IMDB scores//
    val result3 = TransformedData
    .groupBy("genre")
    .agg(avg("score").alias("genre_score"))
    .orderBy(col("genre_score").desc)
    .limit(10)

    result3.show()


    //4. Top 10 successful directors based on the average of IMDB scores//
    val result4 = TransformedData
    .groupBy("director")
    .agg(avg("score").alias("director_score"))
    .orderBy(col("director_score").desc)
    .limit(10)

    result4.show()


    //5. Top 10 successful stars based on the average of IMDB scores
    val result5 = TransformedData
    .groupBy("star")
    .agg(avg("score").alias("actor_score"))
    .orderBy(col("actor_score").desc)
    .limit(10)

    result5.show()


    //6. Companies producing the most movies along with their average scores
    val result6 = TransformedData
    .groupBy("company")
    .agg(
      count("*").alias("movie_count"), avg("score").alias("avg_imdb_score"))
    .orderBy(col("movie_count").desc)
    .limit(10)

    result6.show()

    //7. Most directed genres with average scores
    val result7 = TransformedData
      .groupBy("genre")
      .agg(count("*").alias("movie_count"), avg("score").alias("avg_imdb_score"))
      .orderBy(col("movie_count").desc)
      .limit(10)

    result7.show()


    //8. Director with the most movies along with their average scores
    val result8 = TransformedData
      .groupBy("director")
      .agg(count("*").alias("director_count"), avg("score").alias("avg_imdb_score"))
      .orderBy(col("director_count").desc)
      .limit(10)

    result8.show()


    //9. Actors who appeared in the most movies along with their average scores
    val result9 = TransformedData
      .groupBy("star")
      .agg(count("*").alias("actor_count"), avg("score").alias("avg_imdb_score"))
      .orderBy(col("actor_count").desc)
      .limit(10)

    result9.show()


    //10. Companies which made the most profit over the span of 20 years
    val result10 = TransformedData
      .groupBy("company")
      .agg(sum(col("gross") - col("budget")).alias("total_profit"))
      .orderBy(col("total_profit").desc)
      .limit(10)

    result10.show()

    // 11. People's opinion on long movies
    val result11 = TransformedData
      .filter(col("runtime").isNotNull && col("votes").isNotNull)
      .select(
          when(col("runtime") <= avg("runtime").over(), "Shorter or equal to average")
            .when(col("runtime") > avg("runtime").over(), "Longer than average")
            .otherwise("Unknown").alias("runtime_category"),
          col("votes")
      )
      .groupBy("runtime_category")
      .agg(avg("votes").alias("avg_votes"))
      .orderBy(col("avg_votes").desc)

    result11.show()

    //12. Average scores of longer movies
    val result12 = TransformedData
      .select(
          when(col("runtime") <= avg("runtime").over, "Shorter or equal to average")
            .when(col("runtime") > avg("runtime").over, "Longer than average")
            .otherwise("Unknown").alias("runtime_category"),
          col("score")
      )
      .groupBy("runtime_category")
      .agg(avg("score").alias("avg_imdb_score"))
      .orderBy(col("avg_imdb_score").desc)

    result12.show()

    //13. Avg. runtime of the movies directed by the most successful directors
    val windowSpec = Window.orderBy(col("director_score").desc)

    val rankedDirectorsDF = TransformedData
      .groupBy("director")
      .agg(avg("score").alias("director_score"), avg("runtime").alias("avg_runtime"))
      .withColumn("director_rank", row_number().over(windowSpec))

    val result13 = rankedDirectorsDF
      .filter(col("director_rank") <= 10)
      .select("director", "director_score", "avg_runtime")

    result13.show()

    //14. Countries which made the most profit
    val result14 = TransformedData
      .groupBy("country")
      .agg(sum(col("gross") - col("budget")).alias("total_profit"))
      .orderBy(col("total_profit").desc)
      .limit(10)

    result14.show()

    //15. Countries that produced the best films based on average IMDb score
    val result15 = TransformedData
      .groupBy("country")
      .agg(avg("score").alias("avg_imdb_score"))
      .orderBy(col("avg_imdb_score").desc)
      .limit(10)

    result15.show()

    //16. Movies count released by each country
    val result16 = TransformedData
      .groupBy("country")
      .agg(count("*").alias("movie_count"))
      .orderBy(col("movie_count").desc)

    result16.show()

    //17. Actors preferred by successful companies
    val result17 = TransformedData
      .groupBy("company", "star")
      .agg(sum(col("gross") - col("budget")).alias("total_profit"))
      .orderBy(col("total_profit").desc)
      .limit(10)

    result17.show()

    //18. Directors preferred by most successful companies
    val result18 = TransformedData
      .groupBy("company", "director")
      .agg(sum(col("gross") - col("budget")).alias("total_profit"))
      .orderBy(col("total_profit").desc)
      .limit(10)

    result18.show()

    //19. Average run time of movies per year
    val result19 = TransformedData
      .filter(col("released").isNotNull && col("runtime").isNotNull)
      .select(
          year(expr("TO_DATE(released, 'dd-MMM-yy')")).alias("released_year"),
          col("runtime")
      )
      .groupBy("released_year")
      .agg(avg("runtime").alias("avg_runtime"))
      .orderBy("released_year")

    result19.show()

    //20. Trend of profits earned per year
    val result20 = TransformedData
      .select(
          year(expr("TO_DATE(released, 'dd-MMM-yy')")).alias("released_year"),
          (col("gross") - col("budget")).alias("profit")
      )
      .groupBy("released_year")
      .agg(sum("profit").alias("total_earnings"))
      .orderBy("released_year")

    result20.show()

    //22. Movies released per year
    val result22 = TransformedData
      .groupBy(substring(col("released"), -2, 2).alias("released"))
      .agg(count("*").alias("movie_count"))
      .orderBy("released")

    result22.show()


    //23. Movies released per month in span of 20 years
    val releaseMonthColumn = expr("SUBSTRING_INDEX(SUBSTRING_INDEX(released, '-', 2), '-', -1)")
    val result23 = TransformedData
      .groupBy(releaseMonthColumn)
      .agg(count("*").alias("movie_count"))
      .orderBy(to_date(concat(lit("01-"), releaseMonthColumn, lit("-2000")), "dd-M-yyyy"))

    result23.show()

    //24. Avg scores of movies with different age certificates
    val result24 = TransformedData
      .groupBy("rating")
      .agg(round(avg("score"), 2).alias("avg_imdb_score"))
      .orderBy(desc("avg_imdb_score"))

    result24.show()

    //25. Total Movies released from each age certifications
    val result25 = TransformedData
      .groupBy("rating")
      .agg(count("*").alias("certification_count"))
      .orderBy(desc("certification_count"))
      .limit(9)

    result25.show()


    //26. Most successful directors based on the movie's collection
    val result26 = TransformedData
      .groupBy("director")
      .agg(sum(col("gross") - col("budget")).alias("total_profit"))
      .orderBy(desc("total_profit"))
      .limit(10)

    result26.show


    //27. Quality of movies over time

    val filteredData = TransformedData
      .filter(year(to_date(col("released"), "dd-MMM-yy")).between(1980, 2000))

    val result27 = TransformedData
      .withColumn("released", year(to_date(col("released"), "dd-MMM-yy")))
      .groupBy("released")
      .agg(avg("score").alias("avg_imdb_score"))
      .orderBy("released")

    result27.show()

    // Stop the Spark session
    spark.stop()