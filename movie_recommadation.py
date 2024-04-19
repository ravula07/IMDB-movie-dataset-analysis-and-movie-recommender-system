import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("MovieRecommendation").getOrCreate()

# Load your data into a PySpark DataFrame
TransformedData = spark.read.csv('E:\AMRITA\SEM-5\BDMS\Recommandation\movies.csv', header=True, inferSchema=True)

# Rename the 'score' column to 'imdb_rating'
TransformedData = TransformedData.withColumnRenamed("score", "imdb_rating")

# Calculate profit for each movie
TransformedData = TransformedData.withColumn("profit", col("gross") - col("budget"))

# Rename the 'company' column to 'production_house'
TransformedData = TransformedData.withColumnRenamed("company", "production_house")

# Create a Streamlit app
st.title("Welcome to the Movie Recommendation System!")

# Flag to hide introductory text when a recommendation type is selected
show_intro = True

# Sidebar menu for selecting recommendation type
option = st.sidebar.selectbox("Select Recommendation Type:", ["Select Option", "Movie", "Star", "Production House"])

# Option 1: Movie Recommendations
if option == "Movie":
    show_intro = False
    st.header("Movie Recommendations")
    # Dropdown menu for selecting a movie
    selected_movie = st.selectbox("Select a Movie:", ["Select a Movie"] + TransformedData.select("title").distinct().rdd.flatMap(lambda x: x).collect())
    # Display movie recommendations only if a specific movie is selected
    if selected_movie != "Select a Movie":
        # Get the genre of the selected movie
        selected_genre = TransformedData.filter(col("title") == selected_movie).select("genre").first()["genre"]
        # Recommend top 5 movies with the same genre and highest IMDb ratings
        movie_recommendations = TransformedData.filter(col("genre") == selected_genre).orderBy(col("imdb_rating").desc()).limit(5).toPandas()
        # Display movie recommendations
        st.subheader("Here are the Top-5 Recommendations for you")
        st.table(movie_recommendations[['title', 'imdb_rating']])

# Option 2: Star Recommendations
elif option == "Star":
    show_intro = False
    st.header("Movie Recommendations")
    # Dropdown menu for selecting a star
    selected_star = st.selectbox("Select a Star:", ["Select a Star"] + TransformedData.select("star").distinct().rdd.flatMap(lambda x: x).collect())
    # Display star recommendations only if a specific star is selected
    if selected_star != "Select a Star":
        # Recommend top 5 movies with the same star and highest IMDb ratings
        star_recommendations = TransformedData.filter(col("star") == selected_star).orderBy(col("imdb_rating").desc()).limit(5).toPandas()
        # Display star recommendations
        st.subheader("Here are the Top-5 Recommendations for you")
        st.table(star_recommendations[['title', 'imdb_rating']])

# Option 3: Production House Recommendations
elif option == "Production House":
    show_intro = False
    st.header("Movie Recommendations")
    # Dropdown menu for selecting a production house
    selected_production_house = st.selectbox("Select a Production House:", ["Select a Production House"] + TransformedData.select("production_house").distinct().rdd.flatMap(lambda x: x).collect())
    # Display production house recommendations only if a specific production house is selected
    if selected_production_house != "Select a Production House":
        # Recommend top 5 movies with the highest IMDb rating for the selected production house
        production_house_recommendations = TransformedData.filter(col("production_house") == selected_production_house).orderBy(col("imdb_rating").desc()).limit(5).toPandas()
        # Display production house recommendations
        st.subheader("Here are the Top-5 Recommendations for you")
        st.table(production_house_recommendations[['title', 'imdb_rating']])

# Default option
else:
    if show_intro:
        st.write("Discover your next favorite movie, explore stars, and find top production houses.")
        st.write("The movie data in this application spans the period from 1980 to 2000.")
        st.write("")  # Add an empty line for separation
    st.warning("Please select a recommendation type from the sidebar.")
