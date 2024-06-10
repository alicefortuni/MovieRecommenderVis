#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline
from neo4j import GraphDatabase


# Init Spark

spark = SparkSession.builder \
    .master("yarn") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.locality.wait.node", "0") \
    .appName("InitMovieLensRecommendation") \
    .getOrCreate()


# Load data from HDFS
ratings_path = "hdfs:///user/bigdata2022/input/ratings.csv"
movies_path = "hdfs:///user/bigdata2022/input/movies.csv"

ratings = spark.read.csv(ratings_path, header=True, inferSchema=True)
movies = spark.read.csv(movies_path, header=True, inferSchema=True) 


# EDA for ratings dataset
print("ratings dataset schema:")
ratings.printSchema()

print("First 10 rows of ratings dataset:")
ratings.show(10)

# Delete timestamp column
ratings = ratings.select("userId", "movieId", "rating")
print("First 10 rows of ratings after selection:")
ratings.show(10)

# Check duplicates
dropped_duplicates = ratings.dropDuplicates()
duplicate_count = ratings.count() - dropped_duplicates.count()
print(f"Number of duplicate records: {duplicate_count}")

#Print range for rating values
min_rating = ratings.agg({"rating": "min"}).collect()[0][0]
max_rating = ratings.agg({"rating": "max"}).collect()[0][0]
print(f"Ratings range: [{min_rating}, {max_rating}]")

# Print total number of users
num_users = ratings.select("userId").distinct().count()
print(f"Number of users: {num_users}")

# Print total number of movies
num_movies = ratings.select("movieId").distinct().count()
print(f"Number of movies: {num_movies}")

# Print total number of ratings
print("Total ratings:", ratings.count())


# EDA for movies dataset
print("movies dataset schema:")
movies.printSchema()
print("First 10 rows of movies dataset:")
movies.show(10)

# See different genres 
distinct_genres_rows=movies.select('genres').collect()
distinct_genres = set()
for row in distinct_genres_rows:
    genres_list = row['genres'].split('|') if row['genres'] else []
    distinct_genres.update(genres_list)

print(distinct_genres)



# Split data in training and test
(training, test) = ratings.randomSplit([0.8, 0.2], seed='40')

print("Total training ratings:", training.count())
print("Total test ratings:", test.count())


# Configure ALS algorithm
#als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop", maxIter=10, regParam=0.1, rank=40)
als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")

# create pipeline
pipeline = Pipeline(stages=[als])

param_grid = ParamGridBuilder() \
    .addGrid(als.rank, [10, 30, 40]) \
    .addGrid(als.regParam, [0.01, 0.1, 0.5]) \
    .addGrid(als.maxIter, [10]) \
    .build()

evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")

cross_validator = CrossValidator(estimator=pipeline,
                                 estimatorParamMaps=param_grid,
                                 evaluator=evaluator,
                                 numFolds=3)                                 



# Fit the model on training data
#model=pipeline.fit(training)
cv_model = cross_validator.fit(training)
model = cv_model.bestModel



print(model.stages[0].rank) 
print(model.stages[0]._java_obj.parent().getMaxIter()) 
print(model.stages[0]._java_obj.parent().getRegParam()) 

# Model prediction
predictions = model.transform(test)


#SCORES
#evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
mae_evaluator = RegressionEvaluator(metricName="mae", labelCol="rating", predictionCol="prediction")
mae = mae_evaluator.evaluate(predictions)

print(f"Root-mean-square error = {rmse}") 
print(f"Mean Absolute Error = {mae}")


#Save model in hdfs (Optional)
model_path = "hdfs:///user/bigdata2022/model"
model.write().overwrite().save(model_path)



#get 10 movie recommendations for users
userRecs = model.recommendForAllUsers(10).cache()  
print("userRecs dataset schema:")
userRecs.printSchema()



# Save recommendation in hdfs (Optional)
recommendation_path = "hdfs:///user/bigdata2022/output"
userRecs.write.format("parquet").option("overwrite", True).save(recommendation_path)


# Connection to Neo4j
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "bigdata"))


# Delete all from Neo4j
def delete_all(tx):
    tx.run("MATCH ()-[r]->() DELETE r")
    tx.run("MATCH (n) DELETE n")
           
with driver.session() as session:
    session.execute_write(delete_all)


# create nodes Genre for movie genres
def create_genre_nodes(tx, name):
    tx.run("MERGE (:Genre {name: $name})",
           name=name)
    
with driver.session() as session:
    for row in distinct_genres:
        session.execute_write(create_genre_nodes, row)


# create nodes for Users and Movies and relations :RATED, :BELONGS_TO
ratings_movies = ratings.join(movies, "movieId", "inner")

def create_nodes_and_relationships(tx, userId, movieId, rating, title, genres):
    tx.run("MERGE (u:User {userId: $userId}) "
           "MERGE (m:Movie {movieId: $movieId, title: $title}) "
           "MERGE (u)-[:RATED {rating: $rating}]->(m)",
           userId=userId, movieId=movieId, rating=rating, title=title)
    for genre_name in genres.split("|"):
        tx.run(
            "MATCH (m:Movie {movieId: $movieId}) "
            "MATCH (g:Genre {name: $genre_name}) "
            "MERGE (m)-[:BELONGS_TO]->(g)",
            movieId=movieId, genre_name=genre_name)

with driver.session() as session:
    for row in ratings_movies.collect():
        session.execute_write(create_nodes_and_relationships, row.userId, row.movieId, row.rating, row.title, row.genres)



# create relations :RECOMMENDED
def create_recommendations(tx, userId, recs):
    for rec in recs:
        tx.run("MATCH (u:User {userId: $userId}), (m:Movie {movieId: $movieId}) "
               "MERGE (u)-[:RECOMMENDED{rating: $rating}]->(m)",
               userId=userId, movieId=rec.movieId, rating=rec.rating)

with driver.session() as session:
    for row in userRecs.collect():
        recs = row.recommendations
        recs = recs if isinstance(recs, list) else [recs]
        session.execute_write(create_recommendations, row.userId, recs)


#close all
driver.close()
spark.stop()



