-- Script for data cleaning on pig (part2)
-- unzip ml-latest-small.zip

-- local mode first to debug and test queries

REGISTER /home/sweenk27/ca4022/pig-0.17.0/contrib/piggybank/java/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

movies = LOAD 'ml-latest-small/movies.csv' USING CSVExcelStorage() AS (movieId: int, title: chararray, genres: chararray);

movies_cleaned = FOREACH movies GENERATE movieId, SUBSTRING($1, 0,(int)SIZE($1) - 7) AS title, REGEX_EXTRACT($1, '.*\\((.*)\\)', 1) AS year, STRSPLIT($2, '\\|') as genres;

-- not using links.csv - no real information to be gained from it 
--  tags.csv doesn't seem relevant either as nothing is asked about feedback from users bar ratings which is found in --ratings.csv
--  as a result we will merge only ratings.csv and movies.csv (our cleaned version of this)

-- merging movies and ratings

ratings = LOAD 'ml-latest-small/ratings.csv' USING CSVExcelStorage() AS (userId: int, movieId: int, ratings: int, timestamp: long);

-- remove timestamp column as it is not necessary for our queries

 ratings_drop = FOREACH ratings generate $0, $1, $2;

 -- now lets join our two cleaned files

 merged = JOIN movies_cleaned BY movieId, ratings_drop BY movieId;

-- store our data after cleaning

STORE merged INTO 'ml-latest-small/processed_movieratings' USING CSVExcelStorage;

-- hadoop running (mapreduce)

movies = LOAD 'hdfs://localhost:9000/user/sweenk27/ml-latest-small/movies.csv' USING CSVExcelStorage() AS (movieId: int, title: chararray,  genres: chararray);

movies_cleaned = FOREACH movies GENERATE movieId, SUBSTRING($1, 0,(int)SIZE($1) - 7) AS title, REGEX_EXTRACT($1, '.*\\((.*)\\)', 1) AS year, STRSPLIT($2, '\\|') as genres;

-- same queries as local then..

STORE merged INTO 'hdfs://localhost:9000/user/sweenk27/ml-latest-small/cleaned' using CSVExcelStorage;

-- test if it works

mov = LOAD 'hdfs://localhost:9000/user/sweenk27/ml-latest-small/cleaned' USING CSVExcelStorage() AS (movieId: int, title: chararray,  year: int, genres: chararray,  userId: int, rating: int);

dump mov;