-- Script for data cleaning on pig (part2)

-- not using links.csv - no real information to be gained from it 
--  tags.csv doesn't seem relevant either as nothing is asked about feedback from users bar ratings which is found in --ratings.csv
--  as a result we will merge only ratings.csv and movies.csv (our cleaned version of this)

-- hadoop running (mapreduce)

REGISTER /home/sweenk27/ca4022/pig-0.17.0/contrib/piggybank/java/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

movies = LOAD 'hdfs://localhost:9000/user/sweenk27/ml-latest-small/movies.csv' USING CSVLoader() AS (movieId: int, title: chararray,  genres: chararray);

movies_cleaned = FOREACH movies GENERATE movieId, SUBSTRING($1, 0,(int)SIZE($1) - 7) AS title, REGEX_EXTRACT($1, '.*\\((.*)\\)', 1) AS year, STRSPLIT($2, '\\|') as genres;

ratings = LOAD 'ml-latest-small/ratings.csv' using PigStorage(',') AS (userId: int, movieId: int, ratings: int, timestamp: long);

ratings_drop = FOREACH ratings generate userId, movieId, ratings;

merged = JOIN movies_cleaned BY movieId, ratings_drop BY movieId;

STORE merged INTO 'ml-latest-small/processed_movieratings_fix' USING PigStorage('|');

-- test if it works: dump mov;
