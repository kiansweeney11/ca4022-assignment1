
-- local

-- part1 

movieratings = LOAD 'file:/home/sweenk27/ca4022/pig-0.17.0/ml-latest-small/processed_movieratings' USING CSVExcelStorage() AS (movieId: int, title: chararray,  year: int, genres: chararray,  userId: int, rating: int);

grouped = GROUP movieratings BY title;

count = FOREACH grouped GENERATE $0, COUNT(movieratings) as countratings;

B = ORDER count BY countratings DESC;

C = LIMIT B 5;

DUMP C;

-- part2

moviesavg = GROUP movieratings BY (movieId, title);

avgmoviesavg = FOREACH moviesavg GENERATE group as title, AVG(movieratings.rating) as avgrating;

D = ORDER avgmoviesavg BY avgrating DESC;

E = LIMIT D 10;

DUMP E;

-- there appears to be a lot of movies with an average rating of 5 star. let's check how many exactly

filtered = FILTER avgmoviesavg by avgrating == 5;

no5star = FOREACH (GROUP filtered ALL) GENERATE COUNT(filtered);

dump no5star;

-- 296 movies have an average rating of 5 star!

-- part3

user = GROUP movieratings BY userId;

avguser = FOREACH user GENERATE group as userId, AVG(movieratings.rating) as avgrating;

D = ORDER avguser BY avgrating DESC;

E = LIMIT D 10;

DUMP E;

-- mapreduce

-- q3 part1

REGISTER /home/sweenk27/ca4022/pig-0.17.0/contrib/piggybank/java/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

mov = LOAD 'hdfs://localhost:9000/user/sweenk27/ml-latest-small/processed_movieratings_fix' USING CSVExcelStorage() AS (movieId: int, title: chararray,  year: int, genres: chararray,  userId: int, rating: int);

-- same steps as local for q3 part1 

STORE C INTO 'hdfs://localhost:9000/user/sweenk27/ml-latest-small/q3part1' using CSVExcelStorage;

-- q3 part2



-- q3 part3

mov = LOAD 'hdfs://localhost:9000/user/sweenk27/ml-latest-small/processed_movieratings_fix' USING CSVExcelStorage() AS (movieId: int, title: chararray,  year: int, genres: chararray,  userId: int, mov2: int, rating: int );

user = GROUP mov BY userId;

avguser = FOREACH user GENERATE group as userId, AVG(mov.rating) as avgrating;

D = ORDER avguser BY avgrating DESC;

E = LIMIT D 10;

-- DUMP E, user id 53 has highest average rating with a 5 star average.