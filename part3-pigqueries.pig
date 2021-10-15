
-- local

 --part1 

movieratings = LOAD 'file:/home/sweenk27/ca4022/pig-0.17.0/ml-latest-small/processed_movieratings' USING CSVExcelStorage() AS (movieId: int, title: chararray,  year: int, genres: chararray,  userId: int, rating: int);

grouped = GROUP movieratings BY title;

count = FOREACH grouped GENERATE $0, COUNT(movieratings) as countratings;

B = ORDER count BY countratings DESC;

C = LIMIT B 5;

DUMP C;

-- part3

user = GROUP movieratings BY userId;

avguser = FOREACH user GENERATE group as userId, AVG(movieratings.rating) as avgrating;

D = ORDER avguser BY avgrating DESC;

E = LIMIT D 10;

DUMP E;