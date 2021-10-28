-- movies.csv로부터 파일 로드
movies = LOAD '/user/maria_dev/movies.csv'
            USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER')
            AS (movieId:int, title:chararray, genres:chararray);
-- ratings.csv로부터 파일 로드
ratings = LOAD '/user/maria_dev/ratings.csv'
            USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER')
            AS (userId:int, movieId:int, rating:float, timestamp:long);

-- genre가 여러 개인 경우, '|'를 기준으로 나눠준다.
splitgenres_data = FOREACH movies GENERATE movieId, title, FLATTEN(STRSPLITTOBAG(genres,'\\|')) as genres;
-- 둘의 공통된 속성인 movieId를 통해 ratings와 JOIN 시켜준다. JOIN ON 사용
joined_data = JOIN splitgenres_data BY (movieId), ratings BY (movieId);
-- genres를 기준으로 그룹을 묶는다. GROUP BY 사용
grouped_data = GROUP joined_data BY genres;
-- result에는 각 장르별로 장르명, 평점 COUNT값, 평점 AVG 값이 들어있다.
result = FOREACH grouped_data GENERATE  $0 as genres, COUNT(joined_data.rating) as rating_cnt, AVG(joined_data.rating) as rating_avg;
-- 평점 횟수 (COUNT한 값)을 기준으로 내림차순 정렬을 해준다. ORDER BY 사용
ordered_data = ORDER result BY rating_cnt desc;
-- DUMP로 출력해본다.
DUMP ordered_data;
-- 결과 파일로 저장
STORE ordered_data INTO '/user/maria_dev/best_genre'
   USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'WRITE_OUTPUT_HEADER');
