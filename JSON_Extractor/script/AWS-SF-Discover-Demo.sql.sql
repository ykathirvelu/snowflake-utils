--
-- Setup the environment to setup for the workshop
--
USE ROLE accountadmin;

select '𝟏𝟐𝟑' + 𝟏;

-- Create the workshopadmin and workshopuser roles and adding this role to the login user;
CREATE ROLE workshopadmin;
CREATE ROLE workshopuser;
GRANT ROLE workshopadmin TO USER sfadmin;  -- Change to your login
GRANT ROLE workshopuser TO USER sfadmin;   -- Change to your login

-- Grant the roles to the sysadmin role this is best practice
GRANT ROLE workshopadmin TO ROLE sysadmin;
GRANT ROLE workshopuser TO ROLE sysadmin;

-- Grant the account level permissions needed for the lab to the workshopadmin role
GRANT CREATE DATABASE ON ACCOUNT TO ROLE workshopadmin;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE workshopadmin;
GRANT CREATE SHARE ON ACCOUNT TO ROLE workshopadmin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE workshopadmin;
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE workshopadmin;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE workshopadmin;

USE ROLE workshopadmin;
CREATE OR REPLACE DATABASE MOVIELENS ;
GRANT USAGE ON DATABASE MOVIELENS TO workshopuser;

CREATE SCHEMA movielens.movies;
GRANT USAGE ON SCHEMA movielens.movies TO ROLE workshopuser;
GRANT SELECT,INSERT,delete, update ON FUTURE TABLES IN SCHEMA movielens.movies to role workshopuser;

USE SCHEMA movielens.movies;

----------------

CREATE OR REPLACE WAREHOUSE WORKSHOPWH WITH WAREHOUSE_SIZE = 'XSMALL' 
AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

GRANT USAGE ON WAREHOUSE workshopwh TO ROLE workshopuser;

------------------

CREATE OR REPLACE TABLE movies_raw  (
    movieid int,
    title varchar,
    genres varchar
);

CREATE OR REPLACE TABLE ratings_raw (
    userid int,
    movieid int,
    rating float,
    timestamp timestamp_ntz,
    firstname varchar,
    lastname varchar,
    street varchar,
    city varchar,
    state varchar,
    postcode varchar,
    country varchar,
    email varchar,
    phonenumber varchar
);

----------------------------------

--
-- Handling PII data
--
-- Tags and Masking
--
USE ROLE workshopadmin;
CREATE TAG email;
CREATE TAG name;
CREATE TAG phone;
CREATE TAG address;

-------------------------

CREATE MASKING POLICY email AS (val STRING) RETURNS STRING ->
    CASE
      WHEN current_role() IN ('WORKSHOPADMIN') THEN val
      ELSE 'road_runner'  || substr(val,charindex('@', val))
    END
;

CREATE MASKING POLICY phone AS (val STRING) RETURNS STRING ->
    CASE
        WHEN current_role() IN ('WORKSHOPADMIN') THEN val
        ELSE '0500 123 456'
    END
;

CREATE MASKING POLICY address AS (val STRING) RETURNS STRING ->
    CASE
        WHEN current_role() IN ('WORKSHOPADMIN') THEN val
        ELSE '55 Main Street'
    END
;

CREATE MASKING POLICY name AS (val STRING) RETURNS STRING ->
    CASE
        WHEN current_role() IN ('WORKSHOPADMIN') THEN val
        ELSE '**********'
    END
;

----------------------------------

ALTER TAG email SET MASKING POLICY email;
ALTER TAG phone SET MASKING POLICY phone;
ALTER TAG address SET MASKING POLICY address;
ALTER TAG name SET MASKING POLICY name;

ALTER TABLE ratings_raw MODIFY COLUMN email SET TAG email = 'True';
ALTER TABLE ratings_raw MODIFY COLUMN phonenumber SET TAG phone = 'True';
ALTER TABLE ratings_raw MODIFY COLUMN street SET TAG address = 'True';
ALTER TABLE ratings_raw MODIFY COLUMN firstname SET TAG name = 'True';
ALTER TABLE ratings_raw MODIFY COLUMN lastname SET TAG name = 'True';


------------------------------------

CREATE TABLE movies_curated
(
  movieid number,
  title varchar,
  release integer
);

CREATE TABLE genres_curated
(
  genresid number autoincrement start 1 increment 1,
  genres varchar
);

CREATE TABLE movies_genres_curated
(
  genresid number,
  movieid number
);

CREATE TABLE ratings_curated
(
    userid int,
    movieid int,
    rating float,
    timestamp timestamp_ntz
);

CREATE TABLE users_curated
(
    userid int,
    firstname varchar,
    lastname varchar,
    street varchar,
    city varchar,
    state varchar,
    postcode varchar,
    country varchar,
    email varchar,
    phonenumber varchar
);

---------------------------------

ALTER TABLE users_curated MODIFY COLUMN email SET TAG email = 'True';
ALTER TABLE users_curated MODIFY COLUMN phonenumber SET TAG phone = 'True';
ALTER TABLE users_curated MODIFY COLUMN street SET TAG address = 'True';
ALTER TABLE users_curated MODIFY COLUMN firstname SET TAG name = 'True';
ALTER TABLE users_curated MODIFY COLUMN lastname SET TAG name = 'True';




-----------------------


-- Setup the snowflake context for the new worksheet
use role workshopadmin;
use database movielens;
use schema movies;
use warehouse workshopwh;

CREATE OR REPLACE FILE FORMAT movielens_ffmt
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = gzip;

-----------------------------------------

--
-- Scale up the warehouse to load the data
--
ALTER WAREHOUSE workshopwh SET WAREHOUSE_SIZE = LARGE;


-- movies data
COPY INTO movies_raw
  FROM s3://jhs-sf-aws-bucket/movies.csv.gz
  FILE_FORMAT = (FORMAT_NAME = 'movielens_ffmt');

  ---------=================

  -- ratings data
COPY INTO ratings_raw
  FROM s3://jhs-sf-aws-bucket/ratings.csv.gz
  FILE_FORMAT = (FORMAT_NAME = 'movielens_ffmt');

-- Scale back down the
ALTER WAREHOUSE workshopwh SET WAREHOUSE_SIZE = SMALL;


---=============================

SELECT * FROM movies_raw LIMIT 10;

SELECT * FROM ratings_raw LIMIT 10;

-- ratings_curated
INSERT INTO ratings_curated (userid, movieid, rating, timestamp)
SELECT userid, movieid, rating, timestamp
FROM ratings_raw;

-- users_curated
INSERT INTO users_curated(userid, firstname, lastname, street, city, state, postcode, country, email, phonenumber)
SELECT DISTINCT(userid), firstname, lastname, street, city, state, postcode, country, email, phonenumber
FROM ratings_raw
GROUP BY ALL;

-- Insert the unique generes, with genresid being an autoincrement column
INSERT INTO  genres_curated(genres)
SELECT distinct value
FROM movies_raw, LATERAL SPLIT_TO_TABLE(movies_raw.genres, '|');

SELECT * FROM genres_curated;

-- Insert the curated movied data
INSERT INTO  movies_curated
SELECT movieid,  substr(title,0,regexp_instr(title, '\([0-9]{4}\)')-2) as title,
    regexp_substr(title, '([0-9]{4})') as myear
FROM movies_raw
WHERE myear is not null;

SELECT * FROM movies_curated;

-------------**********************************

--
-- movie_genres_curated
--

-- Use a temprorary table to store the movieid and genres
CREATE OR REPLACE TEMPORARY TABLE movie_genres_tmp
AS
  SELECT movieid, value as genres
  FROM movies_raw, LATERAL SPLIT_TO_TABLE(movies_raw.genres, '|');

-- Now how do we get the genresid for the movie_genres_curated table
SELECT m.movieid, g.genresid
FROM movie_genres_tmp m, genres_curated g
WHERE m.genres = g.genres
LIMIT 10;

-- Insert the data into the movies_genres_curated table using
-- the above select
INSERT INTO movies_genres_curated
  SELECT m.movieid, g.genresid
  FROM movie_genres_tmp m, genres_curated g
  WHERE m.genres = g.genres;

  SELECT * FROM movies_genres_curated limit 5;

  -- ratings table
USE ROLE workshopadmin;
SELECT * FROM movies.ratings_raw LIMIT 5;

USE ROLE workshopuser;
SELECT * FROM movies.ratings_raw LIMIT 5;

----------###############################################33


use role workshopadmin;
use database movielens;
use schema movies;
use warehouse workshopwh;

CREATE OR REPLACE TABLE interactions
AS
SELECT
    userid as USER_ID,
    movieid as ITEM_ID,
    DATE_PART('EPOCH_SECOND', timestamp) AS TIMESTAMP,
    CASE
        WHEN rating > 3 THEN 'watch'
        WHEN rating > 1 THEN 'click'
    END AS EVENT_TYPE
FROM ratings_curated SAMPLE(10)
WHERE rating > 1;

----------------$$$$$$$$$$$$$$$$$$$$$$$$$$

--
--Create the storage integration
--
USE ROLE workshopadmin;
USE SCHEMA movielens.movies;

CREATE OR REPLACE STORAGE INTEGRATION workshop_s3_integration
TYPE = EXTERNAL_STAGE
 STORAGE_PROVIDER = S3
 ENABLED = TRUE
 STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::944840454595:role/snowflake-integration'
 STORAGE_ALLOWED_LOCATIONS = ('s3://serverless-recommendations-944840454595/train/');

DESCRIBE INTEGRATION workshop_s3_integration;

STORAGE_AWS_IAM_USER_ARN		arn:aws:iam::825765398327:user/hmix0000-s
STORAGE_AWS_ROLE_ARN	   	    arn:aws:iam::944840454595:role/snowflake-integration
STORAGE_AWS_EXTERNAL_ID	    	RTB36869_SFCRole=1381_Clbzi3LqadHufKCVrokWqd/l0wU=
STORAGE_ALLOWED_LOCATIONS		s3://serverless-recommendations-944840454595/train/


-- Create the Stage using the storage integration created
GRANT USAGE ON INTEGRATION workshop_s3_integration TO ROLE workshopuser;
GRANT USAGE ON INTEGRATION workshop_s3_integration TO ROLE workshopadmin;

CREATE OR REPLACE STAGE workshop_stage
  url = 's3://serverless-recommendations-944840454595/train/'
  storage_integration = workshop_s3_integration;

GRANT USAGE ON STAGE workshop_stage TO ROLE workshopuser;

list @workshop_stage;

COPY INTO @workshop_stage/interactions.csv
FROM interactions FILE_FORMAT = (TYPE = CSV COMPRESSION='NONE')
HEADER = true
SINGLE = true
MAX_FILE_SIZE = 4000000000;

COPY INTO @workshop_stage/uids.json
FROM (
    SELECT OBJECT_CONSTRUCT('userId', userid::STRING) AS json_data
    FROM (
        SELECT DISTINCT userid
        FROM ratings_curated
    )
)
FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'NONE')
SINGLE = true
MAX_FILE_SIZE = 4000000000;

----------------Lab4 Start

USE ROLE workshopadmin;
USE WAREHOUSE workshopwh;
GRANT SELECT ON TABLE MOVIELENS.MOVIES.INTERACTIONS TO ROLE workshopadmin;

CREATE TABLE movies_dashboard AS
SELECT
    mc.movieid AS movie_id,
    mc.title AS movie_title,
    mc.release AS movie_release_year,
    gc.genres AS genre,
    rcr.rating AS user_rating,
    rcr.timestamp AS rating_timestamp,
    ucr.userid AS user_id,
    ucr.firstname AS user_firstname,
    ucr.lastname AS user_lastname,
    ucr.city AS user_city,
    ucr.state AS user_state,
    ucr.country AS user_country,
    ucr.email AS user_email,
    ucr.phonenumber AS user_phonenumber,
    i.timestamp AS interaction_timestamp,
    i.event_type AS interaction_type
FROM
    movies.movies_curated mc
LEFT JOIN
    movies.movies_genres_curated mgc ON mc.movieid = mgc.movieid
LEFT JOIN
    movies.genres_curated gc ON mgc.genresid = gc.genresid
LEFT JOIN
    movies.ratings_curated rcr ON mc.movieid = rcr.movieid
LEFT JOIN
    movies.users_curated ucr ON rcr.userid = ucr.userid
LEFT JOIN
    movies.interactions i ON ucr.userid = i.user_id AND mc.movieid = i.item_id
WHERE genre IS NOT NULL
AND interaction_timestamp IS NOT NULL
AND interaction_type IS NOT NULL;

----------arn:aws:sso:::instance/ssoins-7223e983412a2294 --Amazon Q Application
-----------https://us-east-1.console.aws.amazon.com/amazonq/business/applications/a908a610-ac22-4948-a842-52434fc24bb9/details?region=us-east-1
-------------https://4xnzfpa5.chat.qbusiness.us-east-1.on.aws/  ---Amazon Q Deployed URL
-------------arn:aws:iam::944840454595:role/service-role/QBusiness-WebExperience-3u5fj

drop table MOVIELENS.MOVIES.TEST_TBL01;

drop table MOVIELENS.DEV.TEST_TBL03;

show grants on user sfadmin;

alter table MOVIELENS.MOVIES.TEST_TBL01 ALTER COLUMN Name SET NOT NULL;

insert into MOVIELENS.MOVIES.TEST_TBL01
select 1,'Yoga',current_timestamp

