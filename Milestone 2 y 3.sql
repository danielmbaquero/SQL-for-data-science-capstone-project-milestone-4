-- Databricks notebook source
USE default;

-- COMMAND ----------

SELECT
  COUNT(DISTINCT id)
FROM
  tweets;

-- COMMAND ----------

SELECT 
  COUNT(DISTINCT id) AS num_users,
  AVG(favourites_count) AS average_favourites,
  AVG(followers_count)  AS average_followers,
  MAX(favourites_count) AS MAX_favourites,
  MAX(followers_count)  AS MAX_followers,
  MIN(favourites_count) AS MIN_favourites,
  MIN(followers_count)  AS MIN_followers
FROM
  users;

-- COMMAND ----------

SELECT
  u.screen_name          AS name,
  MAX(u.followers_count) AS followers,
  MAX(s.amount)          AS self_replies,
  SUM(i.amount)          AS replies
FROM
    self_interaction AS s
  INNER JOIN
    users AS u
  INNER JOIN
    interaction AS i
  ON
    s.user_id = u.id AND i.user_id = u.id
GROUP BY
  u.screen_name
--ORDER BY 
--  followers DESC
--LIMIT 20;

-- COMMAND ----------

SELECT
  SUM(self_replies),
  SUM(replies)
FROM
  (SELECT
    u.screen_name          AS name,
    MAX(u.followers_count) AS followers,
    MAX(s.amount)          AS self_replies,
    SUM(i.amount)          AS replies
  FROM
      self_interaction AS s
    INNER JOIN
      users AS u
    INNER JOIN
      interaction AS i
    ON
      s.user_id = u.id AND i.user_id = u.id
  GROUP BY
    u.screen_name
  ORDER BY 
    followers DESC
  LIMIT 100);

-- COMMAND ----------

SELECT 
  --u.screen_name          AS name,
  MAX(u.followers_count) AS followers,
  SUM(t.favorite_count)  AS favorite_count
FROM
  users u INNER JOIN tweets t
  ON u.id = t.user_id
GROUP BY
  u.screen_name
ORDER BY
  followers DESC
--LIMIT 
--  50;

-- COMMAND ----------

SELECT
  u.screen_name          AS name,
  MAX(u.followers_count) AS followers,
  COUNT(DISTINCT t.id)   AS total_tweets
FROM
  users u INNER JOIN tweets t
  ON u.id = t.user_id
GROUP BY
  u.screen_name
--ORDER BY
--  followers DESC;

-- COMMAND ----------

SELECT
  ((tot_sum - (amt_sum * act_sum / _count)) / sqrt((amt_sum_sq - pow(amt_sum, 2.0) / _count) * (act_sum_sq - pow(act_sum, 2.0) / _count))) AS Corr_Coef_Using_Pearson
FROM
  (SELECT 
      sum(followers) AS amt_sum,
      sum(favorite_count) AS act_sum,
      sum(followers * followers) AS amt_sum_sq,
      sum(favorite_count * favorite_count) AS act_sum_sq,
      sum(followers * favorite_count) AS tot_sum,
      count(*) as _count
  FROM
   (SELECT 
      u.screen_name          AS name,
      MAX(u.followers_count) AS followers,
      SUM(t.favorite_count)  AS favorite_count
    FROM
      users u INNER JOIN tweets t
      ON u.id = t.user_id
    GROUP BY
      u.screen_name))

-- COMMAND ----------

SELECT
  verified,
  COUNT(verified) AS verified_num,
  SUM(followers_count) AS total_followers
FROM
  users
GROUP BY
  verified
