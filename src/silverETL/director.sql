SELECT 
  director_id,
  director_name,
  director_age,
  director_country,
  director_gender
FROM hive_metastore.bronze.movies
ORDER BY director_id