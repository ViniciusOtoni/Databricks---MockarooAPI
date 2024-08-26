SELECT 
  producer_id,
  producer_name,
  producer_age,
  producer_country,
  producer_gender
FROM hive_metastore.bronze.movies
ORDER BY producer_id