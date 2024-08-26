SELECT 
  movie_id,
  title as movie_title,
  genre as movie_genre,
  platforms as movie_platforms,
  release_date as movie_release_date,
  budget_million as movie_budget_million,
  box_office_million as movie_box_office_million,
  rating as movie_rating,
  has_sequel,
  is_animated,
  has_post_credits_scene,
  languages_supported as movie_languages_supported,
  CASE 
    WHEN age_rating = 'G' THEN 'L'
    WHEN age_rating = 'PG' THEN '12'
    WHEN age_rating = 'PG-13' THEN '16'
    WHEN age_rating = 'R' THEN '18'
  END AS movie_age_rating,
  metacritic_score as movie_metacritic_score,
  runtime_minutes as movie_runtime_minutesm,
  has_bonus_features,
  is_reboot
FROM hive_metastore.bronze.movies
ORDER BY movie_id