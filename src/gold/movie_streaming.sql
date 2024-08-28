SELECT movie_id,
       movie_title,
       movie_platforms,
       movie_budget_million,
       movie_box_office_million,
       movie_rating,
       movie_age_rating,
       movie_metacritic_score,
       movie_runtime_minutesm,
       has_bonus_features,
       is_reboot,
       AVG(movie_rating) OVER(PARTITION BY movie_platforms) AS avg_rating_by_plataform,
       SUM(movie_metacritic_score) OVER(PARTITION BY movie_platforms) AS total_metactric_score_by_plataform,
       SUM(movie_budget_million) OVER(PARTITION BY movie_platforms) AS total_budget_by_plataform
FROM silver.movies
WHERE movie_age_rating = '{conditional}'