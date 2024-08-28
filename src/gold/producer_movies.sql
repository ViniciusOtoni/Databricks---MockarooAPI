WITH ProducerMovies AS (
    SELECT
        m.movie_title,
        p.producer_name,
        m.movie_release_date,
        ROW_NUMBER() OVER (PARTITION BY p.producer_id ORDER BY m.movie_release_date DESC) AS rn_recent,
        ROW_NUMBER() OVER (PARTITION BY p.producer_id ORDER BY m.movie_release_date ASC) AS rn_oldest
    FROM
        silver.movies m
    JOIN
        silver.producer p ON m.producer_id = p.producer_id
)
SELECT
    producer_name,
    movie_title AS most_recent_movie
FROM
    ProducerMovies
WHERE
    rn_recent = 1

UNION ALL

SELECT
    producer_name,
    movie_title AS oldest_movie
FROM
    ProducerMovies
WHERE
    rn_oldest = 1
ORDER BY
    producer_name;
