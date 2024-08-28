SELECT
    m.movie_title,
    d.director_name,
    m.movie_release_date,
    m.movie_box_office_million,
    AVG(m.movie_box_office_million) OVER (
        PARTITION BY d.director_id ORDER BY m.movie_release_date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_box_office
FROM
    silver.movies m
JOIN
    silver.director d ON m.director_id = d.director_id
ORDER BY
    d.director_name, m.movie_release_date;
