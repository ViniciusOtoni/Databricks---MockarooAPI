SELECT
    m.movie_title,
    d.director_name,
    m.movie_box_office_million,
    m.movie_box_office_million * 100.0 / SUM(m.movie_box_office_million) OVER (PARTITION BY d.director_id) AS box_office_percentage
FROM
    silver.movies m
JOIN
    silver.director d ON m.director_id = d.director_id
ORDER BY
    d.director_name, box_office_percentage DESC;