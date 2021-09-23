USE pset1;

/* Q1 */
SELECT bid, bname, counts
FROM boats
INNER JOIN (
    SELECT bid, COUNT(*) AS counts
    FROM reserves
    GROUP BY bid
    HAVING counts > 0
) AS temp1
USING(bid);

/* Q2 */
SELECT sid, sname
FROM (
    SELECT sid, sname, COUNT(*) AS counts
    FROM (
      SELECT DISTINCT sid, sname, bid
      FROM boats
      INNER JOIN (
        SELECT *
        FROM sailors
        INNER JOIN reserves
        USING(sid)
      ) AS temp1
      USING(bid)
      WHERE color = 'red'
    ) AS temp2
    GROUP BY sid, sname
) AS temp3
INNER JOIN (
  SELECT COUNT(DISTINCT bid) AS max_count
  FROM boats
  WHERE color = 'red'
) AS temp4
ON counts = max_count;

/* Q3 */
SELECT sid, sname
FROM sailors
INNER JOIN (
    SELECT sid
    FROM reserves
    WHERE sid NOT IN (
        SELECT sid
        FROM reserves
        INNER JOIN (
            SELECT bid
            FROM boats
            WHERE color != 'red'
        ) AS temp1
        USING(bid)
    )
) AS temp2
USING(sid);

/* Q4 */
SELECT bid, bname
FROM boats
INNER JOIN (
    SELECT bid
    FROM (
        SELECT bid, COUNT(*) AS counts
        FROM reserves
        GROUP BY bid
    ) AS temp1
    WHERE counts = (
        SELECT MAX(counts)
        FROM (
            SELECT COUNT(*) AS counts
            FROM reserves
            GROUP BY bid
        ) AS temp2
    )
) AS temp3
USING(bid);

/* Q5 */
SELECT sid, sname
FROM sailors
WHERE (sid, sname) NOT IN (
    SELECT sid, sname
    FROM sailors
    INNER JOIN (
        SELECT sid
        FROM reserves
        INNER JOIN (
            SELECT bid
            FROM boats
            WHERE color = 'red'
        ) AS temp1
        USING(bid)
    ) AS temp2
    USING(sid)
);

/* Q6 */
SELECT AVG(age)
FROM sailors
WHERE rating = 10;

/* Q7 */
SELECT sailors.rating, sid, sname, age
FROM sailors
INNER JOIN (
    SELECT rating, MIN(age) AS min_age
    FROM sailors
    GROUP BY rating
) AS temp1
ON sailors.rating = temp1.rating
AND age = min_age
ORDER BY rating, sid;

/* Q8 */
SELECT bid, sid, sname
FROM sailors
INNER JOIN (
    SELECT bid, sid, COUNT(*) AS counts, max_counts
    FROM reserves
    INNER JOIN (
        SELECT bid, MAX(counts) AS max_counts
        FROM (
        	SELECT bid, sid, COUNT(*) AS counts
        	FROM reserves
        	GROUP BY bid, sid
        ) AS temp1
        GROUP BY bid
    ) AS temp2
    USING(bid)
    GROUP BY bid, sid
) AS temp3
USING(sid)
WHERE counts = max_counts
ORDER BY bid, sid;
