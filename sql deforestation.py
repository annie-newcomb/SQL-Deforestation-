#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().run_line_magic('run', 'setup.py')


# In[ ]:


def view(q):
    return sqldf(q)

query = """
CREATE VIEW forestation AS (
SELECT forest_area.country_name AS name, 
forest_area.year AS year_date, 
forest_area.forest_area_sqkm, 
land_area.total_area_sq_mi*2.59 AS land_sqkm, forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 AS percent_forest_area, regions.region, 
CASE WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 >= 75 THEN '75-100' 
WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 BETWEEN 50 AND 75 THEN '50-75' 
WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 BETWEEN 25 AND 50 THEN '25-50' ELSE '0-25' 
END AS quartile
FROM forest_area
JOIN land_area
ON land_area.country_code = forest_area.country_code
AND land_area.country_name = forest_area.country_name
AND land_area.year = forest_area.year
JOIN regions
ON regions.country_code = land_area.country_code
WHERE forest_area.year ='2016' 
GROUP BY name, forest_area.year,
 forest_area.forest_area_sqkm, 
land_sqkm, 
regions.region
ORDER BY percent_forest_area)
"""
view(query)


# In[ ]:


def frst_area_chng(q):
    return sqldf(q)

query = """
SELECT forest_area_sqkm, 
	year,
    LEAD(forest_area_sqkm) OVER (ORDER BY year) AS lead,
    forest_area_sqkm - LEAD(forest_area_sqkm) OVER (ORDER BY year) AS difference,
    ((forest_area_sqkm - LEAD(forest_area_sqkm) OVER (ORDER BY year))/forest_area_sqkm) * 100 AS percent_change 
FROM forest_area
WHERE country_name = 'World'
	AND year IN ('1990', '2016') 
ORDER BY 2
"""
frst_area_chng(query)


# In[ ]:


def highest_land(q):
    return sqldf(q)

query = """
SELECT DISTINCT country_name, 
		total_area_sq_mi * 2.59 AS land_sq_km, 
        		year
FROM land_area
WHERE (total_area_sq_mi * 2.59) <= 1324449
		AND year = '2016'
ORDER BY 2 DESC
LIMIT 1
"""
highest_land(query)


# In[ ]:


def prct_frst_area(q):
    return sqldf(q)

query = """
SELECT forest_area.country_name AS name, 
forest_area.year AS year_date,
 	forest_area.forest_area_sqkm, 
land_area.total_area_sq_mi*2.59 AS land_sqkm, forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 AS percent_forest_area, 
regions.region
FROM forest_area
JOIN land_area
ON land_area.country_code = forest_area.country_code
AND land_area.country_name = forest_area.country_name
AND land_area.year = forest_area.year
JOIN regions
ON regions.country_code = land_area.country_code
WHERE forest_area.year IN ('1990', '2016') 
GROUP BY name, forest_area.year, 
forest_area.forest_area_sqkm, 
land_sqkm, 
regions.region
ORDER BY name
"""
prct_frst_area(query)


# In[ ]:


def region_prct(q):
    return sqldf(q)

query = """
SELECT forest_area.year AS year_date, SUM(forest_area.forest_area_sqkm)/SUM(land_area.total_area_sq_mi *2.59)*100 AS percent_forest_area, 
regions.region
FROM forest_area
JOIN land_area
ON land_area.country_code = forest_area.country_code
AND land_area.country_name = forest_area.country_name
AND land_area.year = forest_area.year
JOIN regions
ON regions.country_code = land_area.country_code
WHERE forest_area.year IN ('1990', '2016') 
GROUP BY regions.region, 
year_date
"""
region_prct(query)


# In[ ]:


def rnd_region(q):
    return sqldf(q)

query = """
SELECT forest_area.year AS year_date, ROUND((SUM(forest_area.forest_area_sqkm)/SUM(land_area.total_area_sq_mi *2.59)*100)::numeric,2) AS percent_forest_area, 
regions.region
FROM forest_area
JOIN land_area
ON land_area.country_code = forest_area.country_code
AND land_area.country_name = forest_area.country_name
AND land_area.year = forest_area.year
JOIN regions
ON regions.country_code = land_area.country_code
WHERE forest_area.year IN ('1990', '2016') 
GROUP BY regions.region, year_date
ORDER BY percent_forest_area, 
regions.region
"""
rnd_region(query)


# In[ ]:


def chng_frst_area(q):
    return sqldf(q)

query = """
SELECT forest_area_sqkm, year, 
country_name, 
LAG(forest_area_sqkm) OVER (PARTITION BY country_name ORDER BY year) AS lag, forest_area_sqkm - LAG(forest_area_sqkm) OVER (PARTITION BY country_name ORDER BY year) AS difference_forest, 
((forest_area_sqkm - LAG(forest_area_sqkm) OVER (PARTITION BY country_name ORDER BY year))/forest_area_sqkm) * 100 AS percent_change 
FROM forest_area
WHERE year IN ('1990', '2016')
ORDER BY percent_change DESC
"""
chng_frst_area(query)


# In[ ]:


def tot_diff(q):
    return sqldf(q)

query = """
SELECT forest_area.forest_area_sqkm, 
forest_area.year, 
forest_area.country_name AS name, 
regions.region, 
LAG(forest_area.forest_area_sqkm) OVER (PARTITION BY forest_area.country_name ORDER BY forest_area.year) AS lag, 
forest_area.forest_area_sqkm - LAG(forest_area.forest_area_sqkm) OVER (PARTITION BY forest_area.country_name ORDER BY forest_area.year) AS difference_forest, ((forest_area.forest_area_sqkm - LAG(forest_area.forest_area_sqkm) OVER (PARTITION BY forest_area.country_name ORDER BY forest_area.year))/forest_area.forest_area_sqkm) * 100 AS percent_change 
FROM forest_area
JOIN regions
ON forest_area.country_code = regions.country_code
WHERE year IN ('1990', '2016')
ORDER BY difference_forest
"""
tot_diff(query)


# In[ ]:


def prct_change(q):
    return sqldf(q)

query = """
SELECT forest_area.forest_area_sqkm, 
forest_area.year, 
forest_area.country_name AS name, 
regions.region, 
LAG(forest_area.forest_area_sqkm) OVER (PARTITION BY forest_area.country_name ORDER BY forest_area.year) AS lag, 
forest_area.forest_area_sqkm - LAG(forest_area.forest_area_sqkm) OVER (PARTITION BY forest_area.country_name ORDER BY forest_area.year) AS difference_forest, ((forest_area.forest_area_sqkm - LAG(forest_area.forest_area_sqkm) OVER (PARTITION BY forest_area.country_name ORDER BY forest_area.year))/forest_area.forest_area_sqkm) * 100 AS percent_change 
FROM forest_area
JOIN regions
ON forest_area.country_code = regions.country_code
WHERE year IN ('1990', '2016')
ORDER BY percent_change
"""
prct_change(query)


# In[ ]:


def quartiles(q):
    return sqldf(q)

query = """
SELECT forest_area.country_name AS name, 
forest_area.year AS year_date, 
forest_area.forest_area_sqkm, land_area.total_area_sq_mi*2.59 AS land_sqkm, forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 AS percent_forest_area, regions.region, 
CASE WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 >= 75 THEN '75-100' 
WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 BETWEEN 50 AND 75 THEN '50-75' 
WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 BETWEEN 25 AND 50 THEN '25-50' 
ELSE '0-25' 
END AS quartile
FROM forest_area
JOIN land_area
ON land_area.country_code = forest_area.country_code
AND land_area.country_name = forest_area.country_name
AND land_area.year = forest_area.year
JOIN regions
ON regions.country_code = land_area.country_code
WHERE forest_area.year ='2016' 
GROUP BY name, 
forest_area.year, 
forest_area.forest_area_sqkm, 
land_sqkm, 
regions.region
ORDER BY percent_forest_area
"""
quartiles(query)


# In[ ]:


def top_qrt(q):
    return sqldf(q)

query = """
SELECT forest_area.country_name AS name, 
forest_area.year AS year_date, 
forest_area.forest_area_sqkm, 
land_area.total_area_sq_mi*2.59 AS land_sqkm, forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 AS percent_forest_area, regions.region, 
CASE WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 >= 75 THEN '75-100' 
WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 BETWEEN 50 AND 75 THEN '50-75' 
WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 BETWEEN 25 AND 50 THEN '25-50' 
ELSE '0-25' 
END AS quartile
FROM forest_area
JOIN land_area
ON land_area.country_code = forest_area.country_code
AND land_area.country_name = forest_area.country_name
AND land_area.year = forest_area.year
JOIN regions
ON regions.country_code = land_area.country_code
WHERE forest_area.year ='2016' 
GROUP BY name, 
forest_area.year, 
forest_area.forest_area_sqkm, 
land_sqkm, 
regions.region
HAVING forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 > 75
ORDER BY percent_forest_area
"""
top_qrt(query)


# In[ ]:


def btm_qrt(q):
    return sqldf(q)

query = """
SELECT forest_area.country_name AS name, 
forest_area.year AS year_date, 
forest_area.forest_area_sqkm, 
land_area.total_area_sq_mi*2.59 AS land_sqkm, forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 AS percent_forest_area, regions.region, 
CASE WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 >= 75 THEN '75-100' 
WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 BETWEEN 50 AND 75 THEN '50-75' 
WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 BETWEEN 25 AND 50 THEN '25-50' 
ELSE '0-25' 
END AS quartile
FROM forest_area
JOIN land_area
ON land_area.country_code = forest_area.country_code
AND land_area.country_name = forest_area.country_name
AND land_area.year = forest_area.year
JOIN regions
ON regions.country_code = land_area.country_code
WHERE forest_area.year ='2016' 
GROUP BY name, 
forest_area.year, 
forest_area.forest_area_sqkm, 
land_sqkm, 
regions.region
HAVING forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 <25
ORDER BY percent_forest_area
"""
btm_qrt(query)


# In[ ]:


def qrt_country(q):
    return sqldf(q)

query = """
SELECT forest_area.country_name AS name, 
forest_area.year AS year_date, 
forest_area.forest_area_sqkm, 
land_area.total_area_sq_mi*2.59 AS land_sqkm, forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 AS percent_forest_area, regions.region, 
CASE WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 >= 75 THEN '75-100' 
WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 BETWEEN 50 AND 75 THEN '50-75' 
WHEN forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 BETWEEN 25 AND 50 THEN '25-50' 
ELSE '0-25' 
END AS quartile
FROM forest_area
JOIN land_area
ON land_area.country_code = forest_area.country_code
AND land_area.country_name = forest_area.country_name
AND land_area.year = forest_area.year
JOIN regions
ON regions.country_code = land_area.country_code
WHERE forest_area.year ='2016' 
GROUP BY name, 
forest_area.year, 
forest_area.forest_area_sqkm, 
land_sqkm, 
regions.region
HAVING forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 <50 AND forest_area.forest_area_sqkm/(land_area.total_area_sq_mi *2.59)*100 >25
ORDER BY percent_forest_area
"""
qrt_country(query)

