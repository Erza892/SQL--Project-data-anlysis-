-- SQL  Global tech layoffs Project - Data Cleaning

USE practice;

SELECT * FROM layoffs;



-- creating a staging table to  clean the data
CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT layoffs_staging 
SELECT * FROM layoffs;

-- 1. Remove Duplicates

SELECT * FROM layoffs_staging;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM layoffs_staging;



SELECT * FROM (
SELECT company, industry, total_laid_off,`date`,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off,`date`) AS row_num
FROM layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- look at oda to confirm
SELECT *
FROM layoffs_staging
WHERE company = 'Oda'
;
--  these are all legitimate entries and shouldn't be deleted. 




-- create a new column and add  row numbers in. Then delete where row numbers are over 2, then delete that column


ALTER TABLE layoffs_staging ADD row_num INT;


SELECT *
FROM layoffs_staging
;

CREATE TABLE `practice`.`layoffs_stagings2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `practice`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging;

-- now that we have this we can delete rows were row_num is greater than 2

DELETE FROM layoffs_stagings2
WHERE row_num >= 2;



-- 2. Standardize Data

SELECT * 
FROM layoffs_stagings2;

--  look at industry it looks like it have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM layoffs_stagings2
ORDER BY industry;

SELECT *
FROM layoffs_stagings2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM layoffs_stagings2
WHERE company LIKE 'Bally%';
-- nothing wrong here

SELECT *
FROM layoffs_stagings2
WHERE company LIKE 'airbnb%';


UPDATE layoffs_stagings2
SET industry = NULL
WHERE industry = '';



SELECT *
FROM layoffs_stagings2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


UPDATE layoffs_stagings2 t1
JOIN layoffs_stagings2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_stagings2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

-- Crypto has multiple different variations. We need to standardize that -  to Crypto
SELECT DISTINCT industry
FROM layoffs_stagings2
ORDER BY industry;

UPDATE layoffs_stagings2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');


SELECT DISTINCT industry
FROM layoffs_stagings2
ORDER BY industry;



SELECT *
FROM layoffs_stagings2;

SELECT DISTINCT country
FROM layoffs_stagings2
ORDER BY country;

UPDATE layoffs_stagings2
SET country = 'USA'
where country like 'US%';


SELECT DISTINCT country
FROM layoffs_stagings2
ORDER BY country;


--  date columns:
SELECT *
FROM layoffs_stagings2;

-- str to date to update this field
UPDATE layoffs_stagings2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');


ALTER TABLE layoffs_stagings2
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoffs_stagings2;





-- 3. Null Values


SELECT *
FROM layoffs_stagings2
WHERE total_laid_off IS NULL;


SELECT *
FROM layoffs_stagings2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- 4. remove any columns and rows we need to



-- Deleting Useless data 
DELETE FROM layoffs_stagings2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_stagings2
DROP COLUMN row_num;


SELECT * FROM layoffs_stagings2;


-- EDA ----

-- to explore the data and find trends or patterns or outliers

SELECT * 
FROM layoffs_stagings2;



SELECT MAX(total_laid_off)
FROM layoffs_stagings2;


SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_stagings2
WHERE  percentage_laid_off IS NOT NULL;

SELECT *
FROM layoffs_stagings2
WHERE  percentage_laid_off = 1;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;



-- USING GROUP BY--------------------------------------------------------------------------------------------------

-- Companies with the biggest single Layoff

SELECT company, total_laid_off
FROM layoffs_stagings
ORDER BY 2 DESC
LIMIT 5;


-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_stagings2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;



-- by location
SELECT location, SUM(total_laid_off)
FROM layoffs_stagings2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- this it total in the past 3 years or in the dataset

SELECT country, SUM(total_laid_off)
FROM layoffs_stagings2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_stagings2
GROUP BY YEAR(date)
ORDER BY 1 ASC;


SELECT industry, SUM(total_laid_off)
FROM layoffs_stagings2
GROUP BY industry
ORDER BY 2 DESC;


SELECT stage, SUM(total_laid_off)
FROM layoffs_stagings2
GROUP BY stage
ORDER BY 2 DESC;

-- By Year 

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_stagings2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;




-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_stagings2
GROUP BY dates
ORDER BY dates ASC;


WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_stagings2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;
