-- Data Cleaning Project


SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize Data
-- 3. Null Values or Blank Values
-- 4. Remove collumns as needed


-- Removing Duplicates

create table layoffs_staging
like layoffs;


SELECT *
FROM layoffs_staging;

Insert layoffs_staging
select * 
from layoffs;


SELECT *,
row_number() over(
partition by company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) as row_num
FROM layoffs_staging;


with duplicate_cte as
(
SELECT *,
row_number() over(
partition by company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;


select *
from layoffs_staging
where company = 'Casper';

## If using Microsoft SQL Sever use this as a simpler way to delete duplacate collumns ##
with duplicate_cte as
(
SELECT *,
row_number() over(
partition by company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) as row_num
from layoffs_staging
)
DELETE
from duplicate_cte
where row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select *
from layoffs_staging2;


INSERT INTO layoffs_staging2
SELECT *,
row_number() over(
partition by company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging2
Where row_num > 1;

DELETE
from layoffs_staging2
Where row_num > 1;

select *
from layoffs_staging2;

-- Standardizing Data

SELECT company, (TRIM(company))
FROM layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';


SELECT distinct country, trim(trailing '.' from country)
FROM layoffs_staging2
order by 1;

UPDATE layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select *
from layoffs_staging2;

select `date`,
str_to_date(`date`, '%m/%d/%Y%')
from layoffs_staging2;

UPDATE layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y%');


select `date`
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` DATE;

select *
from layoffs_staging2;

-- NULL values or blank values

select * 
from layoffs_staging2
where total_laid_off IS NULL
and percentage_laid_off is NULL; 

update layoffs_staging2
set industry = null 
where industry = '';

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company like 'Bally%';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where t1.industry is null
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

select *
from layoffs_staging2;

select * 
from layoffs_staging2
where total_laid_off IS NULL
and percentage_laid_off is NULL;

-- Removed any rows or collumns not needed 

delete
from layoffs_staging2
where total_laid_off IS NULL
and percentage_laid_off is NULL;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;

-- Final Cleaned Dataset

select *
from layoffs_staging2;


























