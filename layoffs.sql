
select *
from layoff;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove Any Columns

create table layoffs_staging
like layoff;

insert layoffs_staging
select *
from layoff;

INSERT INTO layoffs_staging (company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised)
VALUES
    -- Duplicate data
    ('Alerzo', 'Mumbai', 'Automotive', 320, 0.14, '6/15/2023', 'Series A', 'France', 7725),
    ('Alerzo', 'Mumbai', 'Automotive', 320, 0.14, '6/15/2023', 'Series A', 'France', 7725),

    -- Null values
    (NULL, 'New York', 'Tech', 500, NULL, '7/20/2023', 'Startup', 'USA', 15000),
    ('Google', NULL, 'Tech', NULL, 0.10, NULL, 'Pre-IPO', NULL, 10000),
    ('Facebook', 'London', NULL, 700, 0.20, '5/18/2023', NULL, 'UK',5645);

insert into layoffs_staging(company, location, industry, total_laid_off, percentage_laid_off, date, stage,country, funds_raised)
values ('Tesla', 'London', 'Automotive', 511, 0.07, '7/23/2023', 'Series B', 'France', 6878);

select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off,`date`
) as row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,country, funds_raised
) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company = 'tesla';

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,country, funds_raised
) as row_num
from layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,country, funds_raised
) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num > 1;

delete
from layoffs_staging2
where row_num > 1;


-- standardizing data (finding issues in data and fixing it)

select *
from layoffs_staging2;

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

select distinct location
from layoffs_staging2
order by 1
;
 
update layoffs_staging2
set location = 'Bombay'
where location like 'Mumbai';

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_staging2
modify column `date` DATE;


-- 3. Null Values or blank values


select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
     on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;    

update layoffs_staging2
set industry = null
where industry = '';

update layoffs_staging2 t1
join layoffs_staging2 t2
	 on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop row_num;
















