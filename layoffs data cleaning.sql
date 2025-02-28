-- This is data on companies that have laid off employees. It is going to be cleaned. The process involves:

-- 1. Removing duplicates
-- 2. Standardising the format
-- 3. Dealing with null and blank values
-- 4. Removing any columns or rows 

## the first step is to create a backup table containing all the raw data in case any useful data is lost

create table layoffs_raw
like layoffs;

insert into layoffs_raw
select *
from layoffs;

select *
from layoffs_raw;

alter table layoffs
rename to layoffs_clean;

-- 1. Removing duplciates

## looking at the data to get an understanding of it
select *
from layoffs_clean;


## creating a cte with row numbers that are partitioned over every column to see if 

with numbered as 
(
select *, 
row_number() over(partition by 
					company, location, industry, 
                    total_laid_off, percentage_laid_off, 
                    `date`, stage, country,
                    funds_raised_millions) as row_num
from layoffs_clean
)

select *
from numbered 
where row_num > 1;

## this reveals 5 duplicate rows so another table containing the row numbers needs to be created so they can be deleted

CREATE TABLE `layoffs_clean2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


insert into layoffs_clean2
select *, 
row_number() over(partition by 
					company, location, industry, 
                    total_laid_off, percentage_laid_off, 
                    `date`, stage, country,
                    funds_raised_millions) as row_num
from layoffs_clean;

delete from layoffs_clean2
where row_num > 1;

select *
from layoffs_clean2
where row_num > 1;

## this returns no rows so the duplicates are deleted

drop table layoffs_clean;

alter table layoffs_clean2 rename to layoffs_clean;



-- 2. Standardising the format

## looking at the company column

select 
distinct company
from layoffs_clean
order by company;

## some of the company data have spaces so need to be trimmed

update layoffs_clean
set company = trim(company);

## looking at the locations column

select 
distinct location
from layoffs_clean
order by location;

## no issues with the locations

## looking at the industry column

select 
distinct industry
from layoffs_clean
order by industry;

## there are instances of 'Crypto', 'Crypto Currency' and 'CryptoCurrency' which should all be the same

update layoffs_clean
set industry = 'Crypto'
where industry like 'Crypto%';

## looking at the percentage laid off column

select
percentage_laid_off
from layoffs_clean
order by percentage_laid_off desc;

## no numbers outside the range 0-1 so no issues

## the date column is in non-standard format and is also in datatype text which needs to be changed to date

update layoffs_clean
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_clean
modify column `date` date;

##looking at the country column

select 
distinct country
from layoffs_clean
order by country;

## there is an instance of a '.' following United States

update layoffs_clean
set country = trim(trailing '.' from country);

## format is standardised



-- 3. Null and blank values


## finding null and blank values

select *
from layoffs_clean
where company is null
or company = '';

## none

select *
from layoffs_clean
where location is null
or location = '';

## none

select *
from layoffs_clean
where industry is null
or industry = '';

## null and blanks in industry

select *
from layoffs_clean
where total_laid_off is null
or total_laid_off = '';

## nulls in total laid off

select *
from layoffs_clean
where percentage_laid_off is null
or percentage_laid_off = '';

## nulls in percentage laid off

select *
from layoffs_clean
where `date` is null;

## null in date

select *
from layoffs_clean
where stage is null
or stage = '';

## nulls in stage

select *
from layoffs_clean
where country is null
or country = '';

## none

select *
from layoffs_clean
where funds_raised_millions is null
or funds_raised_millions = '';

## nulls in funds raised millions

## nulls and/or blanks in industry, total laid off, percentage laid off, date, stage and funds raised millions
## for total laid off, percentage laid off, date, stage and funds raised millions there is insufficient data to fill in the columns

## looking at industry

select *
from layoffs_clean
where industry is null
or industry = '';

## blanks/nulls can be filled in for companies that appear in other rows with non-blank/null industry

update layoffs_clean
set industry = null
where industry = '';

update layoffs_clean t1
join layoffs_clean t2
	on t1.company = t2.company
	and t1.location = t2.location
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

## blanks and nulls removed


-- 4. Deleting any rows or columns

## for rows with nulls in both total laid off and percentage laid off, the data is useless and so they are deleted

delete from layoffs_clean
where total_laid_off is null
and percentage_laid_off is null;

## the row num column is no longer needed

alter table layoffs_clean
drop column row_num;

select *
from layoffs_clean;

## data is now clean and table is ready to be exported




