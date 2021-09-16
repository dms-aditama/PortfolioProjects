SELECT * FROM coviddeaths

COPY coviddeaths
FROM 'D:\BACKUP DATA D\looking for a job\2021 Job Upgrade\Post-Bootcamp\Data Portfolio Project\CovidDeaths.csv'
WITH CSV HEADER;

CREATE TABLE PUBLIC.covidvaccinations(
	iso_code varchar(50),
	continent varchar(50),
	location varchar(50),
	date date,
	new_tests numeric,
	total_tests numeric,
	total_tests_per_thousand numeric,
	new_tests_per_thousand numeric,
	new_tests_smoothed numeric,
	new_tests_smoothed_per_thousand numeric,
	positive_rate numeric,
	tests_per_case numeric,
	tests_units varchar(50),
	total_vaccinations numeric,
	people_vaccinated numeric,
	people_fully_vaccinated numeric,
	total_boosters numeric,
	new_vaccinations numeric,
	new_vaccinations_smoothed numeric,
	total_vaccinations_per_hundred numeric,
	people_vaccinated_per_hundred numeric,
	people_fully_vaccinated_per_hundred numeric,
	total_boosters_per_hundred numeric,
	new_vaccinations_smoothed_per_million numeric,
	stringency_index numeric,
	population_density numeric,
	median_age numeric,
	aged_65_older numeric,
	aged_70_older numeric,
	gdp_per_capita numeric,
	extreme_poverty numeric,
	cardiovasc_death_rate numeric,
	diabetes_prevalence numeric,
	female_smokers numeric,
	male_smokers numeric,
	handwashing_facilities numeric,
	hospital_beds_per_thousand numeric,
	life_expectancy numeric,
	human_development_index numeric,
	excess_mortality numeric
)

SELECT * FROM covidvaccinations

COPY covidvaccinations
FROM 'D:\BACKUP DATA D\looking for a job\2021 Job Upgrade\Post-Bootcamp\Data Portfolio Project\CovidVaccinations.csv'
WITH CSV HEADER;

SELECT Location, date, total_cases, new_cases, total_deaths, population
FROM coviddeaths
ORDER by 1,2

--Looking at Total Cases vs Total Deaths
--Shows likelihood of dying if you contract covid in your country
SELECT Location, date, total_cases, new_cases, total_deaths, (total_deaths/total_cases)*100 as DeathPercentage
FROM coviddeaths
WHERE location LIKE '%Indonesia%'
ORDER by 1,2


--Looking at Total Cases vs Population
--Shows what percentage of population got covid
SELECT Location, date, population, total_cases, (total_cases/population)*100 as DeathPercentage
FROM coviddeaths
WHERE location LIKE '%Indonesia%'
ORDER by 1,2

--What country has the highest infection rate?
SELECT Location, Population, MAX(total_cases) AS HighestInfectionCount, 
MAX(total_cases/population*100) AS PercentPopulationInfected
FROM coviddeaths
WHERE total_cases/population*100 IS NOT NULL-- to exclude the nulls; cannot use alias as the not null
GROUP BY Location, population
ORDER BY PercentPopulationInfected DESC

--Showing countries with the highest death count?
SELECT Location, MAX(total_deaths) as TotalDeathCount
FROM coviddeaths
WHERE continent IS NOT NULL and total_deaths IS NOT NULL
GROUP BY location
ORDER BY TotalDeathCount DESC

--Showing countries with the highest death count?
SELECT location, MAX(total_deaths) as TotalDeathCount
FROM coviddeaths
WHERE continent IS NULL and total_deaths IS NOT NULL
GROUP BY location
ORDER BY TotalDeathCount DESC

-- BY CONTINENT
SELECT continent, MAX(total_deaths) as TotalDeathCount
FROM coviddeaths
WHERE continent IS NOT NULL and total_deaths IS NOT NULL
GROUP BY continent
ORDER BY TotalDeathCount DESC

--GLOBAL NUMBERS
SELECT SUM(new_cases) as Total_cases, SUM(new_deaths) as total_deaths, 
SUM(new_deaths)/SUM(new_cases)*100 as DeathPercentage
FROM coviddeaths
WHERE continent IS NOT null
order by 1,2

--Looking at total population vs. vaccinations
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as RollingPeopleVaccinated
FROM coviddeaths as dea
JOIN covidvaccinations as vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.location = 'Indonesia' --dea.continent IS NOT NULL	
ORDER BY 2,3

-- USE CTE
With PopvsVac (Continent, location, date, population, new_vaccinations, RollingPeopleVaccinated)
as
(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as RollingPeopleVaccinated
FROM coviddeaths as dea
JOIN covidvaccinations as vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.location = 'Indonesia'
ORDER BY 2,3
)
SELECT *, (RollingPeopleVaccinated/Population)*100 as RollingVacPercentage
FROM PopvsVac

--TEMP TABLE
CREATE TEMP TABLE PercentPopulationVaccinated AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as RollingPeopleVaccinated
FROM coviddeaths as dea
JOIN covidvaccinations as vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.location = 'Indonesia'	
ORDER BY 2,3

SELECT *, (RollingPeopleVaccinated/Population)*100 as RollingVacPercentage
FROM PercentPopulationVaccinated

--Creating view to store data for later visualizations
CREATE VIEW PercentPopulationVaccinated AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as RollingPeopleVaccinated
FROM coviddeaths as dea
JOIN covidvaccinations as vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL	
ORDER BY 2,3