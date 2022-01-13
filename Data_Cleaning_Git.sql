--Hello, this is a project that I did, inspired by Alex Freberg from Youtube: AlexTheAnalyst in this video: https://www.youtube.com/watch?v=8rO7ztF4NtU&t=2142s
--His project uses MySQL while mine uses PostgreSQL, so you will notice that there are a few differences here and there.
--I hope you can learn from my repo!


--0.Insert table nashville_housing from csvfile

CREATE TABLE nashville_housing(
	UniqueID int,
	ParcelID varchar,
	LandUse varchar,
	PropertyAddress varchar,
	SaleDate date,
	SalePrice int,
	LegalReference varchar,
	SoldAsVacant varchar,
	OwnerName varchar,
	OwnerAddress varchar,
	Acreage numeric,
	TaxDistrict varchar,
	LandValue int,
	BuildingValue int,
	TotalValue int,
	YearBuilt int,
	Bedrooms int,
	FullBath int,
	HalfBath int)

COPY 		nashville_housing
FROM 		'https://github.com/dms-aditama/Datasets/blob/main/nashville_housing_for_data_cleaning.csv?raw=true'
DELIMITER 	','
CSV HEADER;

SELECT * FROM nashville_housing
WHERE propertyaddress is NULL

--1. Populating the nulls in PropertyAddress by self-joining the table
SELECT 		nhA.parcelID, nhA.propertyaddress, nhB.parcelID, nhB.propertyaddress,
COALESCE	(nhA.propertyaddress, nhB.propertyaddress) AS address --COALESCE = IFNULL in MySQL
FROM 		nashville_housing AS nhA
JOIN 		nashville_housing AS nhB
	ON 			nhA.parcelid = nhB.parcelid
	AND			nhA.uniqueid <> nhb.uniqueid
WHERE 		nhA.propertyaddress IS null

--Updating the address to the actual table
UPDATE 	nashville_housing AS nh --this table alias MUST BE DIFFERENT to the alias in FROM (unlike in MySQL)
SET		propertyaddress = COALESCE(nhA.propertyaddress, nhB.propertyaddress)
FROM	nashville_housing AS nhA
JOIN	nashville_housing AS nhB
	ON		nhA.parcelid = nhB.parcelid
	AND		nhA.uniqueid <> nhb.uniqueid
WHERE	nhA.propertyaddress IS null
AND		nh.parcelid = nhA.parcelid --this is a must since the parcelID is the parameter that determines the propertyaddress

SELECT * from nashville_housing
WHERE propertyaddress is NULL -- no longer exists

--2. Breaking out PropertyAddress into individual columns (Address, City, State)
SELECT 	PropertyAddress 
FROM 	nashville_housing

SELECT 
SUBSTRING 	(PropertyAddress, 1, POSITION(',' IN PropertyAddress)-1) AS PropSplitAddress,
			--Split the PropertyAddress, taking all before the ',' and put into a new column called PropSplitAddress
SUBSTRING 	(PropertyAddress, POSITION(',' IN PropertyAddress)+2, LENGTH(PropertyAddress)) AS PropSplitCity
			--Split the PropertyAddress, taking all after the ',' and put into a new column called PropSplitCity
FROM 		nashville_housing

--Creating new columns PropSplitAddress
ALTER TABLE 	nashville_housing
ADD 			PropSplitAddress varchar(300);
UPDATE 			nashville_housing
SET 			PropSplitAddress = SUBSTRING (PropertyAddress, 1, POSITION(',' IN PropertyAddress)-1)

--Creating new column for PropSplitCity
ALTER TABLE 	nashville_housing
ADD 			PropSplitCity varchar(300);
UPDATE 			nashville_housing
SET 			PropSplitCity = SUBSTRING (PropertyAddress, POSITION(',' IN PropertyAddress)+2, LENGTH(PropertyAddress))

SELECT * FROM nashville_housing

--3. Breaking out OwnerAddress into individual columns (Address, City, State)
SELECT 
	SPLIT_PART(OwnerAddress, ', ',1) AS OwnerSplitAddress,
	SPLIT_PART(OwnerAddress, ', ',2) AS OwnerSplitCity,
	SPLIT_PART(OwnerAddress, ', ',3) AS OwnerSplitState
FROM nashville_housing

--Creating new columns
ALTER TABLE nashville_housing
ADD 		OwnerSplitAddress varchar(300);
UPDATE 		nashville_housing
SET 		OwnerSplitAddress = SPLIT_PART(OwnerAddress, ', ',1)

ALTER TABLE nashville_housing
ADD 		OwnerSplitCity varchar(300);
UPDATE 		nashville_housing
SET 		OwnerSplitCity = SPLIT_PART(OwnerAddress, ', ',2)

ALTER TABLE nashville_housing
ADD 		OwnerSplitState varchar(300);
UPDATE 		nashville_housing
SET 		OwnerSplitState = SPLIT_PART(OwnerAddress, ', ',3)

SELECT * FROM nashville_housing

--4. Changing Y and N to 'Yes' and 'No' in soldasvacant field
SELECT DISTINCT soldasvacant, 
				COUNT(soldasvacant)
FROM 			nashville_housing
GROUP BY 		soldasvacant
ORDER BY 		2

SELECT 	SoldAsVacant,
CASE 	WHEN SoldAsVacant = 'Y' THEN 'Yes'
		WHEN SoldAsVacant = 'N' THEN 'No'
		ELSE SoldAsVacant
		END
FROM nashville_housing

UPDATE 	nashville_housing
SET 	SoldAsVacant = CASE WHEN SoldAsVacant = 'Y' THEN 'Yes'
							WHEN SoldAsVacant = 'N' THEN 'No'
							ELSE SoldAsVacant
							END

--5. Removing Duplicates
SELECT * FROM nashville_housing

--Using CTE to create a temporary table where we see the duplicates
WITH RowNumCTE AS(
	SELECT *, ctid,
	ROW_NUMBER() OVER(PARTITION BY --not as straightforward as in MySQL, we need ctid to delete CTE here
					  ParcelID,
					  PropertyAddress,
					  SalePrice,
					  SaleDate,
					  LegalReference
					  ORDER BY UniqueID) row_num --this column has the number of data. 1=unique, >1=duplicate
	FROM nashville_housing
)
SELECT *
FROM RowNumCTE
WHERE row_num > 1 --finding the duplicate
ORDER BY PropertyAddress

--Now, removing the duplicates
WITH RowNumCTE AS(
	SELECT *, ctid, 
	ROW_NUMBER() OVER(PARTITION BY
					  ParcelID,
					  PropertyAddress,
					  SalePrice,
					  SaleDate,
					  LegalReference
					  ORDER BY UniqueID) row_num
	FROM nashville_housing
)
DELETE FROM	nashville_housing
USING		RowNumCTE
WHERE		RowNumCTE.row_num > 1
AND			RowNumCTE.ctid = nashville_housing.ctid

SELECT * FROM nashville_housing

--6. Deleting Unused Columns
ALTER TABLE nashville_housing
DROP COLUMN PropertyAddress, 
DROP COLUMN OwnerAddress,
DROP COLUMN TaxDistrict

SELECT * FROM nashville_housing
