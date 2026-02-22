/*
=======================================================================================
Project: 2025 Amazon Sales Data Cleaning, Exploration and Visualization
Author: Yiming Yang
Raw data source: https://www.kaggle.com/datasets/zahidmughal2343/amazon-sales-2025

Description: This project implemented data cleaning and explored the potential
relationships between variables in a 2025 Amazon e-sales dataset using SQL. 
All data seemed to be drawn from 02/02/25 to 02/04/25. Manifesatation of these 
relationships are visualized using Tableau.

=======================================================================================
*/
USE AmazonSales;

SELECT * FROM amazon_sales;

--------------------------------------------Data Cleaning-----------------------------------------------------

--== 1. Create staging table

SELECT * INTO amazon_sales_staging
FROM amazon_sales;

--==2. Change column names for convenience

SELECT * FROM staging;

EXEC sp_rename 'staging.Order ID', 'Order_ID', 'column';

EXEC sp_rename 'staging.Total Sales', 'Total_sales', 'Column';

EXEC sp_rename 'staging.Customer Name', 'Customer_name', 'Column';

EXEC sp_rename 'staging.Customer Location', 'Customer_location', 'Column';

EXEC sp_rename 'staging.Payment Method', 'Payment_method', 'Column';


--== 3. Change column data types and value formats

---- i) Changing ORDxxxx into numbers only

--SELECT Order_ID, SUBSTRING(Order_ID, CHARINDEX('D', Order_ID)+1, LEN(Order_ID))
--FROM staging          -- to extract the sole number of the order_id rather than with 'ORD' included

ALTER TABLE staging
ADD ID VARCHAR(225);

UPDATE staging
SET ID = SUBSTRING(Order_ID, CHARINDEX('D', Order_ID)+1, LEN(Order_ID)); -- ID column created

---- ii) Changing data type of Date from Datetime to Date

--SELECT Date, CONVERT(DATE, Date)
--FROM staging;

ALTER TABLE staging
ADD Day DATE;

UPDATE staging
SET Day = CONVERT(Date, Date);


---- iii) Separating names into first and last names

--SELECT TRIM(Customer_name) FROM staging; 

UPDATE staging
SET Customer_name = TRIM(Customer_name); -- ensuring no space is interfering as I replace ' ' with '.'

SELECT REPLACE(Customer_name, ' ', '.')
FROM staging; -- checking if REPLACE is done correctly

SELECT Customer_name, 
PARSENAME(REPLACE(Customer_name, ' ', '.'), 2) AS First_name,
PARSENAME(REPLACE(Customer_name, ' ', '.'), 1) AS Last_name
FROM staging; 

ALTER TABLE staging
ADD First_name NVARCHAR(255), Last_name NVARCHAR(255);

UPDATE staging
SET First_name = PARSENAME(REPLACE(Customer_name, ' ', '.'), 2),
Last_name = PARSENAME(REPLACE(Customer_name, ' ', '.'), 1);



--== 4. Inspecting duplicates

WITH CTE1 AS
(
	SELECT *, 
	ROW_NUMBER() OVER(
	PARTITION BY Day, Product, Category, Price, First_name, Last_name
	ORDER BY Date) AS row_num
	FROM staging
)
SELECT * FROM CTE1
WHERE row_num <> 1;   -- 5 rows affected; these rows are kept for they might have placed two orders at the same time


--== 5. Inspecting missing values or NULL values

--SELECT * FROM staging WHERE ID IS NULL OR Day IS NULL OR
--Product IS NULL OR Category IS NULL OR First_name IS NULL 
--OR Last_name IS NULL OR Customer_location IS NULL OR Payment_method IS NULL OR Status IS NULL; -- none missing


--== 6. Removing any unimportant columns

---- i) Order_ID, Date and Customer_name are already transformed 
----    so I will do a final check that they are transformed properly before deleting them

--SELECT * FROM staging WHERE Order_ID IS NULL OR ID IS NULL; -- final check that all order_IDs are transformed properly

--SELECT * FROM staging WHERE Date IS NULL OR Day IS NULL; -- checking all Dates are transformed properly

--SELECT * FROM staging WHERE Customer_name IS NULL OR First_name IS NULL OR Last_name IS NULL;
-- checking all customer names are separated properly

---- ii) Deleting these three columns

ALTER TABLE staging
DROP COLUMN Order_ID, Date, Customer_name;



-----------------------------------------Exploration for Visualization--------------------------------------------------

--== 1. Which categories of products and which products were ordered the most?
SELECT Category, SUM(Quantity) AS QuantityPerCategory, SUM(Total_sales) AS Total_Sales_Per_Category,
COUNT(ID) AS Num_Orders
FROM staging
WHERE status = 'Completed' OR status = 'Pending'
GROUP BY Category
ORDER BY Total_Sales_Per_Category; -- table exported as Orders_Per_Category

SELECT Product, Category, SUM(Quantity) AS QuantityPerProduct, SUM(Total_sales) AS Total_Sales_Per_Product,
COUNT(ID) AS Num_Orders
FROM staging
WHERE status = 'Completed' OR status = 'Pending'
GROUP BY Product, Category
ORDER BY Total_Sales_Per_Product; -- table exported as Total_Sales_Per_Product

--== 2. Which city/location has most sales?
SELECT Customer_location, SUM(Quantity) AS QuantityPerLocation, SUM(Total_sales) AS Total_Sales_Per_Location
FROM staging
WHERE status = 'Completed' OR status = 'Pending'
GROUP BY Customer_location
ORDER BY Total_Sales_Per_Location DESC; -- table exported as Total_Sales_Per_Location

--== 3. How popular is AmazonPay?
SELECT Payment_method, SUM(Quantity) AS QuantityPerMethod, SUM(Total_sales) AS Total_Sales_Per_Method,
SUM(Total_sales)/(SELECT SUM(Total_sales) FROM staging WHERE status <> 'Cancelled') AS Percentage
FROM staging
WHERE status = 'Completed' OR status = 'Pending'
GROUP BY Payment_method
ORDER BY Total_Sales_Per_Method DESC; -- table exported as Total_Sales_Per_Method

--== 4. What are the cancellation rates of all categories?
SELECT Category, Status, COUNT(Status) AS Status_Count, SUM(Total_sales) AS TotalSales, 
SUM(Quantity) AS TotalQuantity,
CAST(COUNT(Status) AS DECIMAL(10,4))/(SUM(CAST(COUNT(Status) AS DECIMAL(10,4))) 
	OVER(PARTITION BY Category)) AS Status_Rate
FROM staging
GROUP BY category, status
ORDER BY
CASE 
	WHEN status = 'Cancelled' THEN 0
	WHEN status = 'Pending' THEN 1
	WHEN status = 'Completed' THEN 2
	END,
Category; -- table exported as Category_Status_Count


--== 5. What are the products that were frequently cancelled?
WITH ProductCount AS
(
	SELECT Product, Status, COUNT(Status) AS Status_Count, 
	SUM(Total_sales) AS TotalSales, SUM(Quantity) AS TotalQuantity,
	CAST(COUNT(Status) AS DECIMAL(10,4))/
		SUM(CAST(COUNT(Status) AS DECIMAL(10,4))) OVER(PARTITION BY Product) AS Status_Rate
	FROM staging
	GROUP BY Product, Status
)	
SELECT Product, Status, Status_Count, TotalSales, TotalQuantity, Status_Rate
FROM ProductCount
WHERE Status = 'Cancelled'; -- table exported as Product_Cancellation

--== 6. In which price ranges are customers ordering the most?
SELECT MIN(Price), MAX(Price)
FROM Staging; -- min is 15 and max is 1200 so I decided to have 4 price ranges

WITH PriceRanging AS
(
SELECT ID, Product, Category, Price, 
	(	CASE
			WHEN Price > 0 and Price <= 300 THEN '$0-300'
			WHEN Price > 300 and Price <= 600 THEN '$300-600'
			WHEN Price > 600 and Price <= 900 THEN '$600-900'
			WHEN Price > 900 and Price <= 1200 THEN '$900-1200'
		END
	) AS PriceRange
FROM staging
WHERE status = 'Completed' OR status = 'Pending'
)
SELECT PriceRange, COUNT(ID) AS Number_Of_Orders,
CAST(COUNT(ID) AS DECIMAL(10,4))/
	(SELECT CAST(COUNT(ID) AS DECIMAL(10,4)) FROM staging WHERE Status <> 'Cancelled') AS Percentage
FROM PriceRanging
GROUP BY PriceRange; -- table exported as Price_Range

--== 7. What are sales looking like in each month?
--SELECT FORMAT(Day, 'yyyy-MM') AS Month, COUNT(ID) AS Number_Of_Orders, SUM(Total_sales) AS Sales
--FROM staging
--WHERE status = 'Completed' OR status = 'Pending'
--GROUP BY FORMAT(Day, 'yyyy-MM'); 
-- 3 months were returned but April only had 2 days so I did following:

WITH Month_sales AS
(
	SELECT ID, Total_sales, Day, 
	(CASE 
		WHEN Day >= '2025-02-02' AND Day < '2025-03-02' THEN 'Feb-Mar'
		WHEN Day >= '2025-03-02' AND Day <= '2025-04-02' THEN 'Mar-Apr'
		END) AS Month
	FROM staging
	WHERE status = 'Completed' OR status = 'Pending'
)
SELECT Month, COUNT(ID) AS Number_Of_Orders, SUM(Total_sales) AS Sales
FROM Month_sales
GROUP BY Month; -- table exported as Month_Sales

--== 8. On which specific days are people ordering the most? Public holidays?
SELECT Day, COUNT(ID) AS Number_Of_Orders, SUM(Quantity) AS Total_Quantity, SUM(Total_sales) AS Sales
FROM staging
GROUP BY Day
ORDER BY Number_Of_Orders DESC; --table exported as Day_Sales

--== 9. How does gender affect buying behaviours? How do men and women differ in different categories?

--SELECT DISTINCT First_name FROM staging; -- to specify who are male/female

WITH Gender AS
(
	SELECT ID, First_name, Last_name,
	(CASE
		WHEN First_name IN ('Chris', 'Daniel', 'David', 'John', 'Michael') THEN 'Male'
		WHEN First_name IN ('Emily', 'Emma', 'Jane', 'Olivia', 'Sophia') THEN 'Female'
		END
	) AS Gender, Quantity, Total_Sales, Category
	FROM staging
	WHERE status = 'Completed' OR status = 'Pending'
)
SELECT Gender, Category, COUNT(ID) AS Number_Of_Orders, 
SUM(Quantity) AS Total_Quantity, SUM(Total_Sales) AS Sales,
ROUND(SUM(Total_Sales)/COUNT(ID), 2) AS Average_Expense
FROM Gender
GROUP BY Gender, Category
ORDER BY Gender; -- table exported as Gender_Sales_Per_Category


WITH Gender1 AS
(
	SELECT ID, First_name, Last_name,
	(CASE
		WHEN First_name IN ('Chris', 'Daniel', 'David', 'John', 'Michael') THEN 'Male'
		WHEN First_name IN ('Emily', 'Emma', 'Jane', 'Olivia', 'Sophia') THEN 'Female'
		END
	) AS Gender, Quantity, Total_Sales, Category
	FROM staging
	WHERE status = 'Completed' OR status = 'Pending'
)
SELECT Gender, COUNT(ID) AS Number_Of_Orders, 
SUM(Quantity) AS Total_Quantity, SUM(Total_Sales) AS Sales,
ROUND(SUM(Total_Sales)/COUNT(ID), 2) AS Average_Expense
FROM Gender1
GROUP BY Gender; -- table exported as Gender_Sales




--------------------------- Below are some questions I test-ran to see if they made sense ------------------------------
-- not included in the actual visualization

--== 10. What are the factors that are positively correlated with actual buying?
--SELECT First_name, Last_name, Status, AVG(Total_sales) AS Average_Sales_Per_Status
--FROM staging
--WHERE Status <> 'Cancelled'
--GROUP BY First_name, Last_name, Status
--ORDER BY First_name, Last_name,
--(CASE
--	WHEN Status = 'Pending' THEN 0
--	WHEN Status = 'Completed' THEN 1
--	END
--);

--SELECT Status,
--ROUND(SUM(Total_sales)/COUNT(Status), 3) AS Average_sales_Per_Status
--FROM staging
--WHERE status <> 'Cancelled'
--GROUP BY Status
--ORDER BY Status; 
-- average for completed is 1006.02 and for pending is 1062.18
-- don't see much discrepancy here so I don't think this establishes a meaningful question



