# This file contain commands to import the dataset from csv format files to MySQL, seller dataset not imported as it was irrelevant to KPI
USE practicedb;
show tables;
-- 1.olist_customer_dataset
CREATE TABLE olist_customer_dataset(
customer_id char(32),
customer_unique_id CHAR(32),
zip_code int,
customer_city varchar(50),
customer_state char(2));
-- Loading data file
LOAD DATA INFILE 'C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\olist_customers_dataset.csv'
INTO TABLE olist_geolocation 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 2.olist_geolocation_dataset
CREATE TABLE olist_geolocation(
geolocation_zip_code_prefix INT(10),
	geolocation_lat	FLOAT(20),
    geolocation_lng	FLOAT(20),
    geolocation_city VARCHAR(100),	
    geolocation_state CHAR(2));
-- Loading data file
LOAD DATA INFILE   'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_geolocation_dataset.csv'
INTO TABLE olist_geolocation 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from practicedb.olist_geolocation; 
-- 3. olist_order_items_dataset.csv
CREATE TABLE olist_order_items_dataset(
order_id	CHAR(32),
order_item_id	INT,
product_id	CHAR(32),
seller_id	CHAR(32),
shipping_limit_date	DATETIME,
price	FLOAT,
freight_value FLOAT);
LOAD DATA INFILE   'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_items_dataset.csv'
INTO TABLE olist_order_items_dataset 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
-- 4. olist_order_payments_dataset.csv
CREATE TABLE olist_order_payments_dataset(
order_id	CHAR(32),
payment_sequential	INT,
payment_type	VARCHAR(50),
payment_installments INT,	
payment_value FLOAT);
LOAD DATA INFILE   'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_payments_dataset.csv'
INTO TABLE olist_order_payments_dataset 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 5. olist_order_reviews_dataset.csv
CREATE TABLE olist_order_reviews_dataset(
review_id	VARCHAR(50),
order_id	VARCHAR(32),
review_score	INT,
review_comment_title	VARCHAR(50),
review_comment_message	TEXT,
review_creation_date	DATETIME,
review_answer_timestamp DATETIME);
LOAD DATA INFILE   'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_reviews_dataset.csv'
INTO TABLE olist_order_reviews_dataset 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 6. olist_orders_dataset.csv
CREATE TABLE olist_orders_dataset(
order_id	CHAR(32),
customer_id	CHAR(32),
order_status VARCHAR(20),
order_purchase_timestamp DATETIME,	
order_approved_at	DATETIME NULL,
order_delivered_carrier_date DATETIME NULL,	
order_delivered_customer_date	DATETIME NULL,
order_estimated_delivery_date DATETIME NULL);
LOAD DATA INFILE   'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_orders_dataset.csv'
INTO TABLE olist_orders_dataset 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
ESCAPED  BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id,customer_id,	order_status,	order_purchase_timestamp,	@order_approved_at,	@order_delivered_carrier_date,	@order_delivered_customer_date,	
order_estimated_delivery_date)
SET order_delivered_customer_date = IF(@order_delivered_customer_date = '', NULL, @order_delivered_customer_date),
  order_delivered_carrier_date = IF(@order_delivered_carrier_date = '', NULL, @order_delivered_carrier_date),
  order_approved_at = IF(@order_approved_at = '', NULL, @order_approved_at);
#Reason to use set clause is to allow the empty cells, here we replaced them with NULL
-- 7. olist_products_dataset.csv
CREATE TABLE olist_products_dataset(
product_id	VARCHAR(32),
product_category_name	VARCHAR(50),
product_name_lenght	INT,
product_description_lenght	INT,
product_photos_qty	INT,
product_weight_g	INT,
product_length_cm	INT,
product_height_cm	INT,
product_width_cm INT);
LOAD DATA INFILE   'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_products_dataset.csv'
INTO TABLE olist_products_dataset 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_id,@product_category_name,@product_name_lenght,	@product_description_lenght,	@product_photos_qty,	@product_weight_g,
@product_length_cm,	@product_height_cm,	@product_width_cm)	
SET product_name_lenght = IF(@product_name_lenght = '', NULL, @product_name_lenght),
 product_category_name = IF(@product_category_name = '', NULL, @product_category_name),
 product_description_lenght = IF(@product_description_lenght = '', NULL, @product_description_lenght),
 product_photos_qty = IF(@product_photos_qty = '', NULL, @product_photos_qty),
 product_weight_g = IF(@product_weight_g = '', NULL, @product_weight_g),
 product_length_cm = IF(@product_length_cm = '', NULL, @product_length_cm),
  product_height_cm = IF(@product_height_cm = '', NULL, @product_height_cm),
  product_width_cm = IF(@product_width_cm = '', NULL, @product_width_cm);

-- 8. product_category_name_translation
CREATE TABLE product_category_name_translation(
product_category_name text,
product_category_english_name text);
LOAD DATA INFILE   'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/product_category_name_translation.csv'
INTO TABLE product_category_name_translation 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- creating KPI's 
-- KPI 1 

SELECT 
    IF(WEEKDAY(olist_orders_dataset.order_purchase_timestamp) IN (5, 6), 'Weekend', 'Weekday') AS day_type, 
    SUM(olist_order_payments_dataset.payment_value) AS total_payment
FROM 
    olist_orders_dataset 
    INNER JOIN olist_order_payments_dataset ON olist_orders_dataset.order_id = olist_order_payments_dataset.order_id
GROUP BY 
    day_type;
    
    
-- KPI 2 

select count(r.order_id), r.review_score, p. payment_type
from olist_order_reviews_dataset r  join olist_order_payments_dataset p on r.order_id = p.order_id
where review_score = 5 and payment_type = "credit_card";

-- KPI 3

select
 p.product_category_name,
ROUND(AVG(DATEDIFF(od.order_delivered_customer_date,od.order_purchase_timestamp))) AS AVG_Delivery_Days
 FROM olist_products_dataset p
 INNER JOIN olist_order_items_dataset  oi  ON p.product_id = oi.product_id
 INNER JOIN olist_orders_dataset od ON oi.order_id = od.order_id
 WHERE p.product_category_name = "pet_shop"
 GROUP BY
  p.product_category_name;
  
-- KPI 4 
SELECT 
  c.customer_city,
ROUND(AVG(i.price)) AS Average_Price,
ROUND(AVG(p.payment_value)) AS Average_Payment
FROM olist_customer_dataset c 
INNER JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
INNER JOIN olist_order_items_dataset i ON o.order_id = i.order_id
INNER JOIN olist_order_payments_dataset p ON o.order_id = p.order_id
WHERE c.customer_city = "sao paulo"
GROUP BY c.customer_city;

-- KPI 5

SELECT review_score, AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)) AS avg_shipping_days
FROM olist_orders_dataset o
INNER JOIN olist_order_reviews_dataset r
  ON o.order_id = r.order_id
GROUP BY review_score;

Select * from olist_customer_dataset;
Select * from olist_geolocation_dataset;
select * from olist_order_items_dataset;
select * from olist_order_payments_dataset;
select * from olist_order_reviews_dataset;
select * from olist_orders_dataset;
select * from olist_products_dataset;
select * from product_category_name_translation;




