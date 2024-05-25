-- Section B --
-- Transformation Function --
CREATE OR REPLACE FUNCTION readable_date(rental_date timestamp)
	RETURNS varchar(100)
	LANGUAGE plpgsql
AS
$$
DECLARE clean_date varchar(100);
BEGIN
	SELECT TO_CHAR(rental_date, 'Mon DD, YYYY') INTO clean_date;
	RETURN clean_date;
END;
$$


-- Section C --
-- Create Detailed Table --
CREATE TABLE detailed_table (
	store_id int,
	inventory_id int,
	film_id int,
	title varchar(1000),
	customer_id int,
	rental_date varchar(100),
	rental_date_ts timestamp);

-- Create Summary Table --
CREATE TABLE summary_table (
	store_id int, 
	title varchar(1000), 
	rentals_count int);


-- Section D --
-- SQL Query to extract raw data from source database and insert into the detailed table --
INSERT INTO detailed_table
	SELECT 
		i.store_id, 
		i.inventory_id, 
		f.film_id, 
		f.title, 
		r.customer_id, 
		readable_date(r.rental_date) AS rental_date,
		r.rental_date AS rental_date_ts
	FROM rental r
	INNER JOIN inventory i ON r.inventory_id = i.inventory_id
	INNER JOIN film f ON i.film_id = f.film_id
	WHERE r.rental_date BETWEEN '2005-05-24' AND '2005-06-24'
	ORDER BY r.rental_date
;


-- Section E --	
--INSERT INTO trigger function for when data is inserted into the detailed_table --
CREATE OR REPLACE FUNCTION insert_trigger_function()
	RETURNS TRIGGER
	LANGUAGE plpgsql
AS 
$$
BEGIN
	DELETE FROM summary_table;

	WITH top_movies AS (
    	SELECT
        	dt.store_id,
        	dt.title,
			COUNT(*) AS rental_count,
        	ROW_NUMBER() OVER (PARTITION BY dt.store_id ORDER BY COUNT(*) DESC) AS rank
    	FROM detailed_table dt
    	GROUP BY
			dt.store_id,
			dt.title
	)
      
	INSERT INTO summary_table
		SELECT
    		tm.store_id,
    		tm.title,
    		tm.rental_count
		FROM top_movies tm
		WHERE tm.rank <= 1;
RETURN NEW;
END;
$$

CREATE TRIGGER summary_table_insert
	AFTER INSERT
	ON detailed_table
	FOR EACH STATEMENT
	EXECUTE PROCEDURE insert_trigger_function();
	
	
-- UPDATE trigger function for when existing data is updated in detailed_table --
CREATE OR REPLACE FUNCTION update_trigger_function()
	RETURNS TRIGGER
	LANGUAGE plpgsql
AS 
$$
BEGIN
	DELETE FROM summary_table;

	WITH top_movies AS (
    	SELECT
        	dt.store_id,
        	dt.title,
			COUNT(*) AS rental_count,
        	ROW_NUMBER() OVER (PARTITION BY dt.store_id ORDER BY COUNT(*) DESC) AS rank
    	FROM detailed_table dt
    	GROUP BY
			dt.store_id,
			dt.title
	)
      
	INSERT INTO summary_table
		SELECT
    		tm.store_id,
    		tm.title,
    		tm.rental_count
		FROM top_movies tm
		WHERE tm.rank <= 1;
RETURN NEW;
END;
$$

CREATE TRIGGER summary_table_update
	AFTER UPDATE
	ON detailed_table
	FOR EACH STATEMENT
	EXECUTE PROCEDURE update_trigger_function();


-- Section F --
-- Stored procedure that refreshes the data in both the detailed table and summary table --
CREATE OR REPLACE PROCEDURE refresh_data()
LANGUAGE plpgsql
AS 
$$
BEGIN
DELETE FROM detailed_table;
DELETE FROM summary_table;
INSERT INTO detailed_table
	SELECT 
		i.store_id, 
		i.inventory_id, 
		f.film_id, 
		f.title, 
		r.customer_id, 
		readable_date(r.rental_date) AS rental_date,
		r.rental_date AS rental_date_ts
	FROM rental r
	INNER JOIN inventory i ON r.inventory_id = i.inventory_id
	INNER JOIN film f ON i.film_id = f.film_id
	WHERE r.rental_date BETWEEN '2005-05-24' AND '2005-06-24'
	ORDER BY r.rental_date
;

RETURN;
END;
$$


CALL refresh_data();


-- The next few commands are for manipulating the above code and are for demonstration purposes only --

SELECT * FROM detailed_table ORDER BY rental_date_ts;

SELECT * FROM summary_table;

INSERT INTO detailed_table VALUES (3, 4371, 953, 'Wait Cider', 371, 'May 24, 2005', '2005-05-24 08:44:23');

UPDATE detailed_table SET store_id = 4 WHERE store_id = 3;


-- The next few commands are for deleting the date from the tables --
-- and for dropping the trigger, procedures, fucntions, and tables --

DELETE FROM summary_table;
DELETE FROM detailed_table;
DROP TRIGGER summary_table_insert ON detailed_table;
DROP TRIGGER summary_table_update ON detailed_table;
DROP PROCEDURE refresh_data();
DROP TABLE detailed_table;
DROP TABLE summary_table;