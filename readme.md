# ETL Pipeline Project

## Setup Instructions

### 1. Setup MySQL Instance
- Install MySQL and start the MySQL service.
- Create two databases:
  - `source_db`
  - `destination_db`

### 2. Configure Environment Variables
- Copy the `example.env` file and rename it to `.env`.
- Update the `.env` file with your MySQL password:
  ```
  DB_PASSWORD=your_mysql_password
  ```

### 3. Prepare Source Database
- Add a table named `flight_data` in the `source_db` database with the following columns:
  - `id` (integer, primary key)
  - `airline` (string)
  - `departure` (datetime)
  - `arrival` (datetime)
  - `price` (float)
- example `command`.
  ```
  create table flight_data(
  id int auto_increment primary key,
  airline varchar(255),
  departure datetime,
  arrival datetime,
  price float
  )
  ```
- Insert sample data into the `flight_data` table for testing.
-for example-
```
  INSERT INTO flight_data (airline, departure, arrival, price)
   VALUES
   ('Airline 1', '2025-01-17 08:30:00', '2025-01-17 12:45:00', 250.50),
   ('American Airlines', '2025-01-18 10:15:00', '2025-01-18 14:20:00', 325.00),
   ('United Airlines', '2025-01-19 09:00:00', '2025-01-19 13:10:00', 289.99),
   ('Southwest Airlines', '2025-01-20 07:45:00', '2025-01-20 12:00:00', 200.75),
   ('JetBlue', '2025-01-21 06:00:00', '2025-01-21 10:30:00', 180.00);
  ```

### 4. Install Dependencies
- Run the following command to install required Python packages:
  ```
  pip install -r requirement.txt
  ```

### 5. Run the Pipeline
- Execute the ETL pipeline script using:
  ```
  python etlScript.py
  ```

### 6. Check Destination Database
- Verify that the `flight_data_transformed` table is created in the `destination_db` database.
- Ensure the transformed data has been successfully inserted into the `flight_data_transformed` table.

---

## Notes
- Ensure the MySQL server is running and accessible before executing the script.
- Modify the `source_db` and `destination_db` configurations in the script if using different database names.

