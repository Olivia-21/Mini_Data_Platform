Metabase Setup (Manual Steps)
=============================

1. Start the stack:
   - `docker compose up -d`

2. Open Metabase:
   - Go to `http://localhost:3000` in your browser.

3. Initial configuration:
   - Create an admin user (any details you like).
   - When asked for a database, choose **PostgreSQL**:
     - Host: `mdp_postgres`
     - Port: `5432`
     - Database name: `mdp_db`
     - Username: `mdp_user`
     - Password: `mdp_pass`

4. Expose the `gold` schema:
   - In Metabase, go to **Admin** → **Databases** → select your PostgreSQL connection.
   - Make sure the `gold` schema is scanned / synced.

5. Build simple dashboards:
   - Use the table `gold.sales_agg_daily`.
   - Example questions:
     - Total revenue by day.
     - Top products by revenue.
     - Quantity sold by day and product.

6. Save a dashboard:
   - Create a new dashboard called **Sales Overview**.
   - Add the above questions as charts.

