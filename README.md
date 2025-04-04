A data pipeline project for processing IMDb data using Azure services, with final visualization in Power BI. This project shows how to:

Extract and clean IMDb data

Automate processing with Azure Functions

Store the data in a database

Visualize it in Power BI

🔧 Features
ETL Script (etl.py): Extracts and processes IMDb datasets.

Azure Function (function_app.py): Automates the ETL process in the cloud.

Power BI Report (IMDB.pbix): Ready-to-use dashboard for data exploration.

Config Files: Azure function configs (host.json, local.settings.json) and Python deps (requirements.txt).

▶️ How to Run
Install dependencies

bash
Copy
Edit
pip install -r requirements.txt
Run ETL locally

bash
Copy
Edit
python etl.py
Deploy to Azure Functions (optional)
Upload function_app.py to your Azure Function App to run it on a schedule or trigger.

Visualize with Power BI
Open IMDB.pbix in Power BI Desktop and connect it to your database.
