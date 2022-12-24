### Big Data Lambda Architecture with dbt, Apache Spark, Apache Airflow, Apache Kafka, Google Cloud Plaform, and Docker

Credit score is an important metric for banks to rate the credit performance of their applicants. 
They use personal information and financial records of credit card applicants to predict whether these applicants will default in the future or not. 
From these predictions, the banks will then decide if they want to issue credit cards to these applicants or not. 
The banks are asking us to create an end-to-end pipeline to help them handle this problem. 
The original datasets and data dictionary can be found in [here](https://www.kaggle.com/datasets/rikdifos/credit-card-approval-prediction).

#### Project Objectives
The objectives of this projects are described below:
- Create an automated pipeline to flow both batch and stream data from data sources to data warehouses and data marts.
- Create a machine learning model to predict whether applicants will be good customers or bad customers.
- Create a visualization dashboard to get insights from the data, which can be used for business decisions.
- Create an infrastructure as code which makes the codes reusable and scalable for another projects.

#### Data Pipeline
![Final Project IYKRA](https://user-images.githubusercontent.com/110159876/209437559-cb7a541e-bace-4afc-b68c-a81eee454080.jpg)

#### Project Instruction
##### Clone this repository and enter the directory
```bash
git clone https://github.com/yevadrian/big-data-lambda-architecture && cd big-data-lambda-architecture
```

##### Create big data tools stack with Docker Compose
```bash
sudo docker compose up -d
```

##### Create a file named "service-account.json" containing your Google service account credentials
```json
{
  "type": "service_account",
  "project_id": "[PROJECT_ID]",
  "private_key_id": "[KEY_ID]",
  "private_key": "-----BEGIN PRIVATE KEY-----\n[PRIVATE_KEY]\n-----END PRIVATE KEY-----\n",
  "client_email": "[SERVICE_ACCOUNT_EMAIL]",
  "client_id": "[CLIENT_ID]",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/[SERVICE_ACCOUNT_EMAIL]"
}
```

##### Load data to PostgreSQL that act as an external data source
```bash
pip install -r requirements.txt && python3 load_postgres.py
```

##### Add the source and sink connector to Kafka Connect
```bash
sudo bash connectors.sh
```

##### Open Spark to monitor Spark master and Spark workers
```
localhost:8080
```

##### Open Airflow with username and password "airflow" to run the DAG
```
localhost:8090
```

##### Open Kafka Connect to view the active connectors
```
localhost:8083/connectors
```

##### Open Schema Registry to view the active schemas
```
localhost:8081/schemas
```