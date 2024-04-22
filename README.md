# Workshop 2 Spotify & Grammys 

# Overview
Welcome, this project is an ETL process implemented in Apache Airflow from two data sets, Spotify Dataset and The Grammy Awards Dataset, using tools such as Docker, Apache Airflow, Postgres, Power BI, Python, SQLAlchemy and Jupyter Notebooks.

Made by Emmanuel Quintero

# Ex:
![Workshop Dag](image.png)

# Tools used

- **Python**
- **Jupyter Notebooks**
- **PostgreSQL 15**
- **SQLAlchemy**
- **Pandas**
- **Matplotlib**
- **Docker Desktop**
- **Power BI**
- **Seaborn**
- **Apache Airflow**

# About the data
The data sets used come from Kaggle, you can find them here:
- [Spotify Dataset](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset)
- [The Grammy Awards](https://www.kaggle.com/datasets/unanimad/grammy-awards) 

# Project Features

- Docker Implementation: The project is located in a Docker container, thus facilitating its operation

- Exploratory data analysis (EDA): In notebooks 001_EDA and 002_EDA, respectively, there are the EDA carried out on each data set, there are also the transformations that will be applied later.

- Creation of Dag: A dag like the one seen above is created to automate tasks such as extracting data from the csv and the database, transforming the csv and the database with the transformations proposed in the EDA Notebooks, additionally joining both sets after the transformations, create and upload the data to a new table in postgres and finally upload the resulting dataset to Google Drive through PyDrive2 (API)

- Dag implementation: Once the functions and tasks of the dag have been created and connected, localhost:8080 is accessed to be able to run the dag (a step by step will be done later)

- Visualizations: Finally, you access PostgreSQL from Power BI, which is where the new table with the data is located after the transformations and the merge to perform the visualizations.

# Requirements

- Install Python 3.9
- Install PostgreSQL 15
- Install PowerBI
- Install Docker Desktop

# Database Configuration

In order to create the tables in postgres, I recommend creating a config folder with two files in json format

One to connect to Postgres without having docker running and another to connect when docker is running since when docker is running the parameters will not be the same. However, both will have the same structure.

This is conection.json

```json
{
    "host": "your_postgres_host",
    "port": "your_postgres_port",
    "database": "database_name",
    "user": "your_postgres_user",
    "password": "your_postgres_password"
}
```

And this conection_air.json
```json
{
    "host": "host.docker.internal",
    "port": "your_postgres_port",
    "database": "database_name",
    "user": "your_postgres_user",
    "password": "your_postgres_password"
}
```

### Note: The creation of the folder and files is done once the project is cloned, as we will do below:

# To Run this project

1. Clone the project:
```bash
  git clone https://github.com/emmanuelqp/WorkShop2.git
```
2. Go to the project directory
```bash
cd WorkShop2
```
### Note: Once you are at the root of the project you can create the config folder with the mentioned files
3. Create virtual environment for Python:
```bash
python -m venv venv
```
4. Activate virtual environment:
```bash
.\venv\Scripts\activate
```
5. Install libraries:
```bash
pip install requirements.txt
```
6. Create a database in PostgreSQL

7. The project has 3 Jupyter notebooks, the first 2 are where the respective EDa is done to the data sets and the 3 is where what transformations can be done to the merge are tested. In the dags folder, specifically in the etl.py file, you will find the functions that each task contains, in the etl_dag.py file there will be the connections for each task. Additionally, you will find a transformations folder which is where all the transformations that will be applied to each dataset are defined. The project has a Data folder which is where the csv are hosted, a pydrive.py file that will be explained later, a requirements.txt and for the docker configuration a Dockerfile and the docker compose.yaml

### Advice: Before starting with Airflow I recommend this video to be able to upload the file to Drive, since you will have to do this before starting airflow

[PyDrive2 Guide](https://www.youtube.com/watch?v=ZI4XjwbpEwU)

8. To Start Airflow:

You open the terminal and verify that you are inside WorkShop2, once this is done you enter the following command:

```bash
docker-compose up airflow-init
```
### Note: So that you can run the command without problem you must have docker desktop open
And once the message exited with code 0 appears, do this:
```bash
docker-compose up
```


You wait for something like this to appear:
![alt text](image-1.png)
And as soon as you get it you can enter 'localhost:8080'

Once you put localhost:8080 in your browser, put the username: airflow and the password: airflow and you will be able to enter

If you get an error like this:
![alt text](<Imagen de WhatsApp 2024-04-21 a las 16.07.59_6031e950.jpg>)

Then you will go to your Docker Desktop and look for these containers (it doesn't matter what order you do):

- flower-1
- airflow-worker-1
- airflow-scheduler-1
- airflow-webserver-1

You will enter each of them and look for the Exec option, inside exec something like this will appear

```bash
(airflow)
```
Once there you write PyDrive2 and hit enter
It would look like this:
```bash
(airflow) pip install PyDrive2
```

Once you have done this in each container, this should appear in airflow:
![alt text](<Imagen de WhatsApp 2024-04-21 a las 16.11.12_4d7bd70c.jpg>)

And you activate the dag and you can now run it
![Workshop Dag](image.png) Like the example

Once it has run completely, you can log into your postgres and verify that the table has been created. In my case the table is called MusicAwards
![alt text](image-2.png)

You can also verify that the file has been uploaded to the drive folder that you configured for PyDrive2

For visualizations:

9. Go to Powerr BI

You create a new report
![alt text](image-3.png)

You select get data (Obtener Datos)
![alt text](image-4.png)

Search for Postgres and select Postgres Database
![alt text](image-5.png)
You put your server (here you can put localhost) since the table is saved in your local postgres and the name of your database
![alt text](image-6.png)

You look for the table that contains the merge and you can now make your visualizations


Thank you for visiting my repository, if you have any questions, don't forget to contact me

You can see my dashboard here:[My DashBoard](https://app.powerbi.com/view?r=eyJrIjoiMTEwM2Q5M2UtMjZhNC00YTk0LWE2YmMtMDg1OGU2YTU5ODI2IiwidCI6IjY5M2NiZWEwLTRlZjktNDI1NC04OTc3LTc2ZTA1Y2I1ZjU1NiIsImMiOjR9)
