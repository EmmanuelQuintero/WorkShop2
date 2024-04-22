FROM apache/airflow:2.2.0-python3.9
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
RUN pip install PyDrive2
