FROM apache/airflow:2.0.2-python3.6
COPY requirements.txt .
RUN pip install -r requirements.txt