for file in $(sudo docker exec airflow-airflow-worker-1 ls /opt/airflow/Staging/); do
    sudo docker cp airflow-airflow-worker-1:/opt/airflow/Staging/$file ./Staging/
done

for file in $(sudo docker exec airflow-airflow-worker-1 ls /opt/airflow/StarSchema/); do
    sudo docker cp airflow-airflow-worker-1:/opt/airflow/StarSchema/$file ./StarSchema/
done

sudo gsutil cp -r ./Staging/* gs://ike-airflow/Staging/
sudo gsutil cp -r ./StarSchema/* gs://ike-airflow/StarSchema/