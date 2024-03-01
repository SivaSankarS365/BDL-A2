# BDL-A2
## Task-1
To run the code:
```bash
docker compose up
docker exec -it <container-id> airflow dags trigger -r 123345 task_1_dag
```
Container id can be found using `docker ps`. If permission error throws up then run `chmod -R 777 /opt/airflow` inside the docker container.


