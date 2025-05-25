

sudo service docker start
sudo docker build -f src/bronze/Dockerfile -t study-case-20250521-bronze-pipeline .
sudo docker build -f src/silver/Dockerfile -t study-case-20250521-silver-pipeline .
sudo docker build -f src/gold/Dockerfile -t study-case-20250521-gold-pipeline .
sudo docker compose up -d


# To remove airflow containers and images:
# sudo docker compose down --volumes --rmi all
# docker compose down --volumes --rmi all

# To run the containers without Airflow:
# sudo docker run --env-file .env study-case-20250521-bronze-pipeline
# sudo docker run --env-file .env study-case-20250521-silver-pipeline
# sudo docker run --env-file .env study-case-20250521-gold-pipeline

# Commands to create buckets automatically:
# terraform -chdir=terraform init
# terraform -chdir=terraform apply