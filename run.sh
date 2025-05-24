sudo service docker start
sudo docker build -f src/bronze/Dockerfile -t bronze-pipeline .
sudo docker run --env-file .env bronze-pipeline