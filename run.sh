docker build -f src/bronze/Dockerfile -t bronze-pipeline .
docker run --env-file .env bronze-pipeline