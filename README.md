# Data Fabric Self-contained Demo

- Clone the repo `git clone https://github.com/erdincka/dfdemo.git; cd dfdemo`

- Run containers using `docker compose -f docker-compose.yaml up -d`

- Wait for all containers to start, might take 15+ minutes!

- Wait for `app` container to become *Ready*: Run `docker ps -l` and ensure "STATUS" shows **(healthy)**

- Open port :8501 on docker host (if running locally, it would be http://localhost:8501/)

! 20GB+ memory required for docker.
