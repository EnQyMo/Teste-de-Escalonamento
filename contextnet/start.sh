# starts gw, kafka and zookeeper in the background
docker compose -f start-gw.yml up -d --build

# starts pn and gd, showing logs in stdout
docker compose -f contextnet-stationary.yml up --build
