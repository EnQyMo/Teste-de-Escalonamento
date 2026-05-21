# starts gw, kafka and zookeeper in the background
sudo docker compose -f start-gw.yml up -d --build

sleep 3

# starts pn and gd, showing logs in stdout
sudo docker compose -f contextnet-stationary.yml up --build
