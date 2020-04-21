Basic kafka setup.

Install homebrew. https://brew.sh/

Then brew install docker

Then goto https://docs.docker.com/docker-for-mac/install/ 

Open terminal and ensure docker is up and running docker run hello-world

Verify in docker desktop

Start local zookeeper 
docker run --name zookeeper -h localhost -p 2181:2181 zookeeper

Verify in docker desktop

Start local kafka
docker run -p 9092:9092 --name kafka -e KAFKA_ZOOKEEPER_CONNECT=yourlocalip:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://yourlocalip:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -d confluentinc/cp-kafka

Verify in docker desktop. 

Clone repo and run topic.js, then producer and then consumer. 