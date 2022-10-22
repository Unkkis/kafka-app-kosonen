docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
--create \
--zookeeper zookeeper:2128 \
--replication-factor 1 \
--partitions 1 \
--topic task