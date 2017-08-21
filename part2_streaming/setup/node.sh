On each kafka node:
sudo yum -y install gcc
sudo /usr/local/bin/pip install glances bottle
nohup glances -w 2>&1 &
/opt/confluent/bin/kafka-topics --create --if-not-exists --partitions 10 --replication-factor 2 --zookeeper localhost:2181 --topic test-1000