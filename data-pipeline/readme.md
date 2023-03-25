# Data pipeline
Пример пайплайна
![img](./flow.png)


## cmds
```
# список с топиками
docker-compose exec kafka kafka-topics --list --zookeeper zookeeper:2181

# создать топик
docker-compose exec kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic events-message-v1
```