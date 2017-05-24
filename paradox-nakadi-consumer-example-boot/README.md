# paradox-nakadi-consumer-example-boot

### Local tests with Nakadi staging server

1) get application credentials

    berry -a <stack name> -m <mint bucket> --once .

2) run application

    java -Dspring.profiles.active=local-simple -jar ./paradox-nakadi-consumer-example-boot-0.0.1-SNAPSHOT.jar


### Local Zookeeper/Exhibitor tests with Nakadi staging server

1) start local exhibitor

   HOSTNAME by which the container knows itself i.e. `-h localhost` is registered as a server in ensemble


    docker run -h localhost -p 2181:2181 -p 2888:2888 -p 3888:3888 -p 8181:8181 -it <docker image>:<version>

2) get application credentials

    berry -a <stack name> -m <mint bucket> --once .

3) run application

    java -Dspring.profiles.active=local-zk -jar ./paradox-nakadi-consumer-example-boot-0.0.1-SNAPSHOT.jar


For the mock server, there are two sample responses under resources folder.