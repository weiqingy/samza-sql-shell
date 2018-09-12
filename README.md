# samza-sql-shell

## Compile

Currently SamzaSQL shell depends on Samza version [0.14.1](http://samza.apache.org/startup/download/).

### Download Samza

    tar -xvzf apache-samza-0.14.1-src.tgz
    cd  apache-samza-0.14.1-src/

### Build Samza
    gradle -b bootstrap.gradle
    ./gradlew clean build

### Publish the artifacts built from the previous step to local maven repository
    ./gradlew publishToMavenLocal

### Build SamzaSQL shell
    git clone https://github.com/weiqingy/samza-sql-shell.git 
    ./gradlew clean releaseTarGz
    
samza-sql-shell build creates a tar-ball under samza-sql-shell/build/distributions/. Untar it by running the below commands.

    cd samza-sql-shell/build/distributions/
    tar -xvzf *.tgz

## Populate data

### Set up Kafka cluster
This is [Kafka quick start](http://kafka.apache.org/quickstart).
#### Start Zookeeper
    bin/zookeeper-server-start.sh config/zookeeper.properties

#### Start Kafka server
    bin/kafka-server-start.sh config/server.properties

#### List existing topics
    bin/kafka-topics.sh --list --zookeeper localhost:2181

#### Use Kafka tool to create Kafka topics
    ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ProfileChangeStream
    ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ProfileChangeStream_sink

### Download Samza tool and use it to generate data
For example, you can use [samza-tools-0.14.1](http://samza.apache.org/startup/download/) to generate data for Kafka topics like “ProfileChangeStream”.

    cd  samza-tools-0.14.1
    ./scripts/generate-kafka-events.sh -t ProfileChangeStream -e ProfileChange

## Run SamzaSQL shell

    cd samza-sql-shell-<version>
    ./scripts/samza-shell.sh
    
Input your SQL statements in the shell:

    select * from kafka.ProfileChangeStream
    insert into kafka.ProfileChangeStream_sink select * from kafka.ProfileChangeStream
    select * from kafka.ProfileChangeStream_sink
    ...
    
