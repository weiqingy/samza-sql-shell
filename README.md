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
    mvn package

## Populate data

### Download Samza tool and use it to generate data
For example, you can use [samza-tools-0.14.1](http://samza.apache.org/startup/download/) to generate data for Kafka topics like “ProfileChangeStream”.

    cd  samza-tools-0.14.1
    ./scripts/generate-kafka-events.sh -t ProfileChangeStream -e ProfileChange

## Run SamzaSQL shell
We'll enable users to build a fat jar, then Samza libraries "samza-tools-0.14.1/lib/*" do not need to be included in classpath. 

    java -Dlog4j.configuration=file:samza-sql-shell/src/main/resources/log4j.xml -cp samza-tools-0.14.1/lib/*:samza-sql-shell/target/samza-sql-shell-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.samza.tools.client.cli.Main
    
Input your SQL statements in the shell:

    select * from kafka.ProfileChangeStream
    insert into kafka.ProfileChangeStream_sink select * from kafka.ProfileChangeStream
    select * from kafka.ProfileChangeStream1
