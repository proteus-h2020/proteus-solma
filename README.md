# PROTEUS SOLMA Library

### Usage
- Clone proteus-engine [1] (if already cloned, execute ``` git pull origin proteus-dev ```)
- Move to above directory and execute ``` mvn clean install ```
- Clone SOLMA library [3] (if already cloned, execute ``` git pull origin proteus-dev ```)
- Move to above directory and execute ``` mvn install ```
- Copy ``` ./target/proteus-solma_x.xx-x.x-SNAPSHOT-jar-with-dependencies.jar ``` to related repo under ``` ~/.m2/ ```
- To use SOLMA library, create Maven project in your favorite IDE
- Import flink client dependency from maven [2] and copied jar file under ``` ./m2 ```
- For simple examples, please look at [unit tests](src/test/scala/eu/proteus/solma).



[1] https://github.com/proteus-h2020/proteus-engine
[2] https://mvnrepository.com/artifact/org.apache.flink/flink-clients_2.10
[3] https://github.com/proteus-h2020/SOLMA
