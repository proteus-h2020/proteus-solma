# PROTEUS SOLMA Library
Solma is  a scalable online machine learning algorithms (including classification, clustering, regression, ensemble algorithms, graph oriented algorithms, linear algebra operations, and anomaly and novelty detection) implemented on top of Apache Flink using the hybrid processing capabilities.

### Usage
- Clone proteus-engine [1] (if already cloned, execute ``` git pull origin proteus-dev ```)
- From commandline go to above directory and execute ``` mvn clean install ```
- Clone SOLMA library [2] (if already cloned, execute ``` git pull origin proteus-dev ```)
- Move to above directory and execute ``` mvn install ```
- To use SOLMA library, create Maven project (in your favorite IDE)
- You can use sample pom file [4]. Please change project specific fields like ``` <mainClass> ``` to related value 
- For simple examples, please look at [unit tests](src/test/scala/eu/proteus/solma).
- To generate jar with a given project, go to related directory (with command line) and execute ``` mvn clean install ```
- Use jar file under ``` ./target/ ``` and use [3] to exetuce jar with flink



[1] https://github.com/proteus-h2020/proteus-engine
[2] https://github.com/proteus-h2020/SOLMA
[3] https://ci.apache.org/projects/flink/flink-docs-release-1.2/setup/cli.html
[4]
```scala
    <dependencies>
        <dependency>
            <groupId>eu.proteus</groupId>
            <artifactId>proteus-solma_2.10</artifactId>
            <version>0.1-SNAPSHOT</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients_2.10</artifactId>
            <version>1.3-SNAPSHOT</version>
            <scope>compile</scope>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <configuration>
                    <recompileMode>incremental</recompileMode>
                </configuration>
                <executions>
                    <execution>
                        <id>scala-compile-first</id>
                        <phase>process-resources</phase>
                        <goals>
                            <goal>add-source</goal>
                            <goal>compile</goal>
                        </goals>
                    </execution>
                    <execution>
                        <id>scala-test-compile</id>
                        <phase>process-test-resources</phase>
                        <goals>
                            <goal>add-source</goal>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>2.4</version>
                <configuration>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                    <archive>
                        <manifest>
                            <mainClass>solma.TestSolma</mainClass>
                        </manifest>
                    </archive>
                </configuration>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-dependency-plugin</artifactId>
                <executions>
                    <execution>
                        <id>copy-dependencies</id>
                        <phase>package</phase>
                        <goals>
                            <goal>copy-dependencies</goal>
                        </goals>
                        <configuration>
                            <useBaseVersion>false</useBaseVersion>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>


```

