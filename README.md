# PROTEUS SOLMA Library
Solma is  a scalable online machine learning algorithms (including classification, clustering, regression, ensemble algorithms, graph oriented algorithms, linear algebra operations, and anomaly and novelty detection) implemented on top of Apache Flink using the hybrid processing capabilities.

### Usage
- Clone proteus-engine [1] (if already cloned, execute ``` git pull origin proteus-dev ```)
- From commandline go to above directory and execute ``` mvn clean install ```
- Clone SOLMA library [2] (if already cloned, execute ``` git pull origin develop ```)
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

```
# Algorithms

## SAX-SVM

The SAX-VSM algorithm is implemented as two independent algorithms in
SOLMA: `SAX` and `SAXDictionary`. Both algorithms can be used together
following the SAX-VSM implementation, or they can be used independently.

References for this algorithm can be found at:

```
Senin, Pavel, and Sergey Malinchik. "Sax-vsm: Interpretable time series
classification using SAX and vector space model."
2013 IEEE 13th International Conference on Data Mining (ICDM),
IEEE, 2013.
```

### SAX

The SAX algorithm provides a method to transform a `DataStream[Double]`
into a set of words supporting PAA transformation.

To train the SAX and use it to transform a time series use the following
approach:

```
 // Obtain the training and evaluation datasets.
 val trainingDataSet : DataSet[Double] = env.fromCollection(...)
 val evalDataSet : DataStream[Double] = streamingEnv.fromCollection(...)

 // Define the SAX algorithm
 val sax = new SAX().setPAAFragmentSize(2).setWordSize(2)
 // Fit the SAX
 sax.fit(trainingDataSet)
 // Transform the datastream
 val transformed = sax.transform(evalDataSet)
```

The algorithm accepts the following parameters:
* **PAAFragmentSize**: It indicates the number of elements that will be
averaged during the PAA process.
* **WordSize**: It determines the size of the words to be used
* **AlphabetSize**: It determines the size of the alphabet in terms of
number of symbols

### SAXDictionary

```
// Dataset 1 corresponds to class 1
val dataset1 : DataSet[String] = env.fromCollection(...)
val toFit1 = dataset1.map((_, 1))
// Dataset 2 corresponds to class 2
val dataset2 : DataSet[String] = env.fromCollection(trainingData2)
val toFit2 = dataset2.map((_, 2))
// Build the SAX dictionary
val saxDictionary = new SAXDictionary()
// Fit the dictionary
saxDictionary.fit(toFit1)
saxDictionary.fit(toFit2)
// Evaluation datastream
val evalDataSet : DataStream[String] = streamingEnv.fromCollection(...)
// Determine the number of words in the window
val evalDictionary = saxDictionary.setNumberWords(3)
val predictions = evalDictionary.predict[String, SAXPrediction](evalDataSet)
```

The algorithm supports the following parameters:
* **NumberWords**: It determines the number of words that need to
appear in a window to compute the prediction.

### SAX + SAXDictionary

To use both algorithms:

1. Select a training set from the signal
2. Use the training set to fit the `SAX` algorithm
3. Transform the training set to obtain the words and use that to fit
the `SAXDictionary`
4. Select the evaluation stream
5. Connect the evaluation stream to pass first through the `SAX`
algorithm and then through the `SAXDictionary` to obtain the
predictions.

