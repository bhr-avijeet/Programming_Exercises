# Kafka

## Maven Dependency

```
<dependencies>
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>0.10.0.1</version>
    </dependency>

    <dependency>
        <groupId>com.google.guava</groupId>
        <artifactId>guava</artifactId>
        <version>18.0</version>
    </dependency>
</dependencies>
```

### Creating Uber JAR

**Reference Link** : https://qr.ae/pGDAR4

pom.xml

```
<build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                        <configuration>
                            <descriptorRefs>
                                <descriptorRef>jar-with-dependencies</descriptorRef>
                            </descriptorRefs>
                        </configuration>
                    </execution>
                </executions>
            </plugin>

        </plugins>
    </build>
```

### **Running Uber JAR**

```
java -cp module01-exploring-kafka-and-core-apis-1.0-SNAPSHOT-jar-with-dependencies.jar org.npntraining.hands_on.SimpleProducer


java -cp module01-exploring-kafka-and-core-apis-1.0-SNAPSHOT-jar-with-dependencies.jar org.npntraining.hands_on.SimpleConsumer
```

## Zookeeper Commands

```
ls /
ls /brokers
ls /brokers/ids
```

## **Pip Installation**

```
pip install kafka-python
```

