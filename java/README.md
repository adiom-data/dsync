Currently this contains a sink connector for CosmosNOSQL

Ensure you have at least Java 21.

In this directory (sub $URL and $KEY):

```
mvn clean install
java -jar target/cosmos-sink-1-jar-with-dependencies.jar 8089 $URL $KEY
```

Then you can use with dsync in a separate terminal:

```
dsync -ns "testconn.testconncol:mydb.mycontainer" testconn://./fixture grpc://localhost:8089 --insecure
```
