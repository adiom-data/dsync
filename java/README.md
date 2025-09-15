Source and sink connector for CosmosNOSQL

Ensure you have at least Java 21.

In this directory (sub $URL and $KEY):

```
mvn clean install
OTEL_SDK_DISABLED=true java -jar target/cosmos-connector-1-jar-with-dependencies.jar 8089 $URL $KEY
```
If you want it to send telemetry to a specific collector (by default, it will try to connect to localhost:4317):
```
OTEL_EXPORTER_OTLP_ENDPOINT=http://<hostname>:4317 java -jar target/cosmos-connector-1-jar-with-dependencies.jar 8089 $URL $KEY
```


Then you can use with dsync in a separate terminal:

```
dsync -ns "testconn.testconncol:mydb.mycontainer" testconn://./fixture grpc://localhost:8089 --insecure
```
