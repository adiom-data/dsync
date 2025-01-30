In this directory (sub $URL and $KEY):

```
mvn package
java -cp target/my-app-1-jar-with-dependencies.jar adiom.Main 8089 $URL $KEY
```

Then you can use with dsync in a separate terminal:

```
dsync -ns "testconn.testconncol:mydb.mycontainer" testconn://./fixture grpc://localhost:8089 --insecure
```
