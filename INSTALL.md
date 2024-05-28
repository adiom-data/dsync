# How to build
Simple "go build" should work

# How to run
1) You will need two MongoDB databases - one for the source and one for the destination. 
Could be Atlas or local. If the source is local, you need to start it as a replica set to ensure that it supports change streams.
2) It's good to have some data to play around with on the source, but the destination should be empty barring any system namespaces (e.g. admin.*, local.*, system.*)
3) Then you can run the "dsync" app
4) Optionally, you may also want to generate some load on the source (I use SimRunner for this)

# Quickstart

1) Checkout git and build
```
git clone https://github.com/adiom-data/dsync.git
cd dsync
go build
```

2) Install mongodb 
3) Start two local mongodb instances
```
mkdir ~/temp
cd ~/temp
mkdir data1 data2
mongod --dbpath data1 --logpath mongod1.log --replSet data1 --fork --port 27017
mongod --dbpath data2 --logpath mongod2.log --replSet data2 --fork --port 27018
```
4) Save connection strings in env variables
```
export MDB_LOCAL1=mongodb://localhost:27017
export MDB_LOCAL2=mongodb://localhost:27018
```
5) Connect to each and initialize it as a replica set
```
mongosh $MDB_LOCAL1
rs.initiate()
```
```
mongosh $MDB_LOCAL2
rs.initiate()
```
6) Import data 
```
curl -O https://atlas-education.s3.amazonaws.com/sampledata.archive
mongorestore --archive=Downloads/sampledata.archive
```
7) Start the sync
```
./dsync -s $MDB_LOCAL1 -d $MDB_LOCAL2 -m $MDB_LOCAL1 --verbosity INFO
```
8) (Optionally) If you want to start load on the source. Note that you will need Java 11 for this JAR (java --version)
```
java -jar SimRunner/SimRunner.jar SimRunner/adiom-load.json
```

# Cleanup

* Kill all running mongod processes
```
killall mongod
```
* Remove all data from a mongo instance
```
mongosh <URI>
var dbs = db.getMongo().getDBNames()
for (var i in dbs) { db = db.getMongo().getDB(dbs[i]); print("dropping db " + db.getName()); (!['admin','config','local'].includes(db.getName())) && db.dropDatabase(); }
```