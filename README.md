Welcome to Adiom Documentation!

Our focus is on building backend data infrastructure and solving the data mobility problem through design of an open protocol for data exchange and a modular software architecture.

# Introduction to Dsync

Dsync is a one-way data migration tool between MongoDB databases, and overtime will extend to other databases including Azure Cosmos, bidirectional communication, and complex flows. Given source and destination databases, the tool completes an initial sync transferring all data from the source database to the destination database. After the initial sync, it continuosly monitors the source database for any changes and updates the destination database accordingly.

# How to Build
```
git clone https://github.com/adiom-data/dsync.git
cd dsync
go build
```

# How to Run

1) You will need two MongoDB databases - one for the source and one for the destination. 
Could be MongoDB Atlas or local. If the source is local, you need to start it as a replica set to ensure that it supports change streams.
2) It's good to have some data to play around with on the source, but the destination should be empty barring any system namespaces (e.g. admin.*, local.*, system.*)
3) Then you can run the "dsync" app
4) Optionally, you may also want to generate some load on the source (I use SimRunner for this)

# Quickstart

1) Install [MongoDB](https://www.mongodb.com/docs/manual/administration/install-community/) 
2) Start two local MongoDB instances
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
mongorestore --archive=sampledata.archive
```
7) Start the sync
```
./dsync -s $MDB_LOCAL1 -d $MDB_LOCAL2 -m $MDB_LOCAL1 --verbosity INFO
```
Now Dsync should be running! 

8) (Optionally) If you want to start load on the source. Note that you will need Java 11 for this JAR (java --version)
If you don't have Java 11, you may need to build [SimRunner](https://github.com/schambon/SimRunner) yourself or it will be giving weird errors
```
java -jar tools/SimRunner/SimRunner.jar tools/SimRunner/adiom-load.json
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

