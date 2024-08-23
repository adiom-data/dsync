**Welcome to Adiom Documentation!**

_Complete documentation can be found [here](https://docs.adiom.io)_

We solve solve data migration and replication between NoSQL databases. We focus on challenges specific to large datasets and mission-critical applications. Our approach involves creating an open data object exchange protocol, a lightweight platform, and open-source tools to streamline data replication and movement.

# Introduction to Dsync
> **_NOTE: Dsync is currently in beta and is undergoing active development and testing._**


Dsync is a data migration tool between NoSQL databases. Over time it will extend to many different databases including Azure Cosmos, MongoDB, and advanced features such as bidirectional communication and many-to-many flows. 

Given source and destination databases, the tool completes an initial sync transferring all data from the source database to the destination database. After the initial sync, it continuously monitors the source database for any changes and updates the destination database accordingly.

![Dsync progress reporting sample](/img/screenshot.png?width=20&raw=true)

# Quickstart
Follow these simple instructions to get dsync up and running and perform a migration from our Cosmos DB demo instance to your MongoDB destination.

## 1. Build dsync
```
git clone https://github.com/adiom-data/dsync.git
cd dsync
go build
```
Alternatively, you can download the latest release from the [GitHub Releases](https://github.com/adiom-data/dsync/releases/latest) page.

## 2. (Optional) Prepare the destination MongoDB instance

If you already have the desired destination MongoDB instance up and running, you can skip this step. Otherwise, use the following steps to launch MongoDB on your local machine:

1) Install [MongoDB](https://www.mongodb.com/docs/manual/administration/install-community/) 
2) Start a local MongoDB instance
```
mkdir ~/temp
cd ~/temp
mkdir data_d
mongod --dbpath data_d --logpath mongod_d.log --fork --port 27017
```

## 3. Start dsync
```
# You can use our publicly accessible (read-only) Cosmos DB instance
export COSMOS_DEMO='mongodb://cosmosdb-demo-instance:JHbEjQobdZm7abDpZvS6kXzA04W50IwexBiPbuIXT6LIf6HlWexajaC8dwN6DBvaXzj0grQkvL3cACDbN67qeg==@cosmosdb-demo-instance.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@cosmosdb-demo-instance@'

# Feel free to use your own MongoDB connection string
export MDB_DEST='mongodb://localhost:27017' 

./dsync -s $COSMOS_DEMO -d $MDB_DEST --progress --logfile dsync.log
```
Now Dsync should be running! Feel free to interrupt the sync process (via Ctrl+C) it once the initial sync is done. The demo database has a few million records and the resources are shared - allow 5-10 minutes for the process to complete.

## 4. Check the data
```
mongosh $MDB_DEST
```
Congratulations! You should be able to access the 'odc' database and see the collections in it that were migrated from the Cosmos DB.

# Questions and reporting issues

Please report any bugs here in the GitHub project.

For technical questions and discussions, please use the dsync-dev@adiom.io mailing list.

For commercial inquires, please contact info@adiom.io
