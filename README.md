**Welcome to Adiom's Dsync GitHub Repository!**

_Complete documentation can be found [here](https://docs.adiom.io)_

We solve data migration and replication between databases. We focus on challenges specific to large datasets and mission-critical applications. Our mission is to make database migration and replication easy for engineers and DevOps, reducing the need for custom solutions.

# Introduction to Dsync

Dsync is an open-source command line tool developed by [Adiom](https://adiom.io). Dsync is designed for seamless, fast, secure data migration and synchronization between databases, ensuring resiliency, ease of use, and production-grade performance. Dsync features first-class support for Cosmos DB (MongoDB RU, vCore, NoSQL), DynamoDB, MongoDB, FerretDB. Several connectors are in development: PostgreSQL, HBase, Qdrant, Weaviate. 

Common use cases for dsync:
* Live database migration
* Database replication
* Data mobility

Dsync is lightweight, flexible, and supports external connectors and transformers via gRPC. To run dsync, you just need to run the binary on your laptop, VM, or a docker container. Compute (CPU and RAM) is the only requirement, no storage is needed (except for logs)!

![image](https://github.com/user-attachments/assets/5ad786fb-c180-4716-a443-e7bb0fef3619)

Given source and destination databases, dsync completes an initial sync transferring all data from the source database to the destination database. After the initial sync, it continuously monitors the source database for any changes and updates the destination database accordingly.

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
export COSMOS_DEMO=$(echo bW9uZ29kYjovL2Nvc21vc2RiLWRlbW8taW5zdGFuY2U6SkhiRWpRb2JkWm03YWJEcFp2UzZrWHpBMDRXNTBJd2V4QmlQYnVJWFQ2TElmNkhsV2V4YWphQzhkd042REJ2YVh6ajBnclFrdkwzY0FDRGJONjdxZWc9PUBjb3Ntb3NkYi1kZW1vLWluc3RhbmNlLm1vbmdvLmNvc21vcy5henVyZS5jb206MTAyNTUvP3NzbD10cnVlJnJlcGxpY2FTZXQ9Z2xvYmFsZGImcmV0cnl3cml0ZXM9ZmFsc2UmbWF4SWRsZVRpbWVNUz0xMjAwMDAmYXBwTmFtZT1AY29zbW9zZGItZGVtby1pbnN0YW5jZUA= | base64 --decode)

# Feel free to use your own MongoDB connection string
export MDB_DEST='mongodb://localhost:27017' 

./dsync --progress --logfile dsync.log $COSMOS_DEMO $MDB_DEST
```
Now Dsync should be running! Feel free to interrupt the sync process (via Ctrl+C) it once the initial sync is done. The demo database has a few million records and the resources are shared - allow 5-10 minutes for the process to complete.

## 4. Check the data
```
mongosh $MDB_DEST
```
Congratulations! You should be able to access the 'odc' database and see the collections in it that were migrated from the Cosmos DB.

# Development Tips

Use `docker-compose up` to start up the docker containers which run multiple mongo instances:
* mongo1 - used as a metadata store and as a potential source/sink
* mongo2 - used as a potential source/sink
* mongotest - used by the mongo tests

If you are developing on a mac, you likely need to edit your /etc/hosts file so that the docker hostnames work. Add this line:

```
127.0.0.1       mongo1 mongo2 mongotest
```

You need to export COSMOS_TEST to be able to run the cosmos test. For DynamoDB you need to have localstack running.

```
make clean-testdb # cleans and populates mongotest for the mongo connector test
make test # just tests without external dependencies
make test-all # if you have everything set up properly, you can run this
# test specific connectors
make test-mongo
make test-cosmos
make test-dynamodb
```

## Mocks

First time install mockery: `brew install mockery`

After just run `make mocks`

## General testing with the docker instances

```
make clean-dbs
make verify # should succeed
make boostrap1
make verify # verify should fail
make boostrap2
make verify # verify should succeed
make clean-dbs
make bootstrap1
make sync12 # syncs mongo1 to mongo2, have to ctrl-C after a few seconds to stop
make verify # verify should suceed
```


# Questions and reporting issues

Please report any bugs here in the GitHub project.

For technical questions and discussions, please join our [Discord server](https://discord.gg/r4xzVfMQeU).

For commercial inquires, please contact info@adiom.io.
