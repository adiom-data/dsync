**Welcome to Adiom's Dsync GitHub Repository!**

_Complete documentation can be found [here](https://docs.adiom.io)_

We solve data migration and replication between databases. We focus on challenges specific to large datasets and mission-critical applications. Our mission is to make database migration and replication easy for engineers and DevOps, reducing the need for custom solutions.

# Introduction to Dsync

Dsync is an open-source command line tool developed by [Adiom](https://adiom.io). Dsync is designed for seamless, fast, secure data migration and synchronization between databases and data stores, ensuring resiliency, ease of use, and production-grade performance. Technically speaking, Dsync is a **parallelized** in-memory **streaming engine** with **dynamic typing**. 

Dsync features first-class support for **NoSQL and SQL databases**, such as MongoDB, DocumentDB, Cosmos DB NoSQL, HBase, DynamoDB, PostgreSQL, SQL Server, Oracle, as well as **vector stores** such as Qdrant, Weaviate, S3 Vector store. 

Data copy and database migration jobs that previously took weeks, can now **be done in one afternoon** with a single command.

Common use cases for dsync:
* Live database migration
* Database replication
* Data mobility

Dsync is lightweight, flexible, and supports external connectors and transformers via gRPC. To run dsync, you just need to run the binary on your laptop, VM, or a docker container. Compute (CPU and RAM) is the only requirement, no storage is needed (except for logs)!

![image](https://github.com/user-attachments/assets/5ad786fb-c180-4716-a443-e7bb0fef3619)

Given source and destination databases, dsync completes an initial sync transferring all data from the source database to the destination database. After the initial sync, it continuously monitors the source database for any changes and updates the destination database accordingly.

![Dsync progress reporting sample](/img/screenshot.png?width=20&raw=true)

## Supported Connectors

### System

<table><thead><tr><th width="139.41015625">Type</th><th>Version</th><th>Status</th><th>Capabilities</th><th width="183.00390625">Notes</th></tr></thead><tbody><tr><td>/dev/null</td><td>-</td><td>Available</td><td>Source</td><td>Does exactly what you'd expect it to do</td></tr><tr><td>/dev/random</td><td>-</td><td>Available</td><td>Sink</td><td>Generates a stream of random operations</td></tr></tbody></table>

### NoSQL Databases

<table><thead><tr><th width="139.41015625">Type</th><th>Version</th><th>Status</th><th>Capabilities</th><th width="183.00390625">Notes</th></tr></thead><tbody><tr><td>MongoDB</td><td>4.2+</td><td>Available</td><td>Source, Sink</td><td>Supports Atlas dedicated, Atlas serverless and self-managed installations </td></tr><tr><td>Cosmos DB RU (MongoDB API)</td><td>4.2, 6.0+</td><td>Available</td><td>Source, Sink</td><td>Supports Azure Cosmos with Mongo API with Provisioned RUs (not serverless)</td></tr><tr><td>Azure Document DB (Cosmos DB vCore)</td><td>current</td><td>Available</td><td>Source, Sink</td><td>Both managed and Open Source versions are supported</td></tr><tr><td>Cosmos DB NoSQL</td><td>current</td><td>Public Preview</td><td>Source, Sink</td><td></td></tr><tr><td>DynamoDB</td><td>current</td><td>Public Preview</td><td>Source, Sink</td><td></td></tr><tr><td>HBase</td><td>1.x, 2.x</td><td><em>Private Preview</em></td><td>Source, Sink</td><td>Includes CDC support<br>See <a href="https://www.adiom.io/post/hbase-to-mongodb-migration">blog</a> for details</td></tr><tr><td>AWS DocumentDB</td><td>4.0, 5.0</td><td>Available</td><td>Source, Sink</td><td>Support via generic MongoDB connector</td></tr></tbody></table>

### SQL Databases / RDBMS

<table><thead><tr><th width="139.41015625">Type</th><th>Version</th><th>Status</th><th>Capabilities</th><th width="183.00390625">Notes</th></tr></thead><tbody><tr><td>PostgreSQL</td><td>15+</td><td>Available</td><td>Source, Sink</td><td>Direct connectivity for 1:1 migrations with transformations</td></tr><tr><td>SQL Server / PostgreSQL / Oracle</td><td>-</td><td><em>Private Preview</em></td><td>Source</td><td>Custom query-based via our "SQL batch" connector<br>See <a href="https://www.adiom.io/post/migrate-rdbms-to-mongodb">blog</a> for details</td></tr></tbody></table>

### Vector Databases

<table><thead><tr><th width="139.41015625">Type</th><th>Version</th><th>Status</th><th>Capabilities</th><th width="183.00390625">Notes</th></tr></thead><tbody><tr><td>Weaviate</td><td><em>latest</em></td><td>Public Preview</td><td>Sink</td><td></td></tr><tr><td>Qdrant</td><td><em>latest</em></td><td><em>In Development</em></td><td>Sink</td><td></td></tr><tr><td>S3 Vector Index</td><td>-</td><td><em>Public Preview</em></td><td>Sink</td><td></td></tr></tbody></table>

### Other

<table><thead><tr><th width="139.41015625">Type</th><th>Version</th><th>Status</th><th>Capabilities</th><th width="183.00390625">Notes</th></tr></thead><tbody><tr><td>S3 Storage</td><td><em>-</em></td><td>Available</td><td>Source, Sink</td><td>Export into and from S3 in JSON format. CDC isn't supported: use "--mode InitialSync"</td></tr></tbody></table>


# Quickstart
Follow these simple instructions to get dsync up and running and perform a migration from our Cosmos DB demo instance to your MongoDB destination.

## 1. Get Dsync

You can build dsync from the source to get all the latest changes or a specific release using tags: 
```
git clone https://github.com/adiom-data/dsync.git
cd dsync
go build
```

Alternatively, you can use docker to run the latest release:
```
docker run --network host --rm markadiom/dsync
```

You can also download the latest release as a binary from the [GitHub Releases](https://github.com/adiom-data/dsync/releases/latest) page.

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
