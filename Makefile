.PHONY: dsync clean clean-dbs verify bootstrap1 bootstrap2 sync12 test-all test clean-testdb test-mongo test-cosmos mocks

RUN_CMD=go run main.go

dsync:
	go build -o dsync main.go

clean:
	go clean -modcache

clean-dbs:
	PORT=27017 ./scripts/clean-mongo.sh
	PORT=27018 ./scripts/clean-mongo.sh

verify:
	$(RUN_CMD) -s "mongodb://mongo1:27017/?replicaSet=data1" -d "mongodb://mongo2:27018/?replicaSet=data2" -m "mongodb://mongo1:27017/?replicaSet=data1" --verbosity INFO --verify

bootstrap1:
	$(RUN_CMD) -d "mongodb://mongo1:27017/?replicaSet=data1" -sourcetype testconn -s ./fixture -m "mongodb://mongo1:27017/?replicaSet=data1" --verbosity INFO

bootstrap2:
	$(RUN_CMD) -d "mongodb://mongo2:27018/?replicaSet=data2" -sourcetype testconn -s ./fixture -m "mongodb://mongo1:27017/?replicaSet=data1" --verbosity INFO

sync12:
	$(RUN_CMD) -s "mongodb://mongo1:27017/?replicaSet=data1" -d "mongodb://mongo2:27018/?replicaSet=data2" -m "mongodb://mongo1:27017/?replicaSet=data1" --verbosity INFO

test-all:
	go test -count=1 -v -p 1 --tags=external ./...

test:
	go test ./...

test-cosmos:
	echo 'Ensure that you have exported COSMOS_TEST pointing to your instance'
	go test -count=1 -v --tags=external github.com/adiom-data/dsync/connectors/cosmos

clean-testdb:
	PORT=27019 ./scripts/clean-mongo.sh	
	$(RUN_CMD) -d "mongodb://mongotest:27019/?replicaSet=datatest" -sourcetype testconn -s ./fixture -m "mongodb://mongotest:27019/?replicaSet=datatest" --verbosity INFO --namespace testconn.test1,testconn.test2,testconn.test3,testconn.test4

test-mongo:
	MONGO_TEST=mongodb://mongotest:27019/?replicaSet=datatest go test -count=1 -v --tags=external github.com/adiom-data/dsync/connectors/mongo

mocks:
	mockery --output ./protocol/iface/mocks --srcpkg ./protocol/iface --name Connector
	mockery --output ./protocol/iface/mocks --srcpkg ./protocol/iface --name Coordinator
	mockery --output ./protocol/iface/mocks --srcpkg ./protocol/iface --name Runner
	mockery --output ./protocol/iface/mocks --srcpkg ./protocol/iface --name Transport
	mockery --output ./protocol/iface/mocks --srcpkg ./protocol/iface --name ConnectorICoordinatorSignal
	mockery --output ./protocol/iface/mocks --srcpkg ./protocol/iface --name CoordinatorIConnectorSignal
	mockery --output ./protocol/iface/mocks --srcpkg ./protocol/iface --name Statestore
