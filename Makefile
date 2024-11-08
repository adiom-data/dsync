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
	$(RUN_CMD) -m "mongodb://mongo1:27017/?replicaSet=data1" --verbosity INFO --verify "mongodb://mongo1:27017/?replicaSet=data1" "mongodb://mongo2:27018/?replicaSet=data2"

bootstrap1:
	$(RUN_CMD) -m "mongodb://mongo1:27017/?replicaSet=data1" --verbosity INFO testconn://./fixture "mongodb://mongo1:27017/?replicaSet=data1"

bootstrap2:
	$(RUN_CMD) -m "mongodb://mongo1:27017/?replicaSet=data1" --verbosity INFO testconn://./fixture "mongodb://mongo2:27018/?replicaSet=data2"

sync12:
	$(RUN_CMD) -m "mongodb://mongo1:27017/?replicaSet=data1" --verbosity INFO "mongodb://mongo1:27017/?replicaSet=data1" "mongodb://mongo2:27018/?replicaSet=data2"

test-all:
	go test -count=1 -v -p 1 --tags=external ./...

test:
	go test ./...

test-cosmos:
	echo 'Ensure that you have exported COSMOS_TEST pointing to your instance'
	go test -count=1 -v --tags=external github.com/adiom-data/dsync/connectors/cosmos

clean-testdb:
	PORT=27019 ./scripts/clean-mongo.sh	
	$(RUN_CMD) -m "mongodb://mongotest:27019/?replicaSet=datatest" --verbosity INFO --namespace testconn.test1,testconn.test2,testconn.test3,testconn.test4 testconn://./fixture "mongodb://mongotest:27019/?replicaSet=datatest"

test-mongo:
	MONGO_TEST=mongodb://mongotest:27019/?replicaSet=datatest go test -count=1 -v --tags=external github.com/adiom-data/dsync/connectors/mongo

test-dynamodb:
	echo 'Ensure that localstack is running'
	AWS_ACCESS_KEY_ID=foobar AWS_SECRET_ACCESS_KEY=foobar go test -count=1 -v --tags=external github.com/adiom-data/dsync/connectors/dynamodb

mocks:
	mockery --output ./protocol/iface/mocks --srcpkg ./protocol/iface --name Connector
	mockery --output ./protocol/iface/mocks --srcpkg ./protocol/iface --name Coordinator
	mockery --output ./protocol/iface/mocks --srcpkg ./protocol/iface --name Runner
	mockery --output ./protocol/iface/mocks --srcpkg ./protocol/iface --name Transport
	mockery --output ./protocol/iface/mocks --srcpkg ./protocol/iface --name ConnectorICoordinatorSignal
	mockery --output ./protocol/iface/mocks --srcpkg ./protocol/iface --name CoordinatorIConnectorSignal
	mockery --output ./protocol/iface/mocks --srcpkg ./protocol/iface --name Statestore
