/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package test

import (
	"context"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/iface/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

/*
* Check that the connector can perform a data integrity check
 */
func (suite *ConnectorTestSuite) TestConnectorDataIntegrity() {
	ctx := context.Background()

	// create mocks for the transport and coordinator
	t := new(mocks.Transport)
	c := new(mocks.Coordinator)

	// transport should return the mock coordinator endpoint
	t.On("GetCoordinatorEndpoint", mock.Anything).Return(c, nil)
	// coordinator should return a connector ID on registration
	testConnectorID := iface.ConnectorID("3")
	var caps iface.ConnectorCapabilities
	c.On("RegisterConnector", mock.Anything, mock.Anything).Return(testConnectorID, nil).Run(func(args mock.Arguments) {
		// Store advertised connector capabilities to skip irrelevant tests
		caps = args.Get(0).(iface.ConnectorDetails).Cap // Perform a type assertion here
	})

	// create a new connector object
	connector := suite.connectorFactoryFunc()

	// setup the connector and make sure it returns no errors
	err := connector.Setup(ctx, t)
	assert.NoError(suite.T(), err)

	// check that the mocked methods were called
	t.AssertExpectations(suite.T())
	c.AssertExpectations(suite.T())

	qdb := "db"
	qcol := "col"
	q := iface.IntegrityCheckQuery{
		Namespace: "db.col",
	}

	// Check if the connector supports integrity check capabilities
	if !caps.IntegrityCheck || suite.SkipIntegrity {
		suite.T().Skip("Skipping test because this connector does not support integrity check capabilities")
	}

	// Connect the test data store
	dataStore := suite.datastoreFactoryFunc()
	err = dataStore.Setup()
	assert.NoError(suite.T(), err)

	// introduce a change in the dataset
	testRecord := map[string]string{
		"a": "1234",
	}

	res, err := connector.IntegrityCheck(ctx, q)
	assert.NoError(suite.T(), err)
	count1 := res.Count

	dataStore.InsertDummy(qdb, qcol+"Other", testRecord)
	res, err = connector.IntegrityCheck(ctx, q)
	assert.NoError(suite.T(), err)
	count2 := res.Count

	dataStore.InsertDummy(qdb, qcol, testRecord)
	res, err = connector.IntegrityCheck(ctx, q)
	assert.NoError(suite.T(), err)
	count3 := res.Count

	// Test new insert in a different same namespace doesn't increment the count
	assert.Equal(suite.T(), count1, count2)
	// Test new insert in the same namespace increments the count
	assert.Equal(suite.T(), count1+1, count3)

	connector.Teardown()
	dataStore.Teardown()
}
