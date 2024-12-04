package testconn

import (
	"os"
	"path"
	"testing"

	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/pkg/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestTestconnConnectorSuite(t *testing.T) {
	p := path.Join(os.TempDir(), "test")
	assert.NoError(t, os.MkdirAll(p, os.ModePerm))
	bootstrapFile, err := os.Create(path.Join(p, "bootstrap.json"))
	assert.NoError(t, err)
	defer bootstrapFile.Close()
	_, err = bootstrapFile.WriteString(`{"_id": {"$oid":"66f4c69291ab1a5533945e37"},"a": "b"}`)
	assert.NoError(t, err)
	assert.NoError(t, bootstrapFile.Close())
	updatesFile, err := os.Create(path.Join(p, "updates.json"))
	assert.NoError(t, err)
	defer updatesFile.Close()
	_, err = updatesFile.WriteString(`insert	{"$oid":"66f4c69291ab1a5533945e79"}	{"_id": {"$oid":"66f4c69291ab1a5533945e79"},"a": "b"}`)
	assert.NoError(t, err)
	assert.NoError(t, updatesFile.Close())

	tSuite := test.NewConnectorTestSuite("test", func() adiomv1connect.ConnectorServiceClient {
		return test.ClientFromHandler(NewConn(p))
	}, nil, nil, 1, 1)
	suite.Run(t, tSuite)
}
