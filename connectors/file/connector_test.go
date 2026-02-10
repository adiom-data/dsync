/*
 * Copyright (C) 2025 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package file

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	pkgtest "github.com/adiom-data/dsync/pkg/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
)

func getTestDataPath() string {
	return filepath.Join(".", "test_data")
}

func TestNewConn_ValidDirectory(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + getTestDataPath(),
	})
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func TestNewConn_ValidFile(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + filepath.Join(getTestDataPath(), "valid.csv"),
	})
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func TestNewConn_InvalidUri(t *testing.T) {
	_, err := NewConn(ConnectorSettings{
		Uri: "invalid://path",
	})
	require.Error(t, err, "should reject URI with invalid scheme (not file://)")
}

func TestNewConn_UnsupportedFormat(t *testing.T) {
	_, err := NewConn(ConnectorSettings{
		Uri:    "file://" + getTestDataPath(),
		Format: "json",
	})
	require.ErrorIs(t, err, ErrUnsupportedFormat)
}

func TestNewConn_NonExistentPath(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file:///nonexistent/path/for/testing",
	})
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func TestGetInfo(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + getTestDataPath(),
	})
	require.NoError(t, err)

	resp, err := conn.GetInfo(context.Background(), connect.NewRequest(&adiomv1.GetInfoRequest{}))
	require.NoError(t, err)
	assert.Equal(t, "file", resp.Msg.GetDbType())
	assert.NotNil(t, resp.Msg.GetCapabilities().GetSource())
	assert.NotNil(t, resp.Msg.GetCapabilities().GetSink())
}

func TestGeneratePlan_Directory(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + getTestDataPath(),
	})
	require.NoError(t, err)

	resp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)

	partitions := resp.Msg.GetPartitions()
	assert.NotEmpty(t, partitions)

	namespaces := make(map[string]bool)
	for _, p := range partitions {
		namespaces[p.GetNamespace()] = true
	}

	assert.True(t, namespaces["valid"], "should have 'valid' namespace")
	assert.True(t, namespaces["subdir.nested"], "should have 'subdir.nested' namespace")
}

func TestGeneratePlan_SingleFile(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + filepath.Join(getTestDataPath(), "valid.csv"),
	})
	require.NoError(t, err)

	resp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)

	partitions := resp.Msg.GetPartitions()
	require.Len(t, partitions, 1)
	assert.Equal(t, "valid", partitions[0].GetNamespace())
	assert.Equal(t, uint64(3), partitions[0].GetEstimatedCount())
}

func TestGeneratePlan_FilterNamespaces(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + getTestDataPath(),
	})
	require.NoError(t, err)

	resp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{
		Namespaces: []string{"valid"},
	}))
	require.NoError(t, err)

	partitions := resp.Msg.GetPartitions()
	require.Len(t, partitions, 1)
	assert.Equal(t, "valid", partitions[0].GetNamespace())
}

func TestListData_ValidCSV_JSON(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + filepath.Join(getTestDataPath(), "valid.csv"),
	})
	require.NoError(t, err)

	planResp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)
	require.Len(t, planResp.Msg.GetPartitions(), 1)

	resp, err := conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: planResp.Msg.GetPartitions()[0],
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)

	data := resp.Msg.GetData()
	require.Len(t, data, 3)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(data[0], &doc))
	assert.Equal(t, "1", doc["id"])
	assert.Equal(t, "Alice", doc["name"])
	assert.Equal(t, "30", doc["age"])
}

func TestListData_ValidCSV_BSON(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + filepath.Join(getTestDataPath(), "valid.csv"),
	})
	require.NoError(t, err)

	planResp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)
	require.Len(t, planResp.Msg.GetPartitions(), 1)

	resp, err := conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: planResp.Msg.GetPartitions()[0],
		Type:      adiomv1.DataType_DATA_TYPE_MONGO_BSON,
	}))
	require.NoError(t, err)

	data := resp.Msg.GetData()
	require.Len(t, data, 3)

	var doc map[string]interface{}
	require.NoError(t, bson.Unmarshal(data[0], &doc))
	assert.Equal(t, "1", doc["id"])
	assert.Equal(t, "Alice", doc["name"])
}

func TestListData_EmptyValues(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + filepath.Join(getTestDataPath(), "empty_values.csv"),
	})
	require.NoError(t, err)

	planResp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)

	resp, err := conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: planResp.Msg.GetPartitions()[0],
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)

	data := resp.Msg.GetData()
	require.Len(t, data, 3)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(data[0], &doc))
	assert.Equal(t, "", doc["age"])

	require.NoError(t, json.Unmarshal(data[1], &doc))
	assert.Equal(t, "", doc["name"])
	assert.Equal(t, "", doc["email"])
}

func TestListData_HeaderOnly(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + filepath.Join(getTestDataPath(), "header_only.csv"),
	})
	require.NoError(t, err)

	planResp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)

	resp, err := conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: planResp.Msg.GetPartitions()[0],
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)
	assert.Empty(t, resp.Msg.GetData())
}

func TestListData_SingleRow(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + filepath.Join(getTestDataPath(), "single_row.csv"),
	})
	require.NoError(t, err)

	planResp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)

	resp, err := conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: planResp.Msg.GetPartitions()[0],
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)

	data := resp.Msg.GetData()
	require.Len(t, data, 1)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(data[0], &doc))
	assert.Equal(t, "100", doc["id"])
}

func TestListData_NestedDirectory(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + getTestDataPath(),
	})
	require.NoError(t, err)

	planResp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{
		Namespaces: []string{"subdir.nested"},
	}))
	require.NoError(t, err)
	require.Len(t, planResp.Msg.GetPartitions(), 1)

	resp, err := conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: planResp.Msg.GetPartitions()[0],
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)

	data := resp.Msg.GetData()
	require.Len(t, data, 2)
}

func TestListData_CustomDelimiter(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri:       "file://" + filepath.Join(getTestDataPath(), "semicolon_delimited.csv"),
		Delimiter: ';',
	})
	require.NoError(t, err)

	planResp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)

	resp, err := conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: planResp.Msg.GetPartitions()[0],
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)

	data := resp.Msg.GetData()
	require.Len(t, data, 2)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(data[0], &doc))
	assert.Equal(t, "1", doc["id"])
	assert.Equal(t, "Item1", doc["name"])
	assert.Equal(t, "Value1", doc["value"])
}

func TestListData_SpecialChars(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + filepath.Join(getTestDataPath(), "special_chars.csv"),
	})
	require.NoError(t, err)

	planResp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)

	resp, err := conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: planResp.Msg.GetPartitions()[0],
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)

	data := resp.Msg.GetData()
	require.Len(t, data, 3)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(data[0], &doc))
	assert.Equal(t, "Name, with comma", doc["name"])
	assert.Equal(t, `Description with "quotes"`, doc["description"])

	require.NoError(t, json.Unmarshal(data[2], &doc))
	assert.Contains(t, doc["name"], "\n")
}

func TestListData_InvalidCSV(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + filepath.Join(getTestDataPath(), "invalid.csv"),
	})
	require.NoError(t, err)

	planResp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)

	_, err = conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: planResp.Msg.GetPartitions()[0],
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.Error(t, err, "should fail to parse CSV with malformed quotes")
}

func TestListData_UnsupportedType(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + filepath.Join(getTestDataPath(), "valid.csv"),
	})
	require.NoError(t, err)

	planResp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)

	_, err = conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: planResp.Msg.GetPartitions()[0],
		Type:      adiomv1.DataType_DATA_TYPE_UNKNOWN,
	}))
	require.Error(t, err, "should reject unsupported data type (DATA_TYPE_UNKNOWN)")
}

func TestGetNamespaceMetadata(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + getTestDataPath(),
	})
	require.NoError(t, err)

	resp, err := conn.GetNamespaceMetadata(context.Background(), connect.NewRequest(&adiomv1.GetNamespaceMetadataRequest{
		Namespace: "valid",
	}))
	require.NoError(t, err)
	assert.Equal(t, uint64(3), resp.Msg.GetCount())
}

func TestGetNamespaceMetadata_EmptyNamespace(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + getTestDataPath(),
	})
	require.NoError(t, err)

	resp, err := conn.GetNamespaceMetadata(context.Background(), connect.NewRequest(&adiomv1.GetNamespaceMetadataRequest{
		Namespace: "",
	}))
	require.NoError(t, err)
	assert.Equal(t, uint64(0), resp.Msg.GetCount())
}

func TestWriteData_JSON(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "file_connector_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + tmpDir,
	})
	require.NoError(t, err)

	doc1, _ := json.Marshal(map[string]interface{}{"id": "1", "name": "Test1", "value": "A"})
	doc2, _ := json.Marshal(map[string]interface{}{"id": "2", "name": "Test2", "value": "B"})

	_, err = conn.WriteData(context.Background(), connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "output",
		Data:      [][]byte{doc1, doc2},
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)

	conn.(interface{ Teardown() }).Teardown()

	_, err = os.Stat(filepath.Join(tmpDir, "output.csv"))
	require.NoError(t, err)

	readConn, err := NewConn(ConnectorSettings{
		Uri: "file://" + filepath.Join(tmpDir, "output.csv"),
	})
	require.NoError(t, err)

	planResp, err := readConn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)

	resp, err := readConn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: planResp.Msg.GetPartitions()[0],
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)
	assert.Len(t, resp.Msg.GetData(), 2)
}

func TestWriteData_BSON(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "file_connector_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + tmpDir,
	})
	require.NoError(t, err)

	doc1, _ := bson.Marshal(bson.M{"_id": "1", "name": "Test1"})
	doc2, _ := bson.Marshal(bson.M{"_id": "2", "name": "Test2"})

	_, err = conn.WriteData(context.Background(), connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "bson_output",
		Data:      [][]byte{doc1, doc2},
		Type:      adiomv1.DataType_DATA_TYPE_MONGO_BSON,
	}))
	require.NoError(t, err)

	conn.(interface{ Teardown() }).Teardown()

	readConn, err := NewConn(ConnectorSettings{
		Uri: "file://" + filepath.Join(tmpDir, "bson_output.csv"),
	})
	require.NoError(t, err)

	planResp, err := readConn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)

	resp, err := readConn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: planResp.Msg.GetPartitions()[0],
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)
	assert.Len(t, resp.Msg.GetData(), 2)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Msg.GetData()[0], &doc))
	assert.Equal(t, "1", doc["id"])
}

func TestWriteData_NestedNamespace(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "file_connector_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + tmpDir,
	})
	require.NoError(t, err)

	doc, _ := json.Marshal(map[string]interface{}{"id": "1", "data": "nested"})

	_, err = conn.WriteData(context.Background(), connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "level1.level2.output",
		Data:      [][]byte{doc},
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)

	conn.(interface{ Teardown() }).Teardown()

	expectedPath := filepath.Join(tmpDir, "level1", "level2", "output.csv")
	_, err = os.Stat(expectedPath)
	require.NoError(t, err)
}

func TestWriteData_EmptyData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "file_connector_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + tmpDir,
	})
	require.NoError(t, err)

	_, err = conn.WriteData(context.Background(), connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "empty",
		Data:      [][]byte{},
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)
}

func TestWriteData_CustomDelimiter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "file_connector_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	conn, err := NewConn(ConnectorSettings{
		Uri:       "file://" + tmpDir,
		Delimiter: ';',
	})
	require.NoError(t, err)

	doc, _ := json.Marshal(map[string]interface{}{"id": "1", "name": "Test"})

	_, err = conn.WriteData(context.Background(), connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "delimited",
		Data:      [][]byte{doc},
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)

	conn.(interface{ Teardown() }).Teardown()

	content, err := os.ReadFile(filepath.Join(tmpDir, "delimited.csv"))
	require.NoError(t, err)
	assert.Contains(t, string(content), ";")
}

func TestWriteUpdates_Unimplemented(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + getTestDataPath(),
	})
	require.NoError(t, err)

	_, err = conn.WriteUpdates(context.Background(), connect.NewRequest(&adiomv1.WriteUpdatesRequest{}))
	require.Error(t, err, "WriteUpdates should return unimplemented error for file connector")
}

func TestStreamUpdates_Unimplemented(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + getTestDataPath(),
	})
	require.NoError(t, err)

	err = conn.StreamUpdates(context.Background(), connect.NewRequest(&adiomv1.StreamUpdatesRequest{}), nil)
	require.Error(t, err, "StreamUpdates should return unimplemented error for file connector")
}

func TestStreamLSN_Unimplemented(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + getTestDataPath(),
	})
	require.NoError(t, err)

	err = conn.StreamLSN(context.Background(), connect.NewRequest(&adiomv1.StreamLSNRequest{}), nil)
	require.Error(t, err, "StreamLSN should return unimplemented error for file connector")
}



func TestUnevenRows(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + filepath.Join(getTestDataPath(), "uneven_rows.csv"),
	})
	require.NoError(t, err)

	planResp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)

	_, err = conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: planResp.Msg.GetPartitions()[0],
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.Error(t, err, "should fail to parse CSV with inconsistent number of fields per row")
}

func TestNoIdColumn(t *testing.T) {
	conn, err := NewConn(ConnectorSettings{
		Uri: "file://" + filepath.Join(getTestDataPath(), "no_id_column.csv"),
	})
	require.NoError(t, err)

	planResp, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)

	resp, err := conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: planResp.Msg.GetPartitions()[0],
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)
	assert.Len(t, resp.Msg.GetData(), 2)
}

func TestFileConnectorSuite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "file_connector_suite_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "suite_test.csv")

	tSuite := pkgtest.NewConnectorTestSuite(
		"suite_test",
		func() adiomv1connect.ConnectorServiceClient {
			conn, err := NewConn(ConnectorSettings{
				Uri: "file://" + tmpDir,
			})
			if err != nil {
				t.FailNow()
			}
			return pkgtest.ClientFromHandler(conn)
		},
		func(ctx context.Context) error {
			content := `id,data
1,hi
2,hi2
3,hi3
`
			return os.WriteFile(testFile, []byte(content), 0644)
		},
		nil, // no streaming updates support
		1,   // 1 partition/page
		3,   // 3 items
	)
	// Skip duplicate test since BSON marshaling of maps doesn't guarantee key order
	tSuite.SkipDuplicateTest = true
	// File connector doesn't support WriteUpdates
	tSuite.SkipWriteUpdatesTest = true
	suite.Run(t, tSuite)
}
