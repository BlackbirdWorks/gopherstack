package cloudformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResourceCreator_Glue_Crawler_CreateDelete verifies the crawler is created
// in the Glue backend and removed on delete.
func TestResourceCreator_Glue_Crawler_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newExtraServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"Name": "cfn-test-crawler",
		"Role": "AWSGlueServiceRole",
	}

	physID, err := rc.Create(t.Context(), "MyCrawler", "AWS::Glue::Crawler", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "cfn-test-crawler", physID)

	crawler, err := backends.Glue.Backend.GetCrawler("cfn-test-crawler")
	require.NoError(t, err)
	assert.Equal(t, "cfn-test-crawler", crawler.Name)

	err = rc.Delete(t.Context(), "AWS::Glue::Crawler", physID, nil)
	require.NoError(t, err)
}

// TestResourceCreator_Glue_Trigger_CreateDelete verifies the trigger is created
// and deleted.
func TestResourceCreator_Glue_Trigger_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newExtraServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"Name": "cfn-test-trigger",
		"Type": "ON_DEMAND",
	}

	physID, err := rc.Create(t.Context(), "MyTrigger", "AWS::Glue::Trigger", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "cfn-test-trigger", physID)

	trigger, err := backends.Glue.Backend.GetTrigger("cfn-test-trigger")
	require.NoError(t, err)
	assert.Equal(t, "ON_DEMAND", trigger.Type)

	err = rc.Delete(t.Context(), "AWS::Glue::Trigger", physID, nil)
	require.NoError(t, err)
}

// TestResourceCreator_Glue_Connection_CreateDelete verifies the connection is
// created and deleted.
func TestResourceCreator_Glue_Connection_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newExtraServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "cfn-test-conn",
			"ConnectionType": "JDBC",
		},
	}

	physID, err := rc.Create(t.Context(), "MyConn", "AWS::Glue::Connection", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "cfn-test-conn", physID)

	conn, err := backends.Glue.Backend.GetConnection("cfn-test-conn")
	require.NoError(t, err)
	assert.Equal(t, "JDBC", conn.ConnectionType)

	err = rc.Delete(t.Context(), "AWS::Glue::Connection", physID, nil)
	require.NoError(t, err)
}

// TestResourceCreator_GluePartition_MissingProps ensures creating a Glue Partition without
// required DatabaseName/TableName returns an error instead of a stub physical ID.
func TestResourceCreator_GluePartition_MissingProps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props map[string]any
		name  string
	}{
		{
			name:  "missing_both",
			props: map[string]any{},
		},
		{
			name:  "missing_table",
			props: map[string]any{"DatabaseName": "mydb"},
		},
		{
			name:  "missing_database",
			props: map[string]any{"TableName": "mytable"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtraServiceBackends(t)
			rc := cloudformation.NewResourceCreator(backends)

			_, err := rc.Create(t.Context(), "MyPartition", "AWS::Glue::Partition", tt.props, nil, nil)
			require.Error(t, err)
		})
	}
}
