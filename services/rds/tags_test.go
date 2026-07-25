package rds_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRDSHandler_AddTagsToResource(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	arn := "arn:aws:rds:us-east-1:000000000000:db:test-db"

	// Add two tags (SDK encodes as Tags.Tag.N.Key).
	rec := postRDSForm(t, h,
		"Action=AddTagsToResource&Version=2014-10-31"+
			"&ResourceName="+arn+
			"&Tags.Tag.1.Key=Env&Tags.Tag.1.Value=prod"+
			"&Tags.Tag.2.Key=Team&Tags.Tag.2.Value=platform")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AddTagsToResourceResponse")

	// List should now return both tags.
	rec2 := postRDSForm(t, h,
		"Action=ListTagsForResource&Version=2014-10-31&ResourceName="+arn)
	assert.Equal(t, http.StatusOK, rec2.Code)

	body := rec2.Body.String()
	assert.Contains(t, body, "<Key>Env</Key>")
	assert.Contains(t, body, "<Value>prod</Value>")
	assert.Contains(t, body, "<Key>Team</Key>")
	assert.Contains(t, body, "<Value>platform</Value>")
}

func TestRDSHandler_RemoveTagsFromResource(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	arn := "arn:aws:rds:us-east-1:000000000000:db:rm-db"

	// Add two tags.
	postRDSForm(t, h,
		"Action=AddTagsToResource&Version=2014-10-31"+
			"&ResourceName="+arn+
			"&Tags.Tag.1.Key=Env&Tags.Tag.1.Value=test"+
			"&Tags.Tag.2.Key=Team&Tags.Tag.2.Value=infra")

	// Remove one (SDK encodes as TagKeys.member.N).
	rec := postRDSForm(t, h,
		"Action=RemoveTagsFromResource&Version=2014-10-31"+
			"&ResourceName="+arn+
			"&TagKeys.member.1=Env")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RemoveTagsFromResourceResponse")

	// List should only have Team left.
	rec2 := postRDSForm(t, h,
		"Action=ListTagsForResource&Version=2014-10-31&ResourceName="+arn)

	body := rec2.Body.String()
	assert.NotContains(t, body, "<Key>Env</Key>")
	assert.Contains(t, body, "<Key>Team</Key>")
}

func TestRDSBackend_TagsCleanedUpOnDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, h *rds.Handler)
		del   string
		check string
	}{
		{
			name: "instance",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h,
					"Action=CreateDBInstance&Version=2014-10-31"+
						"&DBInstanceIdentifier=tag-inst&Engine=postgres")
				postRDSForm(t, h, "Action=AddTagsToResource&Version=2014-10-31"+
					"&ResourceName=arn:aws:rds:us-east-1:000000000000:db:tag-inst"+
					"&Tags.Tag.1.Key=k&Tags.Tag.1.Value=v")
			},
			del: "Action=DeleteDBInstance&Version=2014-10-31" +
				"&DBInstanceIdentifier=tag-inst&SkipFinalSnapshot=true",
			check: "Action=ListTagsForResource&Version=2014-10-31" +
				"&ResourceName=arn:aws:rds:us-east-1:000000000000:db:tag-inst",
		},
		{
			name: "snapshot",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h,
					"Action=CreateDBInstance&Version=2014-10-31"+
						"&DBInstanceIdentifier=snap-inst&Engine=postgres")
				postRDSForm(t, h,
					"Action=CreateDBSnapshot&Version=2014-10-31"+
						"&DBSnapshotIdentifier=tag-snap&DBInstanceIdentifier=snap-inst")
				postRDSForm(t, h, "Action=AddTagsToResource&Version=2014-10-31"+
					"&ResourceName=arn:aws:rds:us-east-1:000000000000:snapshot:tag-snap"+
					"&Tags.Tag.1.Key=k&Tags.Tag.1.Value=v")
			},
			del: "Action=DeleteDBSnapshot&Version=2014-10-31" +
				"&DBSnapshotIdentifier=tag-snap",
			check: "Action=ListTagsForResource&Version=2014-10-31" +
				"&ResourceName=arn:aws:rds:us-east-1:000000000000:snapshot:tag-snap",
		},
		{
			name: "subnet_group",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h,
					"Action=CreateDBSubnetGroup&Version=2014-10-31"+
						"&DBSubnetGroupName=tag-sg&DBSubnetGroupDescription=d&VpcId=vpc-1")
				postRDSForm(t, h, "Action=AddTagsToResource&Version=2014-10-31"+
					"&ResourceName=arn:aws:rds:us-east-1:000000000000:subgrp:tag-sg"+
					"&Tags.Tag.1.Key=k&Tags.Tag.1.Value=v")
			},
			del: "Action=DeleteDBSubnetGroup&Version=2014-10-31" +
				"&DBSubnetGroupName=tag-sg",
			check: "Action=ListTagsForResource&Version=2014-10-31" +
				"&ResourceName=arn:aws:rds:us-east-1:000000000000:subgrp:tag-sg",
		},
		{
			name: "parameter_group",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h,
					"Action=CreateDBParameterGroup&Version=2014-10-31"+
						"&DBParameterGroupName=tag-pg&DBParameterGroupFamily=postgres14"+
						"&Description=d")
				postRDSForm(t, h, "Action=AddTagsToResource&Version=2014-10-31"+
					"&ResourceName=arn:aws:rds:us-east-1:000000000000:pg:tag-pg"+
					"&Tags.Tag.1.Key=k&Tags.Tag.1.Value=v")
			},
			del: "Action=DeleteDBParameterGroup&Version=2014-10-31" +
				"&DBParameterGroupName=tag-pg",
			check: "Action=ListTagsForResource&Version=2014-10-31" +
				"&ResourceName=arn:aws:rds:us-east-1:000000000000:pg:tag-pg",
		},
		{
			name: "option_group",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h,
					"Action=CreateOptionGroup&Version=2014-10-31"+
						"&OptionGroupName=tag-og&EngineName=mysql"+
						"&MajorEngineVersion=8.0&OptionGroupDescription=d")
				postRDSForm(t, h, "Action=AddTagsToResource&Version=2014-10-31"+
					"&ResourceName=arn:aws:rds:us-east-1:000000000000:og:tag-og"+
					"&Tags.Tag.1.Key=k&Tags.Tag.1.Value=v")
			},
			del: "Action=DeleteOptionGroup&Version=2014-10-31&OptionGroupName=tag-og",
			check: "Action=ListTagsForResource&Version=2014-10-31" +
				"&ResourceName=arn:aws:rds:us-east-1:000000000000:og:tag-og",
		},
		{
			name: "cluster",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h,
					"Action=CreateDBCluster&Version=2014-10-31"+
						"&DBClusterIdentifier=tag-cluster&Engine=aurora-postgresql"+
						"&MasterUsername=admin&MasterUserPassword=pass")
				postRDSForm(t, h, "Action=AddTagsToResource&Version=2014-10-31"+
					"&ResourceName=arn:aws:rds:us-east-1:000000000000:cluster:tag-cluster"+
					"&Tags.Tag.1.Key=k&Tags.Tag.1.Value=v")
			},
			del: "Action=DeleteDBCluster&Version=2014-10-31" +
				"&DBClusterIdentifier=tag-cluster&SkipFinalSnapshot=true",
			check: "Action=ListTagsForResource&Version=2014-10-31" +
				"&ResourceName=arn:aws:rds:us-east-1:000000000000:cluster:tag-cluster",
		},
		{
			name: "cluster_snapshot",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h,
					"Action=CreateDBCluster&Version=2014-10-31"+
						"&DBClusterIdentifier=csnap-cluster&Engine=aurora-postgresql"+
						"&MasterUsername=admin&MasterUserPassword=pass")
				postRDSForm(t, h,
					"Action=CreateDBClusterSnapshot&Version=2014-10-31"+
						"&DBClusterSnapshotIdentifier=tag-csnap"+
						"&DBClusterIdentifier=csnap-cluster")
				postRDSForm(t, h, "Action=AddTagsToResource&Version=2014-10-31"+
					"&ResourceName=arn:aws:rds:us-east-1:000000000000:cluster-snapshot:tag-csnap"+
					"&Tags.Tag.1.Key=k&Tags.Tag.1.Value=v")
			},
			del: "Action=DeleteDBClusterSnapshot&Version=2014-10-31" +
				"&DBClusterSnapshotIdentifier=tag-csnap",
			check: "Action=ListTagsForResource&Version=2014-10-31" +
				"&ResourceName=arn:aws:rds:us-east-1:000000000000:cluster-snapshot:tag-csnap",
		},
		{
			name: "cluster_endpoint",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h,
					"Action=CreateDBCluster&Version=2014-10-31"+
						"&DBClusterIdentifier=ep-cluster&Engine=aurora-postgresql"+
						"&MasterUsername=admin&MasterUserPassword=pass")
				postRDSForm(t, h,
					"Action=CreateDBClusterEndpoint&Version=2014-10-31"+
						"&DBClusterEndpointIdentifier=tag-ep"+
						"&DBClusterIdentifier=ep-cluster&EndpointType=READER")
				postRDSForm(t, h, "Action=AddTagsToResource&Version=2014-10-31"+
					"&ResourceName=arn:aws:rds:us-east-1:000000000000:cluster-endpoint:tag-ep"+
					"&Tags.Tag.1.Key=k&Tags.Tag.1.Value=v")
			},
			del: "Action=DeleteDBClusterEndpoint&Version=2014-10-31" +
				"&DBClusterEndpointIdentifier=tag-ep",
			check: "Action=ListTagsForResource&Version=2014-10-31" +
				"&ResourceName=arn:aws:rds:us-east-1:000000000000:cluster-endpoint:tag-ep",
		},
		{
			// Regression test for a ghost-row leak: deleting a cluster must
			// cascade-delete its custom cluster endpoints (and their tags)
			// too, not just deleting an endpoint directly (the case above).
			// Before this fix, DeleteDBCluster left the cluster's endpoints
			// (and their tags) behind forever in the backend's maps.
			name: "cluster_endpoint_cascade_via_cluster_delete",
			setup: func(t *testing.T, h *rds.Handler) {
				t.Helper()
				postRDSForm(t, h,
					"Action=CreateDBCluster&Version=2014-10-31"+
						"&DBClusterIdentifier=cascade-ep-cluster&Engine=aurora-postgresql"+
						"&MasterUsername=admin&MasterUserPassword=pass")
				postRDSForm(t, h,
					"Action=CreateDBClusterEndpoint&Version=2014-10-31"+
						"&DBClusterEndpointIdentifier=cascade-ep"+
						"&DBClusterIdentifier=cascade-ep-cluster&EndpointType=READER")
				postRDSForm(t, h, "Action=AddTagsToResource&Version=2014-10-31"+
					"&ResourceName=arn:aws:rds:us-east-1:000000000000:cluster-endpoint:cascade-ep"+
					"&Tags.Tag.1.Key=k&Tags.Tag.1.Value=v")
			},
			del: "Action=DeleteDBCluster&Version=2014-10-31" +
				"&DBClusterIdentifier=cascade-ep-cluster&SkipFinalSnapshot=true",
			check: "Action=ListTagsForResource&Version=2014-10-31" +
				"&ResourceName=arn:aws:rds:us-east-1:000000000000:cluster-endpoint:cascade-ep",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()
			tt.setup(t, h)

			// Confirm tag was stored.
			rec := postRDSForm(t, h, tt.check)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "<Key>k</Key>")

			// Delete the resource.
			delRec := postRDSForm(t, h, tt.del)
			assert.Equal(t, http.StatusOK, delRec.Code)

			// Confirm tags are gone.
			rec2 := postRDSForm(t, h, tt.check)
			assert.Equal(t, http.StatusOK, rec2.Code)
			assert.NotContains(t, rec2.Body.String(), "<Key>k</Key>")
		})
	}
}

// TestRDSBackend_RemoveTagsNoAliasing ensures RemoveTagsFromResource doesn't corrupt the underlying slice.
func TestRDSBackend_RemoveTagsNoAliasing(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBInstance("db", "postgres", "", "", "", "", 0, rds.DBInstanceOptions{})
	require.NoError(t, err)

	arn := "arn:aws:rds:us-east-1:000000000000:db:db"

	b.AddTagsToResource(arn, []rds.Tag{
		{Key: "env", Value: "prod"},
		{Key: "team", Value: "sre"},
		{Key: "cost-center", Value: "eng"},
	})

	b.RemoveTagsFromResource(arn, []string{"team"})

	tags := b.ListTagsForResource(arn)
	assert.Len(t, tags, 2)

	keys := make([]string, 0, len(tags))
	for _, tag := range tags {
		keys = append(keys, tag.Key)
	}
	assert.Contains(t, keys, "env")
	assert.Contains(t, keys, "cost-center")
	assert.NotContains(t, keys, "team")

	// Second removal should work without corrupting data.
	b.RemoveTagsFromResource(arn, []string{"env"})
	tags = b.ListTagsForResource(arn)
	assert.Len(t, tags, 1)
	assert.Equal(t, "cost-center", tags[0].Key)
}
