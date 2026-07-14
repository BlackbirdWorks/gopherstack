package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- ModifyEndpoint ----

func TestBackendCoverage_ModifyEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddEndpointInternal("mod-ep", "source", "mysql")

	// Describe to get ARN.
	descRec := doDMS(t, h, "DescribeEndpoints", map[string]any{
		"Filters": []map[string]any{{"Name": "endpoint-id", "Values": []string{"mod-ep"}}},
	})
	require.Equal(t, http.StatusOK, descRec.Code)
	eps := parseJSON(t, descRec)["Endpoints"].([]any)
	require.Len(t, eps, 1)
	epArn := eps[0].(map[string]any)["EndpointArn"].(string)

	rec := doDMS(t, h, "ModifyEndpoint", map[string]any{
		"EndpointArn": epArn,
		"ServerName":  "new-server",
		"Port":        float64(5432),
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Not found case.
	rec2 := doDMS(t, h, "ModifyEndpoint", map[string]any{
		"EndpointArn": "arn:nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---- ModifyReplicationInstance ----

func TestBackendCoverage_ModifyReplicationInstance(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("mod-ri", "dms.t3.medium")

	descRec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	ris := parseJSON(t, descRec)["ReplicationInstances"].([]any)
	require.Len(t, ris, 1)
	riArn := ris[0].(map[string]any)["ReplicationInstanceArn"].(string)

	rec := doDMS(t, h, "ModifyReplicationInstance", map[string]any{
		"ReplicationInstanceArn":   riArn,
		"ReplicationInstanceClass": "dms.r5.large",
		"EngineVersion":            "3.5.1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "ModifyReplicationInstance", map[string]any{
		"ReplicationInstanceArn": "arn:nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---- ModifyReplicationTask ----

func TestBackendCoverage_ModifyReplicationTask(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("mt-ri", "dms.t3.medium")
	h.Backend.AddEndpointInternal("mt-src", "source", "mysql")
	h.Backend.AddEndpointInternal("mt-tgt", "target", "postgres")
	h.Backend.AddReplicationTaskInternal("mt-task", "mt-src", "mt-tgt", "mt-ri", "full-load")

	descRec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	tasks := parseJSON(t, descRec)["ReplicationTasks"].([]any)
	require.Len(t, tasks, 1)
	taskArn := tasks[0].(map[string]any)["ReplicationTaskArn"].(string)

	rec := doDMS(t, h, "ModifyReplicationTask", map[string]any{
		"ReplicationTaskArn": taskArn,
		"MigrationType":      "cdc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "ModifyReplicationTask", map[string]any{
		"ReplicationTaskArn": "arn:nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---- DeleteDataMigration ----

func TestBackendCoverage_DeleteDataMigration(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataMigrationInternal("del-dm", "full-load")

	rec := doDMS(t, h, "DeleteDataMigration", map[string]any{
		"DataMigrationIdentifier": "del-dm",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.DataMigrationCount())

	rec2 := doDMS(t, h, "DeleteDataMigration", map[string]any{
		"DataMigrationIdentifier": "del-dm",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---- DeleteDataProvider ----

func TestBackendCoverage_DeleteDataProvider(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataProviderInternal("del-dp", "mysql")

	rec := doDMS(t, h, "DeleteDataProvider", map[string]any{
		"DataProviderArn": "del-dp",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.DataProviderCount())
}

// ---- DeleteEventSubscription ----

func TestBackendCoverage_DeleteEventSubscription(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddEventSubscriptionInternal("del-es", "arn:aws:sns:us-east-1:123:topic")

	rec := doDMS(t, h, "DeleteEventSubscription", map[string]any{
		"SubscriptionName": "del-es",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.EventSubscriptionCount())

	rec2 := doDMS(t, h, "DeleteEventSubscription", map[string]any{
		"SubscriptionName": "del-es",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---- DeleteFleetAdvisorCollector ----

func TestBackendCoverage_DeleteFleetAdvisorCollector(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddFleetAdvisorCollectorInternal("del-fac")

	rec := doDMS(t, h, "DeleteFleetAdvisorCollector", map[string]any{
		"CollectorReferencedId": "del-fac",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.FleetAdvisorCollectorCount())
}

// ---- DeleteInstanceProfile ----

func TestBackendCoverage_DeleteInstanceProfile(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddInstanceProfileInternal("del-ip")

	rec := doDMS(t, h, "DeleteInstanceProfile", map[string]any{
		"InstanceProfileArn": "del-ip",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.InstanceProfileCount())

	rec2 := doDMS(t, h, "DeleteInstanceProfile", map[string]any{
		"InstanceProfileArn": "del-ip",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---- RemoveTagsFromResource ----

func TestBackendCoverage_RemoveTagsFromResource(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	createRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "ri-for-tags",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	riArn := parseJSON(t, createRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
		"ResourceArn": riArn,
		"Tags": []map[string]any{
			{"Key": "k1", "Value": "v1"},
			{"Key": "k2", "Value": "v2"},
		},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	rec := doDMS(t, h, "RemoveTagsFromResource", map[string]any{
		"ResourceArn": riArn,
		"TagKeys":     []string{"k1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	listRec := doDMS(t, h, "ListTagsForResource", map[string]any{"ResourceArn": riArn})
	require.Equal(t, http.StatusOK, listRec.Code)
	tagList := parseJSON(t, listRec)["TagList"].([]any)
	assert.Len(t, tagList, 1)
	assert.Equal(t, "k2", tagList[0].(map[string]any)["Key"])
}

// ---- ModifyDataMigration ----

func TestBackendCoverage_ModifyDataMigration(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataMigrationInternal("mod-dm", "full-load")

	rec := doDMS(t, h, "ModifyDataMigration", map[string]any{
		"DataMigrationIdentifier": "mod-dm",
		"DataMigrationType":       "cdc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "ModifyDataMigration", map[string]any{
		"DataMigrationIdentifier": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---- ModifyDataProvider ----

func TestBackendCoverage_ModifyDataProvider(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataProviderInternal("mod-dp", "mysql")

	rec := doDMS(t, h, "ModifyDataProvider", map[string]any{
		"DataProviderArn": "mod-dp",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "ModifyDataProvider", map[string]any{
		"DataProviderArn": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---- ModifyEventSubscription ----

func TestBackendCoverage_ModifyEventSubscription(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddEventSubscriptionInternal("mod-es", "arn:aws:sns:us-east-1:123:topic")

	enabled := true
	rec := doDMS(t, h, "ModifyEventSubscription", map[string]any{
		"SubscriptionName": "mod-es",
		"Enabled":          enabled,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "ModifyEventSubscription", map[string]any{
		"SubscriptionName": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---- ModifyInstanceProfile ----

func TestBackendCoverage_ModifyInstanceProfile(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddInstanceProfileInternal("mod-ip")

	descRec := doDMS(t, h, "DescribeInstanceProfiles", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	profiles := parseJSON(t, descRec)["InstanceProfiles"].([]any)
	require.Len(t, profiles, 1)
	ipArn := profiles[0].(map[string]any)["InstanceProfileArn"].(string)

	rec := doDMS(t, h, "ModifyInstanceProfile", map[string]any{
		"InstanceProfileArn": ipArn,
		"Description":        "updated description",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "ModifyInstanceProfile", map[string]any{
		"InstanceProfileArn": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---- StartDataMigration / StopDataMigration ----

func TestBackendCoverage_StartStopDataMigration(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataMigrationInternal("start-stop-dm", "full-load")

	startRec := doDMS(t, h, "StartDataMigration", map[string]any{
		"DataMigrationIdentifier": "start-stop-dm",
	})
	assert.Equal(t, http.StatusOK, startRec.Code)

	stopRec := doDMS(t, h, "StopDataMigration", map[string]any{
		"DataMigrationIdentifier": "start-stop-dm",
	})
	assert.Equal(t, http.StatusOK, stopRec.Code)

	// Not found cases.
	rec := doDMS(t, h, "StartDataMigration", map[string]any{
		"DataMigrationIdentifier": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec2 := doDMS(t, h, "StopDataMigration", map[string]any{
		"DataMigrationIdentifier": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---- RebootReplicationInstance ----

func TestBackendCoverage_RebootReplicationInstance(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("reboot-ri", "dms.t3.medium")

	descRec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	ris := parseJSON(t, descRec)["ReplicationInstances"].([]any)
	require.Len(t, ris, 1)
	riArn := ris[0].(map[string]any)["ReplicationInstanceArn"].(string)

	rec := doDMS(t, h, "RebootReplicationInstance", map[string]any{
		"ReplicationInstanceArn": riArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "RebootReplicationInstance", map[string]any{
		"ReplicationInstanceArn": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---- MoveReplicationTask ----

func TestBackendCoverage_MoveReplicationTask(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("move-ri-src", "dms.t3.medium")
	h.Backend.AddReplicationInstanceInternal("move-ri-tgt", "dms.t3.medium")
	h.Backend.AddEndpointInternal("move-src", "source", "mysql")
	h.Backend.AddEndpointInternal("move-tgt", "target", "postgres")
	h.Backend.AddReplicationTaskInternal("move-task", "move-src", "move-tgt", "move-ri-src", "full-load")

	descRec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	tasks := parseJSON(t, descRec)["ReplicationTasks"].([]any)
	require.Len(t, tasks, 1)
	taskArn := tasks[0].(map[string]any)["ReplicationTaskArn"].(string)

	rec := doDMS(t, h, "MoveReplicationTask", map[string]any{
		"ReplicationTaskArn":           taskArn,
		"TargetReplicationInstanceArn": "arn:aws:dms:us-east-1:123:rep:move-ri-tgt",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "MoveReplicationTask", map[string]any{
		"ReplicationTaskArn":           "nonexistent",
		"TargetReplicationInstanceArn": "arn:aws:dms:us-east-1:123:rep:move-ri-tgt",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---- TestConnection ----

func TestBackendCoverage_TestConnection(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// Create a replication instance and endpoint via handler to get ARNs.
	riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "tc-ri",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, riRec.Code)
	riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	epRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "tc-ep",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, epRec.Code)
	epArn := parseJSON(t, epRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	rec := doDMS(t, h, "TestConnection", map[string]any{
		"ReplicationInstanceArn": riArn,
		"EndpointArn":            epArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- ImportCertificate / DeleteCertificate ----

func TestBackendCoverage_ImportDeleteCertificate(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	importRec := doDMS(t, h, "ImportCertificate", map[string]any{
		"CertificateIdentifier": "my-cert",
		"CertificatePem":        "-----BEGIN CERTIFICATE-----",
	})
	require.Equal(t, http.StatusOK, importRec.Code)
	certArn := parseJSON(t, importRec)["Certificate"].(map[string]any)["CertificateArn"].(string)

	// Duplicate should fail.
	dupRec := doDMS(t, h, "ImportCertificate", map[string]any{
		"CertificateIdentifier": "my-cert",
		"CertificatePem":        "pem2",
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	// Missing identifier.
	missingRec := doDMS(t, h, "ImportCertificate", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, missingRec.Code)

	// Describe.
	descRec := doDMS(t, h, "DescribeCertificates", map[string]any{})
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Delete by ARN.
	delRec := doDMS(t, h, "DeleteCertificate", map[string]any{
		"CertificateArn": certArn,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	// Not found.
	delRec2 := doDMS(t, h, "DeleteCertificate", map[string]any{
		"CertificateArn": certArn,
	})
	assert.Equal(t, http.StatusNotFound, delRec2.Code)
}

// ---- CreateMigrationProject / DeleteMigrationProject ----

func TestBackendCoverage_MigrationProject(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	createRec := doDMS(t, h, "CreateMigrationProject", map[string]any{
		"MigrationProjectName": "proj-1",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	projArn := parseJSON(t, createRec)["MigrationProject"].(map[string]any)["MigrationProjectArn"].(string)

	// Duplicate.
	dupRec := doDMS(t, h, "CreateMigrationProject", map[string]any{
		"MigrationProjectName": "proj-1",
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	// Describe.
	descRec := doDMS(t, h, "DescribeMigrationProjects", map[string]any{})
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Modify by ARN.
	modRec := doDMS(t, h, "ModifyMigrationProject", map[string]any{
		"MigrationProjectArn": projArn,
		"Description":         "updated",
	})
	assert.Equal(t, http.StatusOK, modRec.Code)

	// Modify not found.
	notFoundRec := doDMS(t, h, "ModifyMigrationProject", map[string]any{
		"MigrationProjectArn": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, notFoundRec.Code)

	// Delete.
	delRec := doDMS(t, h, "DeleteMigrationProject", map[string]any{
		"MigrationProjectArn": projArn,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	delRec2 := doDMS(t, h, "DeleteMigrationProject", map[string]any{
		"MigrationProjectArn": projArn,
	})
	assert.Equal(t, http.StatusNotFound, delRec2.Code)
}

// ---- CreateReplicationSubnetGroup / DeleteReplicationSubnetGroup ----

func TestBackendCoverage_ReplicationSubnetGroup(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	createRec := doDMS(t, h, "CreateReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier":  "subgrp-1",
		"ReplicationSubnetGroupDescription": "test group",
		"SubnetIds":                         []string{"subnet-1", "subnet-2"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	sgArn := parseJSON(t, createRec)["ReplicationSubnetGroup"].(map[string]any)["ReplicationSubnetGroupArn"].(string)

	// Duplicate.
	dupRec := doDMS(t, h, "CreateReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier":  "subgrp-1",
		"ReplicationSubnetGroupDescription": "test group",
		"SubnetIds":                         []string{"subnet-1", "subnet-2"},
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	// Missing identifier.
	missingRec := doDMS(t, h, "CreateReplicationSubnetGroup", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, missingRec.Code)

	// Missing SubnetIds.
	missingSubnetsRec := doDMS(t, h, "CreateReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier":  "subgrp-2",
		"ReplicationSubnetGroupDescription": "test group",
	})
	assert.Equal(t, http.StatusBadRequest, missingSubnetsRec.Code)

	// Describe.
	descRec := doDMS(t, h, "DescribeReplicationSubnetGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Modify: the description is updated and echoed back on the response.
	modRec := doDMS(t, h, "ModifyReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier":  "subgrp-1",
		"ReplicationSubnetGroupDescription": "updated",
		"SubnetIds":                         []string{"subnet-1"},
	})
	require.Equal(t, http.StatusOK, modRec.Code)
	assert.Equal(t,
		"updated",
		parseJSON(t, modRec)["ReplicationSubnetGroup"].(map[string]any)["ReplicationSubnetGroupDescription"],
	)

	// The description change must be real backend state, not just echoed in
	// the Modify response: a fresh Describe must also see "updated".
	postModifyDescRec := doDMS(t, h, "DescribeReplicationSubnetGroups", map[string]any{})
	require.Equal(t, http.StatusOK, postModifyDescRec.Code)
	postModifyGroups := parseJSON(t, postModifyDescRec)["ReplicationSubnetGroups"].([]any)
	found := false
	for _, g := range postModifyGroups {
		grp, ok := g.(map[string]any)
		if ok && grp["ReplicationSubnetGroupIdentifier"] == "subgrp-1" {
			found = true
			assert.Equal(t, "updated", grp["ReplicationSubnetGroupDescription"])
		}
	}
	assert.True(t, found, "expected subgrp-1 in DescribeReplicationSubnetGroups")

	// Modify missing SubnetIds.
	modMissingSubnetsRec := doDMS(t, h, "ModifyReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier": "subgrp-1",
	})
	assert.Equal(t, http.StatusBadRequest, modMissingSubnetsRec.Code)

	// Modify not found.
	notFoundRec := doDMS(t, h, "ModifyReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier": "nonexistent",
		"SubnetIds":                        []string{"subnet-1"},
	})
	assert.Equal(t, http.StatusNotFound, notFoundRec.Code)

	// Delete by ARN.
	delRec := doDMS(t, h, "DeleteReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier": sgArn,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	delRec2 := doDMS(t, h, "DeleteReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier": sgArn,
	})
	assert.Equal(t, http.StatusNotFound, delRec2.Code)
}

// ---- CreateReplicationConfig / DeleteReplicationConfig ----

func TestBackendCoverage_ReplicationConfig(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	createRec := doDMS(t, h, "CreateReplicationConfig", map[string]any{
		"ReplicationConfigIdentifier": "rc-1",
		"ReplicationType":             "full-load",
		"SourceEndpointArn":           "arn:src",
		"TargetEndpointArn":           "arn:tgt",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	rcArn := parseJSON(t, createRec)["ReplicationConfig"].(map[string]any)["ReplicationConfigArn"].(string)

	// Duplicate.
	dupRec := doDMS(t, h, "CreateReplicationConfig", map[string]any{
		"ReplicationConfigIdentifier": "rc-1",
		"ReplicationType":             "cdc",
		"SourceEndpointArn":           "arn:src",
		"TargetEndpointArn":           "arn:tgt",
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	// Describe.
	descRec := doDMS(t, h, "DescribeReplicationConfigs", map[string]any{})
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Modify by ARN.
	modRec := doDMS(t, h, "ModifyReplicationConfig", map[string]any{
		"ReplicationConfigArn": rcArn,
		"ReplicationType":      "cdc",
	})
	assert.Equal(t, http.StatusOK, modRec.Code)

	// Modify not found.
	notFoundRec := doDMS(t, h, "ModifyReplicationConfig", map[string]any{
		"ReplicationConfigArn": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, notFoundRec.Code)

	// Delete by ARN.
	delRec := doDMS(t, h, "DeleteReplicationConfig", map[string]any{
		"ReplicationConfigArn": rcArn,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	delRec2 := doDMS(t, h, "DeleteReplicationConfig", map[string]any{
		"ReplicationConfigArn": rcArn,
	})
	assert.Equal(t, http.StatusNotFound, delRec2.Code)
}

// ---- Describe* operations ----

func TestBackendCoverage_DescribeOps(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataMigrationInternal("desc-dm", "full-load")
	h.Backend.AddDataProviderInternal("desc-dp", "mysql")
	h.Backend.AddEventSubscriptionInternal("desc-es", "arn:aws:sns:us-east-1:123:topic")
	h.Backend.AddFleetAdvisorCollectorInternal("desc-fac")
	h.Backend.AddInstanceProfileInternal("desc-ip")

	for _, action := range []string{
		"DescribeDataMigrations",
		"DescribeDataProviders",
		"DescribeEventSubscriptions",
		"DescribeFleetAdvisorCollectors",
		"DescribeInstanceProfiles",
	} {
		rec := doDMS(t, h, action, map[string]any{})
		assert.Equal(t, http.StatusOK, rec.Code, "action=%s", action)
	}
}

// ---- Pass-through handlers (no backend state) ----

func TestBackendCoverage_PassThroughHandlers(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	type passCase struct {
		body   map[string]any
		action string
	}

	mpx := map[string]any{"MigrationProjectIdentifier": "x"}

	cases := []passCase{
		{body: map[string]any{"EndpointArn": "ep-arn", "ReplicationInstanceArn": "ri-arn"}, action: "DeleteConnection"},
		{body: map[string]any{}, action: "DescribeAccountAttributes"},
		{body: map[string]any{}, action: "DescribeApplicableIndividualAssessments"},
		{body: map[string]any{}, action: "DescribeConnections"},
		{body: map[string]any{}, action: "DescribeConversionConfiguration"},
		{body: map[string]any{"EngineName": "mysql"}, action: "DescribeEndpointSettings"},
		{body: map[string]any{}, action: "DescribeEndpointTypes"},
		{body: map[string]any{}, action: "DescribeEngineVersions"},
		{body: map[string]any{}, action: "DescribeEventCategories"},
		{body: map[string]any{}, action: "DescribeEvents"},
		{body: mpx, action: "DescribeExtensionPackAssociations"},
		{body: map[string]any{}, action: "DescribeFleetAdvisorDatabases"},
		{body: map[string]any{}, action: "DescribeFleetAdvisorLsaAnalysis"},
		{body: map[string]any{}, action: "DescribeFleetAdvisorSchemaObjectSummary"},
		{body: map[string]any{}, action: "DescribeFleetAdvisorSchemas"},
		{body: mpx, action: "DescribeMetadataModel"},
		{body: mpx, action: "DescribeMetadataModelAssessments"},
		{body: mpx, action: "DescribeMetadataModelChildren"},
		{body: mpx, action: "DescribeMetadataModelConversions"},
		{body: mpx, action: "DescribeMetadataModelCreations"},
		{body: mpx, action: "DescribeMetadataModelExportsAsScript"},
		{body: mpx, action: "DescribeMetadataModelExportsToTarget"},
		{body: mpx, action: "DescribeMetadataModelImports"},
		{body: map[string]any{}, action: "DescribeOrderableReplicationInstances"},
		{body: map[string]any{}, action: "DescribePendingMaintenanceActions"},
		{body: map[string]any{}, action: "DescribeRecommendationLimitations"},
		{body: map[string]any{}, action: "DescribeRecommendations"},
		{body: map[string]any{}, action: "DescribeRefreshSchemasStatus"},
		{body: map[string]any{"ReplicationInstanceArn": "ri-arn"}, action: "DescribeReplicationInstanceTaskLogs"},
		{body: map[string]any{"ReplicationTaskArn": "rt-arn"}, action: "DescribeReplicationTableStatistics"},
		{body: map[string]any{}, action: "DescribeReplicationTaskAssessmentResults"},
		{body: map[string]any{}, action: "DescribeReplicationTaskAssessmentRuns"},
		{body: map[string]any{}, action: "DescribeReplicationTaskIndividualAssessments"},
		{body: map[string]any{}, action: "DescribeReplications"},
		{body: map[string]any{}, action: "DescribeSchemas"},
		{body: map[string]any{"ReplicationTaskArn": "rt-arn"}, action: "DescribeTableStatistics"},
		{body: mpx, action: "ExportMetadataModelAssessment"},
		{body: map[string]any{}, action: "GetTargetSelectionRules"},
		{body: mpx, action: "ModifyConversionConfiguration"},
		{body: map[string]any{}, action: "RefreshSchemas"},
		{body: map[string]any{"ReplicationTaskArn": "rt-arn"}, action: "ReloadReplicationTables"},
		{body: map[string]any{"ReplicationTaskArn": "rt-arn"}, action: "ReloadTables"},
		{body: map[string]any{}, action: "RunFleetAdvisorLsaAnalysis"},
		{body: mpx, action: "StartExtensionPackAssociation"},
		{body: mpx, action: "StartMetadataModelAssessment"},
		{body: mpx, action: "StartMetadataModelConversion"},
		{body: mpx, action: "StartMetadataModelCreation"},
		{body: mpx, action: "StartMetadataModelExportAsScript"},
		{body: mpx, action: "StartMetadataModelExportToTarget"},
		{body: mpx, action: "StartMetadataModelImport"},
		{body: map[string]any{}, action: "StartRecommendations"},
		{
			body:   map[string]any{"ReplicationConfigArn": "rc-arn", "StartReplicationType": "full-load"},
			action: "StartReplication",
		},
		{body: map[string]any{"ReplicationTaskArn": "rt-arn"}, action: "StartReplicationTaskAssessment"},
		{
			body:   map[string]any{"ReplicationTaskArn": "rt-arn", "ServiceAccessRoleArn": "arn"},
			action: "StartReplicationTaskAssessmentRun",
		},
		{body: map[string]any{"ReplicationConfigArn": "rc-arn"}, action: "StopReplication"},
		{body: map[string]any{}, action: "UpdateSubscriptionsToEventBridge"},
		{
			body:   map[string]any{"ReplicationTaskAssessmentRunArn": "arn"},
			action: "DeleteReplicationTaskAssessmentRun",
		},
		{body: map[string]any{}, action: "DeleteFleetAdvisorDatabases"},
	}

	for _, tc := range cases {
		rec := doDMS(t, h, tc.action, tc.body)
		// Just make sure the handler returns a response (not a panic or unrouted).
		assert.NotEqual(t, http.StatusInternalServerError, rec.Code, "action=%s", tc.action)
	}
}
