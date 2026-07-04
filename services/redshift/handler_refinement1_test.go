package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- StorageBackend interface satisfaction ----

func TestStorageBackend_InterfaceSatisfied(t *testing.T) {
	t.Parallel()

	var _ redshift.StorageBackend = redshift.NewInMemoryBackend("000000000000", "us-east-1")
}

// ---- Backend.Reset ----

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateCluster("c1", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	b.AddSnapshotInternal(
		&redshift.Snapshot{SnapshotIdentifier: "snap-1", ClusterIdentifier: "c1", Status: "available"},
	)
	b.AddDataShareInternal(&redshift.DataShare{DataShareArn: "arn:aws:redshift::123:datashare:ds1"})

	assert.Equal(t, 1, redshift.ClusterCount(b))
	assert.Equal(t, 1, redshift.SnapshotCount(b))
	assert.Equal(t, 1, redshift.DataShareCount(b))

	b.Reset()

	assert.Equal(t, 0, redshift.ClusterCount(b))
	assert.Equal(t, 0, redshift.SnapshotCount(b))
	assert.Equal(t, 0, redshift.DataShareCount(b))
}

// ---- Handler.Reset ----

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=reset-c1")
	assert.Equal(t, 1, redshift.ClusterCount(b))

	h.Reset()

	assert.Equal(t, 0, redshift.ClusterCount(b))
}

// ---- ErrNilAppContext ----

func TestProvider_NilAppContextReturnsError(t *testing.T) {
	t.Parallel()

	p := &redshift.Provider{}
	svc, err := p.Init(nil)

	assert.Nil(t, svc)
	require.Error(t, err)
	assert.ErrorIs(t, err, redshift.ErrNilAppContext)
}

// ---- GetSupportedOperations sorted ----

func TestHandler_GetSupportedOperations_Sorted(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i], "ops not sorted at index %d", i)
	}
}

// ---- HandlerOpsLen ----

func TestHandler_OpsLen(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	assert.Equal(t, len(h.GetSupportedOperations()), redshift.HandlerOpsLen(h))
}

// ---- Export count helpers ----

func TestExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")

	assert.Equal(t, 0, redshift.ClusterCount(b))
	assert.Equal(t, 0, redshift.ReservedNodeCount(b))
	assert.Equal(t, 0, redshift.PartnerCount(b))
	assert.Equal(t, 0, redshift.DataShareCount(b))
	assert.Equal(t, 0, redshift.SecurityGroupCount(b))
	assert.Equal(t, 0, redshift.SnapshotCount(b))
	assert.Equal(t, 0, redshift.EndpointAuthCount(b))
	assert.Equal(t, 0, redshift.ActiveResizeCount(b))

	_, err := b.CreateCluster("c1", "dc2.large", "dev", "admin")
	require.NoError(t, err)
	assert.Equal(t, 1, redshift.ClusterCount(b))

	b.AddReservedNodeInternal(&redshift.ReservedNode{ReservedNodeID: "rn-1"})
	assert.Equal(t, 1, redshift.ReservedNodeCount(b))

	b.AddDataShareInternal(&redshift.DataShare{DataShareArn: "ds-arn"})
	assert.Equal(t, 1, redshift.DataShareCount(b))

	b.AddSecurityGroupInternal(&redshift.ClusterSecurityGroup{ClusterSecurityGroupName: "sg-1"})
	assert.Equal(t, 1, redshift.SecurityGroupCount(b))

	b.AddSnapshotInternal(&redshift.Snapshot{SnapshotIdentifier: "snap-1"})
	assert.Equal(t, 1, redshift.SnapshotCount(b))

	b.AddActiveResizeInternal("c1", &redshift.ResizeProgress{Status: "IN_PROGRESS", AllowCancelResize: true})
	assert.Equal(t, 1, redshift.ActiveResizeCount(b))
}

// ---- CreateCluster returns NumberOfNodes and Port ----

func TestCreateCluster_ReturnsExpectedFields(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h,
		"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=fields-cluster&NodeType=dc2.8xlarge")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "CreateClusterResponse")
	assert.Contains(t, body, "fields-cluster")
	assert.Contains(t, body, "dc2.8xlarge")
	// Port 5439 is default
	assert.Contains(t, body, "5439")
}

// ---- DescribeClusters: deep copy check ----

func TestDescribeClusters_DeepCopy(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateCluster("c1", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	clusters, _, err := b.DescribeClusters("", "", 0)
	require.NoError(t, err)
	require.Len(t, clusters, 1)

	// Modifying the returned slice should not affect the backend
	clusters[0].ClusterIdentifier = "mutated"

	clusters2, _, err := b.DescribeClusters("", "", 0)
	require.NoError(t, err)
	assert.Equal(t, "c1", clusters2[0].ClusterIdentifier, "backend should not be mutated by caller")
}

// ---- Error code: InvalidParameterValue ----

func TestRedshiftHandler_ErrorCode_InvalidParameterValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "create_cluster_missing_id",
			body: "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=",
		},
		{
			name: "cancel_resize_missing_id",
			body: "Action=CancelResize&Version=2012-12-01&ClusterIdentifier=",
		},
		{
			name: "accept_reserved_node_exchange_missing_id",
			body: "Action=AcceptReservedNodeExchange&Version=2012-12-01",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "InvalidParameterValue",
				"expected AWS-standard error code, not legacy RedshiftInvalidParameter")
		})
	}
}

// ---- CancelResize: AllowCancelResize=false is rejected ----

func TestCancelResize_AllowCancelResizeFalse(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	_, err := b.CreateCluster("cr-cluster", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	b.AddActiveResizeInternal("cr-cluster", &redshift.ResizeProgress{
		Status:            "IN_PROGRESS",
		AllowCancelResize: false, // not cancellable
	})

	rec := postRedshiftForm(t, h,
		"Action=CancelResize&Version=2012-12-01&ClusterIdentifier=cr-cluster")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidClusterState")
}

// ---- AddPartner response includes ClusterIdentifier ----

func TestAddPartner_ResponseIncludesClusterIdentifier(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ap-cluster")

	rec := postRedshiftForm(t, h,
		"Action=AddPartner&Version=2012-12-01"+
			"&ClusterIdentifier=ap-cluster"+
			"&DatabaseName=mydb"+
			"&PartnerIntegrationId=mypartner")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "AddPartnerResponse")
	assert.Contains(t, body, "ap-cluster")
	assert.Contains(t, body, "mydb")
	assert.Contains(t, body, "mypartner")
}

// ---- AuthorizeEndpointAccess: AllowedAllVPCs when no VPCs specified ----

func TestAuthorizeEndpointAccess_AllowedAllVPCsWhenEmpty(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	_, err := b.CreateCluster("ea-cluster", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	rec := postRedshiftForm(t, h,
		"Action=AuthorizeEndpointAccess&Version=2012-12-01"+
			"&ClusterIdentifier=ea-cluster"+
			"&Account=111111111111")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "AuthorizeEndpointAccessResponse")
	assert.Contains(t, body, "true") // AllowedAllVPCs
}

// ---- AuthorizeEndpointAccess: duplicate returns error ----

func TestAuthorizeEndpointAccess_DuplicateReturnsError(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	_, err := b.CreateCluster("ea-dup-cluster", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	body := "Action=AuthorizeEndpointAccess&Version=2012-12-01" +
		"&ClusterIdentifier=ea-dup-cluster" +
		"&Account=111111111111"

	rec1 := postRedshiftForm(t, h, body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := postRedshiftForm(t, h, body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "EndpointAuthorizationAlreadyExists")
}

// ---- Persistence round-trip ----

func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	b1 := redshift.NewInMemoryBackend("123456789012", "us-west-2")
	_, err := b1.CreateCluster("p-cluster", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	b1.AddReservedNodeInternal(&redshift.ReservedNode{ReservedNodeID: "rn-p1", NodeType: "dc2.large", State: "active"})
	b1.AddDataShareInternal(&redshift.DataShare{DataShareArn: "arn:aws:redshift::123:datashare:ds-p1"})
	b1.AddSecurityGroupInternal(&redshift.ClusterSecurityGroup{ClusterSecurityGroupName: "sg-p1"})
	b1.AddSnapshotInternal(
		&redshift.Snapshot{SnapshotIdentifier: "snap-p1", ClusterIdentifier: "p-cluster", Status: "available"},
	)
	b1.AddActiveResizeInternal("p-cluster", &redshift.ResizeProgress{Status: "IN_PROGRESS", AllowCancelResize: true})

	data := b1.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := redshift.NewInMemoryBackend("", "")
	err = b2.Restore(t.Context(), data)
	require.NoError(t, err)

	assert.Equal(t, "123456789012", b2.AccountID())
	assert.Equal(t, "us-west-2", b2.Region())
	assert.Equal(t, 1, redshift.ClusterCount(b2))
	assert.Equal(t, 1, redshift.ReservedNodeCount(b2))
	assert.Equal(t, 1, redshift.DataShareCount(b2))
	assert.Equal(t, 1, redshift.SecurityGroupCount(b2))
	assert.Equal(t, 1, redshift.SnapshotCount(b2))
	assert.Equal(t, 1, redshift.ActiveResizeCount(b2))
}

// ---- BatchDeleteClusterSnapshots partial success ----

func TestBatchDeleteClusterSnapshots_PartialSuccess(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	b.AddSnapshotInternal(
		&redshift.Snapshot{SnapshotIdentifier: "snap-good", ClusterIdentifier: "c1", Status: "available"},
	)

	rec := postRedshiftForm(t, h,
		"Action=BatchDeleteClusterSnapshots&Version=2012-12-01"+
			"&Identifiers.SnapshotIdentifier.1=snap-good"+
			"&Identifiers.SnapshotIdentifier.2=snap-missing")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "snap-good")
	assert.Contains(t, body, "snap-missing")
	assert.Contains(t, body, "ClusterSnapshotNotFound")

	// snap-good should be deleted
	assert.Equal(t, 0, redshift.SnapshotCount(b))
}

// ---- BatchModifyClusterSnapshots bad retention period ----

func TestBatchModifyClusterSnapshots_InvalidRetentionPeriod(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h,
		"Action=BatchModifyClusterSnapshots&Version=2012-12-01"+
			"&SnapshotIdentifierList.String.1=snap-1"+
			"&ManualSnapshotRetentionPeriod=not-a-number")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

// ---- AuthorizeSnapshotAccess duplicate account ----

func TestAuthorizeSnapshotAccess_DuplicateAccount(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	b.AddSnapshotInternal(
		&redshift.Snapshot{SnapshotIdentifier: "snap-dup", ClusterIdentifier: "c1", Status: "available"},
	)

	body := "Action=AuthorizeSnapshotAccess&Version=2012-12-01" +
		"&SnapshotIdentifier=snap-dup" +
		"&AccountWithRestoreAccess=111111111111"

	rec1 := postRedshiftForm(t, h, body)
	require.Equal(t, http.StatusOK, rec1.Code)
	assert.Contains(t, rec1.Body.String(), "111111111111")

	// Second authorize adds another entry (AWS allows multiple accounts)
	rec2 := postRedshiftForm(t, h, body)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// ---- CancelResize: cluster not found ----

func TestCancelResize_ClusterNotFound(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h,
		"Action=CancelResize&Version=2012-12-01&ClusterIdentifier=nonexistent")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ClusterNotFound")
}

// ---- CancelResize: no active resize ----

func TestCancelResize_NoActiveResize(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	_, err := b.CreateCluster("nr-cluster", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	rec := postRedshiftForm(t, h,
		"Action=CancelResize&Version=2012-12-01&ClusterIdentifier=nr-cluster")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ResizeNotFound")
}

// ---- AssociateDataShareConsumer: missing DataShareArn ----

func TestAssociateDataShareConsumer_MissingArn(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h,
		"Action=AssociateDataShareConsumer&Version=2012-12-01&ConsumerArn=arn:aws:redshift::222:ns/ns1")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

// ---- AuthorizeDataShare: missing ConsumerIdentifier ----

func TestAuthorizeDataShare_MissingConsumerIdentifier(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h,
		"Action=AuthorizeDataShare&Version=2012-12-01&DataShareArn=arn:aws:redshift::123:datashare:ds1")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

// ---- AuthorizeClusterSecurityGroupIngress: missing both CIDRIP and EC2GroupName ----

func TestAuthorizeClusterSecurityGroupIngress_MissingBothParams(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	b.AddSecurityGroupInternal(&redshift.ClusterSecurityGroup{ClusterSecurityGroupName: "sg-test"})

	rec := postRedshiftForm(t, h,
		"Action=AuthorizeClusterSecurityGroupIngress&Version=2012-12-01"+
			"&ClusterSecurityGroupName=sg-test")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

// ---- AuthorizeClusterSecurityGroupIngress: security group not found ----

func TestAuthorizeClusterSecurityGroupIngress_SGNotFound(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h,
		"Action=AuthorizeClusterSecurityGroupIngress&Version=2012-12-01"+
			"&ClusterSecurityGroupName=nonexistent"+
			"&CIDRIP=10.0.0.0/8")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ClusterSecurityGroupNotFound")
}

// ---- DataShare: not found ----

func TestDataShare_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "authorize_data_share",
			body: "Action=AuthorizeDataShare&Version=2012-12-01" +
				"&DataShareArn=arn:nonexistent&ConsumerIdentifier=222222222222",
		},
		{
			name: "associate_data_share_consumer",
			body: "Action=AssociateDataShareConsumer&Version=2012-12-01" +
				"&DataShareArn=arn:nonexistent&ConsumerArn=arn:consumer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "DataShareNotFound")
		})
	}
}

// ---- Backend.Region and AccountID ----

func TestBackend_RegionAndAccountID(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("999888777666", "ap-southeast-1")
	assert.Equal(t, "999888777666", b.AccountID())
	assert.Equal(t, "ap-southeast-1", b.Region())
}

// ---- AuthorizeSnapshotAccess: snapshot not found ----

func TestAuthorizeSnapshotAccess_SnapshotNotFound(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	rec := postRedshiftForm(t, h,
		"Action=AuthorizeSnapshotAccess&Version=2012-12-01"+
			"&SnapshotIdentifier=nonexistent"+
			"&AccountWithRestoreAccess=111111111111")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ClusterSnapshotNotFound")
}

// ---- Resize progress XML fields ----

func TestCancelResize_ReturnsAllXMLFields(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	_, err := b.CreateCluster("rp-cluster", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	b.AddActiveResizeInternal("rp-cluster", &redshift.ResizeProgress{
		Status:                 "IN_PROGRESS",
		TargetNodeType:         "ra3.xlplus",
		TargetClusterType:      "multi-node",
		TargetNumberOfNodes:    4,
		ResizeType:             "ClassicResize",
		AllowCancelResize:      true,
		ImportTablesCompleted:  []string{"table1"},
		ImportTablesInProgress: []string{"table2"},
		ImportTablesNotStarted: []string{"table3", "table4"},
	})

	rec := postRedshiftForm(t, h,
		"Action=CancelResize&Version=2012-12-01&ClusterIdentifier=rp-cluster")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "CancelResizeResponse")
	assert.Contains(t, body, "CANCELLED")
	assert.Contains(t, body, "ra3.xlplus")
	assert.Contains(t, body, "table1")
	assert.Contains(t, body, "table2")
	assert.Contains(t, body, "table3")
}

// ---- AuthorizeClusterSecurityGroupIngress: EC2 security group ----

func TestAuthorizeClusterSecurityGroupIngress_EC2Group(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	b.AddSecurityGroupInternal(&redshift.ClusterSecurityGroup{
		ClusterSecurityGroupName: "ec2-sg-test",
		Description:              "Test security group",
	})

	rec := postRedshiftForm(t, h,
		"Action=AuthorizeClusterSecurityGroupIngress&Version=2012-12-01"+
			"&ClusterSecurityGroupName=ec2-sg-test"+
			"&EC2SecurityGroupName=sg-abc123"+
			"&EC2SecurityGroupOwnerId=999888777666")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "AuthorizeClusterSecurityGroupIngressResponse")
	assert.Contains(t, body, "sg-abc123")
	assert.Contains(t, body, "authorized")
}

// ---- BatchModifyClusterSnapshots success ----

func TestBatchModifyClusterSnapshots_Success(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	b.AddSnapshotInternal(
		&redshift.Snapshot{SnapshotIdentifier: "mod-snap-1", ClusterIdentifier: "c1", Status: "available"},
	)
	b.AddSnapshotInternal(
		&redshift.Snapshot{SnapshotIdentifier: "mod-snap-2", ClusterIdentifier: "c1", Status: "available"},
	)

	rec := postRedshiftForm(t, h,
		"Action=BatchModifyClusterSnapshots&Version=2012-12-01"+
			"&SnapshotIdentifierList.String.1=mod-snap-1"+
			"&SnapshotIdentifierList.String.2=mod-snap-2"+
			"&ManualSnapshotRetentionPeriod=14")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "BatchModifyClusterSnapshotsResponse")
	assert.Contains(t, body, "mod-snap-1")
	assert.Contains(t, body, "mod-snap-2")
}

// ---- parseRedshiftTags bounds ----

func TestCreateTags_MultipleTags(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=mt-cluster")

	rec := postRedshiftForm(t, h,
		"Action=CreateTags&Version=2012-12-01"+
			"&ResourceName=mt-cluster"+
			"&Tags.Tag.1.Key=k1&Tags.Tag.1.Value=v1"+
			"&Tags.Tag.2.Key=k2&Tags.Tag.2.Value=v2"+
			"&Tags.Tag.3.Key=k3&Tags.Tag.3.Value=v3")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateTagsResponse")

	// Verify tags were stored
	tags := b.DescribeTags()
	assert.Equal(t, "v1", tags["mt-cluster"]["k1"])
	assert.Equal(t, "v2", tags["mt-cluster"]["k2"])
	assert.Equal(t, "v3", tags["mt-cluster"]["k3"])
}

// ---- ChaosServiceName and ChaosOperations ----

func TestHandler_ChaosFields(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	assert.Equal(t, "redshift", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}
