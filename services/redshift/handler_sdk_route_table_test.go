package redshift_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// sdkClassicRouteCases is the authoritative Action value for every real
// classic Redshift operation, extracted from redshift@v1.65.4 serializers.go:
// each op's awsAwsquery_serializeOp<Op>.HandleSerialize sets
// body.Key("Action").String("<Op>") and always POSTs to "/" -- classic
// Redshift is AWS Query/XML (services/_PROTOCOLS.md), so unlike a
// REST-family service there is no path template to get wrong: dispatch is
// entirely by this one form field, via h.ops[action] map lookup
// (handler.go:524). ExtractOperation reads r.Form.Get("Action") directly, so
// the class of bug this table catches is a dispatch-table key that doesn't
// exactly match the real op name (typo, wrong case) -- not a route-template
// mismatch.
//
// This table covers all 145 real classic Redshift ops (redshift@v1.65.4) --
// confirmed by diffing h.buildOps()'s 145 map keys (quoted string literals
// plus the opXxx constants shared with the Serverless client below) against
// this exact list, zero mismatches either direction.
//
// Regenerate by grepping serializers.go for every
// `body.Key("Action").String("` and pulling the argument.
func sdkClassicRouteCases() []string {
	return []string{
		"AcceptReservedNodeExchange",
		"AddPartner",
		"AssociateDataShareConsumer",
		"AuthorizeClusterSecurityGroupIngress",
		"AuthorizeDataShare",
		"AuthorizeEndpointAccess",
		"AuthorizeSnapshotAccess",
		"BatchDeleteClusterSnapshots",
		"BatchModifyClusterSnapshots",
		"CancelResize",
		"CopyClusterSnapshot",
		"CreateAuthenticationProfile",
		"CreateCluster",
		"CreateClusterParameterGroup",
		"CreateClusterSecurityGroup",
		"CreateClusterSnapshot",
		"CreateClusterSubnetGroup",
		"CreateCustomDomainAssociation",
		"CreateEndpointAccess",
		"CreateEventSubscription",
		"CreateHsmClientCertificate",
		"CreateHsmConfiguration",
		"CreateIntegration",
		"CreateQev2IdcApplication",
		"CreateRedshiftIdcApplication",
		"CreateScheduledAction",
		"CreateSnapshotCopyGrant",
		"CreateSnapshotSchedule",
		"CreateTags",
		"CreateUsageLimit",
		"DeauthorizeDataShare",
		"DeleteAuthenticationProfile",
		"DeleteCluster",
		"DeleteClusterParameterGroup",
		"DeleteClusterSecurityGroup",
		"DeleteClusterSnapshot",
		"DeleteClusterSubnetGroup",
		"DeleteCustomDomainAssociation",
		"DeleteEndpointAccess",
		"DeleteEventSubscription",
		"DeleteHsmClientCertificate",
		"DeleteHsmConfiguration",
		"DeleteIntegration",
		"DeletePartner",
		"DeleteQev2IdcApplication",
		"DeleteRedshiftIdcApplication",
		"DeleteResourcePolicy",
		"DeleteScheduledAction",
		"DeleteSnapshotCopyGrant",
		"DeleteSnapshotSchedule",
		"DeleteTags",
		"DeleteUsageLimit",
		"DeregisterNamespace",
		"DescribeAccountAttributes",
		"DescribeAuthenticationProfiles",
		"DescribeClusterDbRevisions",
		"DescribeClusterParameterGroups",
		"DescribeClusterParameters",
		"DescribeClusterSecurityGroups",
		"DescribeClusterSnapshots",
		"DescribeClusterSubnetGroups",
		"DescribeClusterTracks",
		"DescribeClusterVersions",
		"DescribeClusters",
		"DescribeCustomDomainAssociations",
		"DescribeDataShares",
		"DescribeDataSharesForConsumer",
		"DescribeDataSharesForProducer",
		"DescribeDefaultClusterParameters",
		"DescribeEndpointAccess",
		"DescribeEndpointAuthorization",
		"DescribeEventCategories",
		"DescribeEventSubscriptions",
		"DescribeEvents",
		"DescribeHsmClientCertificates",
		"DescribeHsmConfigurations",
		"DescribeInboundIntegrations",
		"DescribeIntegrations",
		"DescribeLoggingStatus",
		"DescribeNodeConfigurationOptions",
		"DescribeOrderableClusterOptions",
		"DescribePartners",
		"DescribeQev2IdcApplications",
		"DescribeRedshiftIdcApplications",
		"DescribeReservedNodeExchangeStatus",
		"DescribeReservedNodeOfferings",
		"DescribeReservedNodes",
		"DescribeResize",
		"DescribeScheduledActions",
		"DescribeSnapshotCopyGrants",
		"DescribeSnapshotSchedules",
		"DescribeStorage",
		"DescribeTableRestoreStatus",
		"DescribeTags",
		"DescribeUsageLimits",
		"DisableLogging",
		"DisableSnapshotCopy",
		"DisassociateDataShareConsumer",
		"EnableLogging",
		"EnableSnapshotCopy",
		"FailoverPrimaryCompute",
		"GetClusterCredentials",
		"GetClusterCredentialsWithIAM",
		"GetIdentityCenterAuthToken",
		"GetReservedNodeExchangeConfigurationOptions",
		"GetReservedNodeExchangeOfferings",
		"GetResourcePolicy",
		"ListRecommendations",
		"ModifyAquaConfiguration",
		"ModifyAuthenticationProfile",
		"ModifyCluster",
		"ModifyClusterDbRevision",
		"ModifyClusterIamRoles",
		"ModifyClusterMaintenance",
		"ModifyClusterParameterGroup",
		"ModifyClusterSnapshot",
		"ModifyClusterSnapshotSchedule",
		"ModifyClusterSubnetGroup",
		"ModifyCustomDomainAssociation",
		"ModifyEndpointAccess",
		"ModifyEventSubscription",
		"ModifyIntegration",
		"ModifyLakehouseConfiguration",
		"ModifyQev2IdcApplication",
		"ModifyRedshiftIdcApplication",
		"ModifyScheduledAction",
		"ModifySnapshotCopyRetentionPeriod",
		"ModifySnapshotSchedule",
		"ModifyUsageLimit",
		"PauseCluster",
		"PurchaseReservedNodeOffering",
		"PutResourcePolicy",
		"RebootCluster",
		"RegisterNamespace",
		"RejectDataShare",
		"ResetClusterParameterGroup",
		"ResizeCluster",
		"RestoreFromClusterSnapshot",
		"RestoreTableFromClusterSnapshot",
		"ResumeCluster",
		"RevokeClusterSecurityGroupIngress",
		"RevokeEndpointAccess",
		"RevokeSnapshotAccess",
		"RotateEncryptionKey",
		"UpdatePartnerStatus",
	}
}

// TestExtractOperation_SDKRouteTable_Classic drives every real classic
// Redshift operation's authoritative Action value through ExtractOperation
// and Handler(), asserting the form field resolves to the right op name and
// that Handler() does not fall through to the "is not a valid Redshift
// action" sentinel that a dispatch-table key mismatch would produce.
func TestExtractOperation_SDKRouteTable_Classic(t *testing.T) {
	t.Parallel()

	for _, op := range sdkClassicRouteCases() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action="+op))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "is not a valid Redshift action",
				"action=%s: dispatched to the unmatched-route handler", op)
		})
	}
}

// sdkServerlessRouteCases is the authoritative X-Amz-Target for every real
// Redshift Serverless operation gopherstack implements, extracted from
// redshiftserverless@v1.38.5 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("RedshiftServerless.<Op>")
// and always POSTs to "/" -- Redshift Serverless is JSON-RPC 1.1
// (services/_PROTOCOLS.md), the multi-protocol oddity this directory hosts
// alongside classic Redshift's AWS Query/XML above. Dispatch is entirely by
// this one header, via slDispatchTable[op] map lookup (handler_serverless.go:204).
//
// The real SDK defines 65 operations; gopherstack implements 60 of them. The
// remaining 5 -- CreateReservation, GetReservation, GetReservationOffering,
// ListReservationOfferings, ListReservations -- are the Redshift Serverless
// Reservations API, added to the SDK but not yet implemented here (no
// slDispatchTable entry, no handler). They are real gaps, not table
// candidates: a route-table entry would assert a dispatch that does not
// exist. Confirmed by diffing slDispatchTable's 60 map keys (quoted string
// literals plus the opXxx constants shared with classic Redshift above)
// against the full 65-op SDK list.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("RedshiftServerless.` and pulling the suffix.
func sdkServerlessRouteCases() []string {
	return []string{
		"ConvertRecoveryPointToSnapshot",
		"CreateCustomDomainAssociation",
		"CreateEndpointAccess",
		"CreateNamespace",
		"CreateScheduledAction",
		"CreateSnapshot",
		"CreateSnapshotCopyConfiguration",
		"CreateUsageLimit",
		"CreateWorkgroup",
		"DeleteCustomDomainAssociation",
		"DeleteEndpointAccess",
		"DeleteNamespace",
		"DeleteResourcePolicy",
		"DeleteScheduledAction",
		"DeleteSnapshot",
		"DeleteSnapshotCopyConfiguration",
		"DeleteUsageLimit",
		"DeleteWorkgroup",
		"GetCredentials",
		"GetCustomDomainAssociation",
		"GetEndpointAccess",
		"GetIdentityCenterAuthToken",
		"GetNamespace",
		"GetRecoveryPoint",
		"GetResourcePolicy",
		"GetScheduledAction",
		"GetSnapshot",
		"GetTableRestoreStatus",
		"GetTrack",
		"GetUsageLimit",
		"GetWorkgroup",
		"ListCustomDomainAssociations",
		"ListEndpointAccess",
		"ListManagedWorkgroups",
		"ListNamespaces",
		"ListRecoveryPoints",
		"ListScheduledActions",
		"ListSnapshotCopyConfigurations",
		"ListSnapshots",
		"ListTableRestoreStatus",
		"ListTagsForResource",
		"ListTracks",
		"ListUsageLimits",
		"ListWorkgroups",
		"PutResourcePolicy",
		"RestoreFromRecoveryPoint",
		"RestoreFromSnapshot",
		"RestoreTableFromRecoveryPoint",
		"RestoreTableFromSnapshot",
		"TagResource",
		"UntagResource",
		"UpdateCustomDomainAssociation",
		"UpdateEndpointAccess",
		"UpdateLakehouseConfiguration",
		"UpdateNamespace",
		"UpdateScheduledAction",
		"UpdateSnapshot",
		"UpdateSnapshotCopyConfiguration",
		"UpdateUsageLimit",
		"UpdateWorkgroup",
	}
}

// TestExtractOperation_SDKRouteTable_Serverless drives every implemented
// real Redshift Serverless operation's authoritative X-Amz-Target through
// ExtractOperation and Handler(), asserting the header resolves to the right
// op name and that Handler() does not fall through to the "unknown
// operation: " sentinel that a dispatch-table key mismatch would produce.
func TestExtractOperation_SDKRouteTable_Serverless(t *testing.T) {
	t.Parallel()

	for _, op := range sdkServerlessRouteCases() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", "us-east-1"))
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", "RedshiftServerless."+op)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown operation: ",
				"target=RedshiftServerless.%s: dispatched to the unmatched-route handler", op)
		})
	}
}
