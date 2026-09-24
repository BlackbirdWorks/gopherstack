package cloudformation_test

import (
	"testing"

	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"

	backupbackend "github.com/blackbirdworks/gopherstack/services/backup"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

// newMoreResourcesTestClient extends newDependentServiceBackends (which
// already wires IAM, ECR, ElastiCache, Neptune, DocDB, Glue, CodeBuild, and
// Kinesis) with Backup and Lambda, for the new resource types added across
// resources_iam_more.go, resources_ecr_more.go, resources_elasticache_more.go,
// resources_neptune_more.go, resources_docdb_more.go, resources_backup_more.go,
// resources_glue_more.go, and resources_misc_more.go.
func newMoreResourcesTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newDependentServiceBackends(t)
	backends.Backup = backupbackend.NewHandler(backupbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	backends.Lambda = lambdabackend.NewHandler(lambdabackend.NewInMemoryBackend(
		nil, nil, lambdabackend.DefaultSettings(), "000000000000", "us-east-1",
	))

	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}
