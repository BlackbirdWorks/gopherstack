package gcpenforce_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpenforce"
)

func TestFromMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msg   proto.Message
		name  string
		want  string
		paths []string
	}{
		{name: "pubsub topic name", msg: &pubsubpb.Topic{Name: "projects/p1/topics/t"}, want: "p1"},
		{name: "pubsub publish topic", msg: &pubsubpb.PublishRequest{Topic: "projects/p2/topics/t"}, want: "p2"},
		{
			name: "pubsub subscription beats its topic",
			msg:  &pubsubpb.Subscription{Name: "projects/a/subscriptions/s", Topic: "projects/b/topics/t"}, want: "a",
		},
		{
			name: "pubsub update nests the topic",
			msg:  &pubsubpb.UpdateTopicRequest{Topic: &pubsubpb.Topic{Name: "projects/p3/topics/t"}}, want: "p3",
		},
		{name: "secretmanager parent", msg: &secretmanagerpb.CreateSecretRequest{Parent: "projects/p4"}, want: "p4"},
		{
			name: "regional parent", want: "p5",
			msg: &secretmanagerpb.ListSecretsRequest{Parent: "projects/p5/locations/us-east1"},
		},
		{name: "kms name", msg: &kmspb.GetCryptoKeyRequest{Name: "projects/p6/locations/g/keyRings/k/cryptoKeys/c"}, want: "p6"},
		{name: "iam resource", msg: &iampb.GetIamPolicyRequest{Resource: "projects/p7/topics/t"}, want: "p7"},
		{
			name: "full resource name", want: "p8",
			msg: &iampb.GetIamPolicyRequest{Resource: "//pubsub.googleapis.com/projects/p8/topics/t"},
		},
		{name: "spanner database", msg: &spannerpb.CreateSessionRequest{Database: "projects/p9/instances/i/databases/d"}, want: "p9"},
		{
			name: "bigtable table", want: "p10",
			msg: &btpb.ReadRowsRequest{TableName: "projects/p10/instances/i/tables/t"},
		},
		{name: "firestore database", msg: &firestorepb.GetDocumentRequest{Name: "projects/p11/databases/(default)/documents/c/d"}, want: "p11"},
		{name: "tasks parent", msg: &cloudtaskspb.ListTasksRequest{Parent: "projects/p12/locations/l/queues/q"}, want: "p12"},
		{name: "wildcard is no project", msg: &secretmanagerpb.ListSecretsRequest{Parent: "projects/-"}, want: ""},
		{name: "no project field", msg: &secretmanagerpb.ListSecretsRequest{}, want: ""},
		{name: "non project parent", msg: &secretmanagerpb.ListSecretsRequest{Parent: "folders/1"}, want: ""},
		{
			name: "override path wins", want: "viaOverride", paths: []string{"topic.name"},
			msg: &pubsubpb.UpdateTopicRequest{
				Topic:      &pubsubpb.Topic{Name: "projects/viaOverride/topics/t"},
				UpdateMask: nil,
			},
		},
		{name: "override missing falls back", msg: &pubsubpb.Topic{Name: "projects/p13/topics/t"}, paths: []string{"nope"}, want: "p13"},
		{name: "nil message", msg: nil, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, gcpenforce.FromMessage(tt.msg, tt.paths...))
		})
	}
}

func TestFromRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		url       string
		want      string
		queryOnly bool
	}{
		{name: "compute path", url: "/compute/v1/projects/p1/zones/z/instances", want: "p1"},
		{name: "bigquery path", url: "/bigquery/v2/projects/p2/datasets", want: "p2"},
		{name: "dns path with verb", url: "/dns/v1/projects/p3/managedZones", want: "p3"},
		{name: "storage query", url: "/storage/v1/b?project=p4", want: "p4", queryOnly: true},
		{name: "storage ignores object path", url: "/storage/v1/b/bkt/o/projects/x", want: "", queryOnly: true},
		{name: "encoded object name is no project", url: "/storage/v1/b/bkt/o/projects%2Fx", want: ""},
		{name: "no project", url: "/v1/accounts:signUp", want: ""},
		{name: "wildcard", url: "/v1/projects/-/instances", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, tt.url, nil)
			assert.Equal(t, tt.want, gcpenforce.FromRequest(r, tt.queryOnly))
		})
	}
}

func TestServiceForMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, method, fallback, want string
	}{
		{"pubsub", "/google.pubsub.v1.Publisher/Publish", "", "pubsub.googleapis.com"},
		{"subscriber", "/google.pubsub.v1.Subscriber/Pull", "", "pubsub.googleapis.com"},
		{"secretmanager", "/google.cloud.secretmanager.v1.SecretManagerService/CreateSecret", "", "secretmanager.googleapis.com"},
		{"kms", "/google.cloud.kms.v1.KeyManagementService/GetCryptoKey", "", "cloudkms.googleapis.com"},
		{"spanner", "/google.spanner.v1.Spanner/CreateSession", "", "spanner.googleapis.com"},
		{"firestore", "/google.firestore.v1.Firestore/GetDocument", "", "firestore.googleapis.com"},
		{"tasks", "/google.cloud.tasks.v2.CloudTasks/ListTasks", "", "cloudtasks.googleapis.com"},
		{"iam policy falls back", "/google.iam.v1.IAMPolicy/GetIamPolicy", "pubsub.googleapis.com", "pubsub.googleapis.com"},
		{"unknown falls back", "/nope.Service/Do", "x.googleapis.com", "x.googleapis.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, gcpenforce.ServiceForMethod(tt.method, tt.fallback))
		})
	}
}

func TestExempt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method string
		want   bool
	}{
		{"/google.longrunning.Operations/GetOperation", true},
		{"/google.cloud.resourcemanager.v3.Projects/GetProject", true},
		{"/google.api.serviceusage.v1.ServiceUsage/EnableService", true},
		{"/grpc.health.v1.Health/Check", true},
		{"/google.pubsub.v1.Publisher/Publish", false},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, gcpenforce.Exempt(tt.method))
		})
	}
}

func TestOperationsPath(t *testing.T) {
	t.Parallel()

	assert.True(t, gcpenforce.OperationsPath("/v1/projects/p/locations/l/operations/op"))
	assert.True(t, gcpenforce.OperationsPath("/v1/operations"))
	assert.False(t, gcpenforce.OperationsPath("/v1/projects/p/topics"))
}
