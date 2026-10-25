package dashboard_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
	"github.com/blackbirdworks/gopherstack/services/gcpbigtable"
	"github.com/blackbirdworks/gopherstack/services/gcpeventarc"
	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

func TestGCPBigtableProxy(t *testing.T) {
	t.Parallel()

	const (
		root  = "/dashboard/api/v1/gcp/bigtable/v1"
		inst  = root + "/projects/p/instances/inst1"
		table = inst + "/tables/t1"
	)

	seed := func(t *testing.T) *dashboard.DashboardHandler {
		t.Helper()

		bt := gcpbigtable.NewHandler(gcpbigtable.NewInMemoryBackend())
		_, err := bt.Backend.CreateInstance(&adminpb.CreateInstanceRequest{
			Parent:     "projects/p",
			InstanceId: "inst1",
			Instance:   &adminpb.Instance{DisplayName: "Inst1"},
			Clusters: map[string]*adminpb.Cluster{
				"c1": {Location: "projects/p/locations/us-central1-b", ServeNodes: 1},
			},
		})
		require.NoError(t, err)

		_, err = bt.Backend.CreateTable(&adminpb.CreateTableRequest{
			Parent: "projects/p/instances/inst1", TableId: "t1",
			Table: &adminpb.Table{ColumnFamilies: map[string]*adminpb.ColumnFamily{"cf": {}}},
		})
		require.NoError(t, err)

		for _, key := range []string{"user#1", "user#2", "order#1"} {
			_, err = bt.Backend.MutateRow(&btpb.MutateRowRequest{
				TableName: "projects/p/instances/inst1/tables/t1", RowKey: []byte(key),
				Mutations: []*btpb.Mutation{{Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
					FamilyName: "cf", ColumnQualifier: []byte("q"), TimestampMicros: 1000, Value: []byte("v-" + key),
				}}}},
			})
			require.NoError(t, err)
		}

		return dashboard.NewHandler(dashboard.Config{GCPBigtableOps: bt})
	}

	prefix := `{"rows":{"rowRanges":[{"startKeyClosed":"dXNlciM=","endKeyOpen":"dXNlciQ="}]}}`
	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"browse", []gcpStep{
			{http.MethodGet, root + "/projects/p/instances", "", `"displayName":"Inst1"`, 200},
			{http.MethodGet, inst, "", `"name":"projects/p/instances/inst1"`, 200},
			{http.MethodGet, inst + "/clusters", "", `/clusters/c1"`, 200},
			{http.MethodGet, inst + "/tables?view=FULL", "", `"columnFamilies"`, 200},
			{http.MethodGet, table, "", `"cf"`, 200},
			{http.MethodPost, table + ":readRows", `{}`, `"rows"`, 200},
			{http.MethodPost, table + ":readRows", prefix, "dXNlciMx", 200},
			{http.MethodDelete, table, "", "{}", 200},
			{http.MethodGet, table, "", "NOT_FOUND", http.StatusNotFound},
			{http.MethodDelete, inst, "", "{}", 200},
		}},
		{"rejects", []gcpStep{
			{http.MethodGet, root + "/projects/p/other", "", "unsupported", http.StatusNotFound},
			{http.MethodPut, inst, "{}", "unsupported", http.StatusNotFound},
			{http.MethodPost, inst + ":bogus", "", "unsupported", http.StatusNotFound},
			{http.MethodGet, inst + "/other", "", "unsupported", http.StatusNotFound},
			{http.MethodGet, inst + "/tables/t1/other", "", "unsupported", http.StatusNotFound},
			{http.MethodPost, table + ":readRows", `{bad`, "invalid request body", http.StatusBadRequest},
			{http.MethodPost, inst + "/tables/nope:readRows", `{}`, "NOT_FOUND", http.StatusNotFound},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runGCPSteps(t, seed(t), tt.steps)
		})
	}
}

func TestGCPEventarcProxy(t *testing.T) {
	t.Parallel()

	const (
		loc     = "/dashboard/api/v1/gcp/eventarc/v1/projects/p/locations/us-central1"
		trigger = loc + "/triggers/trig1"
	)

	seed := func(t *testing.T) *dashboard.DashboardHandler {
		t.Helper()

		ea := gcpeventarc.NewHandler(gcpeventarc.NewInMemoryBackend())
		_, err := ea.Backend.CreateTrigger(&eventarcpb.CreateTriggerRequest{
			Parent: "projects/p/locations/us-central1", TriggerId: "trig1",
			Trigger: &eventarcpb.Trigger{
				EventFilters: []*eventarcpb.EventFilter{
					{Attribute: "type", Value: "google.cloud.pubsub.topic.v1.messagePublished"},
				},
				Destination: &eventarcpb.Destination{Descriptor_: &eventarcpb.Destination_CloudRun{
					CloudRun: &eventarcpb.CloudRun{Service: "svc", Region: "us-central1"},
				}},
			},
		})
		require.NoError(t, err)

		return dashboard.NewHandler(dashboard.Config{GCPEventarcOps: ea})
	}

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"browse", []gcpStep{
			{http.MethodGet, loc + "/triggers", "", `"triggers"`, 200},
			{http.MethodGet, trigger, "", `"cloudRun"`, 200},
			{http.MethodGet, loc + "/channels", "", "{", 200},
			{http.MethodGet, loc + "/providers", "", "{", 200},
			{http.MethodDelete, trigger, "", `"done":true`, 200},
			{http.MethodGet, trigger, "", "NOT_FOUND", http.StatusNotFound},
		}},
		{"rejects", []gcpStep{
			{http.MethodGet, loc + "/other", "", "unsupported", http.StatusNotFound},
			{http.MethodPost, loc + "/triggers", "{}", "unsupported", http.StatusNotFound},
			{http.MethodPut, trigger, "{}", "unsupported", http.StatusNotFound},
			{http.MethodPost, trigger + ":bogus", "", "unsupported", http.StatusNotFound},
			{http.MethodGet, trigger + "/x/y", "", "unsupported", http.StatusNotFound},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runGCPSteps(t, seed(t), tt.steps)
		})
	}
}

func TestGCPArtifactRegistryProxy(t *testing.T) {
	t.Parallel()

	const (
		loc  = "/dashboard/api/v1/gcp/artifactregistry/v1/projects/p/locations/us-central1"
		repo = loc + "/repositories/r1"
	)

	seed := func(t *testing.T) *dashboard.DashboardHandler {
		t.Helper()

		ar := gcpartifactregistry.NewHandler(gcpartifactregistry.NewInMemoryBackend())
		_, err := ar.Backend.CreateRepository(&artifactregistrypb.CreateRepositoryRequest{
			Parent: "projects/p/locations/us-central1", RepositoryId: "r1",
			Repository: &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_GENERIC},
		})
		require.NoError(t, err)

		return dashboard.NewHandler(dashboard.Config{GCPArtifactRegistryOps: ar})
	}

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"browse", []gcpStep{
			{http.MethodGet, loc + "/repositories", "", `"repositories"`, 200},
			{http.MethodGet, repo, "", `"GENERIC"`, 200},
			{http.MethodGet, repo + "/packages", "", "{", 200},
			{http.MethodGet, repo + "/files", "", "{", 200},
			{http.MethodGet, repo + "/dockerImages", "", "not a DOCKER repository", http.StatusBadRequest},
			{http.MethodGet, repo + "/packages/nope", "", "NOT_FOUND", http.StatusNotFound},
			{http.MethodGet, repo + "/packages/a%2Fb/versions", "", "NOT_FOUND", http.StatusNotFound},
			{http.MethodDelete, repo, "", `"done":true`, 200},
			{http.MethodGet, repo, "", "NOT_FOUND", http.StatusNotFound},
		}},
		{"rejects", []gcpStep{
			{http.MethodGet, loc + "/other", "", "unsupported", http.StatusNotFound},
			{http.MethodPost, loc + "/repositories", "{}", "unsupported", http.StatusNotFound},
			{http.MethodPost, repo + ":bogus", "", "unsupported", http.StatusNotFound},
			{http.MethodGet, repo + "/rules", "", "unsupported", http.StatusNotFound},
			{http.MethodPut, repo, "{}", "unsupported", http.StatusNotFound},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runGCPSteps(t, seed(t), tt.steps)
		})
	}
}

func TestGCPSpannerProxy(t *testing.T) {
	t.Parallel()

	const root = "/dashboard/api/v1/gcp/spanner/v1/projects/p"

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"browse", []gcpStep{
			{http.MethodGet, root + "/instances", "", "{", 200},
			{http.MethodGet, root + "/instanceConfigs", "", `"instanceConfigs"`, 200},
			{http.MethodGet, root + "/instances/nope", "", "NOT_FOUND", http.StatusNotFound},
			{http.MethodGet, root + "/instances/nope/databases", "", "NOT_FOUND", http.StatusNotFound},
			{http.MethodGet, root + "/instances/nope/databases/d/ddl", "", "NOT_FOUND", http.StatusNotFound},
			{http.MethodDelete, root + "/instances/nope", "", "NOT_FOUND", http.StatusNotFound},
		}},
		{"rejects", []gcpStep{
			{http.MethodGet, root + "/other", "", "unsupported", http.StatusNotFound},
			{http.MethodPost, root + "/instances", "{}", "unsupported", http.StatusNotFound},
			{http.MethodPut, root + "/instances/i", "{}", "unsupported", http.StatusNotFound},
			{http.MethodPost, root + "/instances/i:bogus", "", "unsupported", http.StatusNotFound},
			{http.MethodGet, root + "/instances/i/other", "", "unsupported", http.StatusNotFound},
			{http.MethodGet, root + "/instances/i/databases/d/other", "", "unsupported", http.StatusNotFound},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{
				GCPSpannerOps: gcpspanner.NewHandler(gcpspanner.NewInMemoryBackend()),
			})
			runGCPSteps(t, h, tt.steps)
		})
	}
}

func TestGCPExplorerProxiesUnavailable(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	for _, svc := range []string{"bigtable", "eventarc", "spanner", "artifactregistry"} {
		path := "/dashboard/api/v1/gcp/" + svc + "/v1/projects/p/instances"
		runGCPSteps(t, h, []gcpStep{{http.MethodGet, path, "", "", http.StatusServiceUnavailable}})
	}
}
