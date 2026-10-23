package app

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"testing"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpfunctionsbackend "github.com/blackbirdworks/gopherstack/services/gcpfunctions"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

func TestWireGCPFunctions(t *testing.T) {
	t.Parallel()

	const (
		loc   = "projects/p/locations/us-central1"
		topic = "projects/p/topics/events"
		sub   = "projects/p/subscriptions/gcf-w-us-central1-events"
	)

	tests := []struct {
		name     string
		wired    bool
		wantCode codes.Code
	}{
		{"wired", true, codes.OK},
		{"not wired", false, codes.FailedPrecondition},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ps := gcppubsubbackend.NewHandler(gcppubsubbackend.NewInMemoryBackend())
			gcs := gcpstoragebackend.NewHandler(gcpstoragebackend.NewInMemoryBackend())
			fns := gcpfunctionsbackend.NewHandler(gcpfunctionsbackend.NewInMemoryBackend())
			fns.Backend.SetIngressPort(8129)

			byName := map[string]service.Registerable{"GCPFunctions": fns}
			if tt.wired {
				byName["GCPPubSub"], byName["GCPStorage"] = ps, gcs
			}

			wireGCPFunctions(byName)

			_, err := ps.Backend.CreateTopic(&pubsubpb.Topic{Name: topic})
			require.NoError(t, err)

			_, err = gcs.Backend.CreateBucket("p", gcpstoragebackend.Patch{"name": json.RawMessage(`"src-bucket"`)})
			require.NoError(t, err)

			var buf bytes.Buffer

			zw := zip.NewWriter(&buf)
			w, err := zw.Create("index.js")
			require.NoError(t, err)
			_, err = w.Write([]byte("exports.h = () => {};"))
			require.NoError(t, err)
			require.NoError(t, zw.Close())

			_, err = gcs.Backend.PutObject("src-bucket", "fn.zip", buf.Bytes(), gcpstoragebackend.Patch{},
				gcpstoragebackend.Preconds{})
			require.NoError(t, err)

			op, err := fns.Backend.CreateFunction(&functionspb.CreateFunctionRequest{
				Parent: loc, FunctionId: "w", Function: &functionspb.Function{
					BuildConfig: &functionspb.BuildConfig{
						Runtime: "nodejs22", EntryPoint: "h",
						Source: &functionspb.Source{Source: &functionspb.Source_StorageSource{
							StorageSource: &functionspb.StorageSource{Bucket: "src-bucket", Object: "fn.zip"},
						}},
					},
					EventTrigger: &functionspb.EventTrigger{
						EventType: gcpfunctionsbackend.EventTypePubSub, PubsubTopic: topic,
					},
				},
			})
			require.Equal(t, tt.wantCode, status.Code(err), "%v", err)

			if tt.wantCode != codes.OK {
				return
			}

			fns.Backend.Drain()
			assert.NotNil(t, op)

			got, err := ps.Backend.GetSubscription(sub)
			require.NoError(t, err)
			assert.Equal(t, topic, got.GetTopic())
		})
	}
}
