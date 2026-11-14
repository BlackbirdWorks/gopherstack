package app

import (
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/redis/apiv1/redispb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpeventarcbackend "github.com/blackbirdworks/gopherstack/services/gcpeventarc"
	gcpkmsbackend "github.com/blackbirdworks/gopherstack/services/gcpkms"
	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
	gcpredisbackend "github.com/blackbirdworks/gopherstack/services/gcpredis"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
	gcpworkflowsbackend "github.com/blackbirdworks/gopherstack/services/gcpworkflows"
)

func TestWireGCPCMEK(t *testing.T) {
	t.Parallel()

	kms := gcpkmsbackend.NewInMemoryBackend()
	sm := gcpsecretmanagerbackend.NewInMemoryBackend()
	gcs := gcpstoragebackend.NewInMemoryBackend()
	logs := gcploggingbackend.NewInMemoryBackend()
	arc := gcpeventarcbackend.NewInMemoryBackend()
	wf := gcpworkflowsbackend.NewInMemoryBackend()
	rd := gcpredisbackend.NewInMemoryBackend()

	t.Cleanup(rd.Close)

	wireGCPCMEK(map[string]service.Registerable{
		"GCPKMS":           gcpkmsbackend.NewHandler(kms),
		"GCPSecretManager": gcpsecretmanagerbackend.NewHandler(sm),
		"GCPStorage":       gcpstoragebackend.NewHandler(gcs),
		"GCPLogging":       gcploggingbackend.NewHandler(logs),
		"GCPEventarc":      gcpeventarcbackend.NewHandler(arc),
		"GCPWorkflows":     gcpworkflowsbackend.NewHandler(wf),
		"GCPRedis":         gcpredisbackend.NewHandler(rd),
	})

	const loc = "projects/p/locations/us-east1"

	_, err := kms.CreateKeyRing(&kmspb.CreateKeyRingRequest{Parent: loc, KeyRingId: "r"})
	require.NoError(t, err)

	_, err = kms.CreateCryptoKey(&kmspb.CreateCryptoKeyRequest{
		Parent:      loc + "/keyRings/r",
		CryptoKeyId: "k",
		CryptoKey:   &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
	})
	require.NoError(t, err)

	const (
		good = loc + "/keyRings/r/cryptoKeys/k"
		bad  = loc + "/keyRings/r/cryptoKeys/missing"
	)

	secret := func(key string) error {
		_, e := sm.CreateSecret(&secretmanagerpb.CreateSecretRequest{
			Parent: "projects/p", SecretId: "s" + key[len(key)-1:], Secret: &secretmanagerpb.Secret{
				Replication: &secretmanagerpb.Replication{Replication: &secretmanagerpb.Replication_UserManaged_{
					UserManaged: &secretmanagerpb.Replication_UserManaged{
						Replicas: []*secretmanagerpb.Replication_UserManaged_Replica{{
							Location:                  "us-east1",
							CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: key},
						}},
					},
				}},
			},
		})

		return e
	}

	require.NoError(t, secret(good))
	require.Error(t, secret(bad))

	_, err = logs.UpdateCmekSettings(&loggingpb.UpdateCmekSettingsRequest{
		Name: "projects/p/cmekSettings", CmekSettings: &loggingpb.CmekSettings{KmsKeyName: bad},
	})
	require.Error(t, err)

	_, err = arc.CreateChannel(&eventarcpb.CreateChannelRequest{
		Parent: loc, ChannelId: "chan", Channel: &eventarcpb.Channel{CryptoKeyName: bad},
	})
	require.Error(t, err)

	_, err = wf.CreateWorkflow(&workflowspb.CreateWorkflowRequest{
		Parent: loc, WorkflowId: "w", Workflow: &workflowspb.Workflow{
			SourceCode:    &workflowspb.Workflow_SourceContents{SourceContents: "- s:\n    return: 1\n"},
			CryptoKeyName: bad,
		},
	})
	require.Error(t, err)

	_, err = rd.CreateInstance(&redispb.CreateInstanceRequest{
		Parent: loc, InstanceId: "i", Instance: &redispb.Instance{
			Tier: redispb.Instance_BASIC, MemorySizeGb: 1, CustomerManagedKey: bad,
		},
	})
	require.Error(t, err)

	_, err = gcs.CreateBucket("p", gcpstoragebackend.Patch{
		"name": []byte(`"bkt"`), "location": []byte(`"US-EAST1"`),
		"encryption": []byte(`{"defaultKmsKeyName":"` + bad + `"}`),
	})
	require.Error(t, err)
}
