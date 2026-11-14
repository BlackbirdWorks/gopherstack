package app

import (
	"fmt"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpcloudbuildbackend "github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
	gcprunbackend "github.com/blackbirdworks/gopherstack/services/gcprun"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
)

// cloudRunBuilder submits Cloud Run SubmitBuild requests to the Cloud Build emulator.
type cloudRunBuilder struct {
	b *gcpcloudbuildbackend.InMemoryBackend
}

func (c cloudRunBuilder) SubmitBuild(parent string, build *cloudbuildpb.Build) (*longrunningpb.Operation, error) {
	op, err := c.b.CreateBuild(&cloudbuildpb.CreateBuildRequest{Parent: parent, Build: build})
	if err != nil {
		return nil, fmt.Errorf("cloud build: %w", err)
	}

	return op, nil
}

// wireGCPRun lets Cloud Run read secret environment variables and secret volumes from Secret Manager and
// submit builds to Cloud Build.
func wireGCPRun(byName map[string]service.Registerable) {
	run, ok := byName["GCPRun"].(*gcprunbackend.Handler)
	if !ok {
		return
	}

	if cb, isCB := byName["GCPCloudBuild"].(*gcpcloudbuildbackend.Handler); isCB {
		run.Backend.SetBuilder(cloudRunBuilder{b: cb.Backend})
	}

	if sm, isSM := byName["GCPSecretManager"].(*gcpsecretmanagerbackend.Handler); isSM {
		run.Backend.SetSecretResolver(func(versionName string) ([]byte, error) {
			resp, err := sm.Backend.AccessSecretVersion(versionName)
			if err != nil {
				return nil, fmt.Errorf("access %s: %w", versionName, err)
			}

			return resp.GetPayload().GetData(), nil
		})
	}
}
