package personalize_test

import (
	"testing"

	personalizesdk "github.com/aws/aws-sdk-go-v2/service/personalize"
	personalizeruntimesdk "github.com/aws/aws-sdk-go-v2/service/personalizeruntime"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/personalize"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := personalize.NewHandler(personalize.NewInMemoryBackend("000000000000", "us-east-1"))

	// runtimeOps are the personalize *runtime* (inference-time) operations.
	// AWS models these on a separate SDK client, personalizeruntime.Client,
	// distinct from the personalize control-plane client
	// (personalizesdk.Client) checked below. gopherstack's single Handler
	// implements both surfaces and reports them together from
	// GetSupportedOperations(), so this test splits them before checking
	// each half against the SDK client that actually owns it.
	runtimeOps := map[string]bool{
		"GetPersonalizedRanking": true,
		"GetRecommendations":     true,
	}

	var controlPlaneOps, inferenceOps []string
	for _, op := range h.GetSupportedOperations() {
		if runtimeOps[op] {
			inferenceOps = append(inferenceOps, op)
		} else {
			controlPlaneOps = append(controlPlaneOps, op)
		}
	}

	sdkcheck.CheckCompleteness(t, &personalizesdk.Client{}, controlPlaneOps, []string{})
	// GetActionRecommendations is a real personalizeruntime operation this
	// Handler does not implement.
	sdkcheck.CheckCompleteness(
		t, &personalizeruntimesdk.Client{}, inferenceOps, []string{"GetActionRecommendations"},
	)
}
