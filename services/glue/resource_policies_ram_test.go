package glue_test

import (
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// fakeResourceShareCreator is a test double for glue.ResourceShareCreator that records
// every call instead of touching a real RAM backend.
type fakeResourceShareCreator struct {
	putCalls    []fakeShareCall
	deleteCalls []string
	mu          sync.Mutex
}

type fakeShareCall struct {
	resourceARN string
	principals  []string
	actions     []string
}

func (f *fakeResourceShareCreator) PutPolicyBasedShare(resourceARN string, principals, actions []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.putCalls = append(f.putCalls, fakeShareCall{resourceARN: resourceARN, principals: principals, actions: actions})

	return nil
}

func (f *fakeResourceShareCreator) DeletePolicyBasedShare(resourceARN string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.deleteCalls = append(f.deleteCalls, resourceARN)

	return nil
}

func TestPutResourcePolicy_RAMPolicyShareSeam(t *testing.T) {
	t.Parallel()

	const sharedResourceARN = "arn:aws:glue:us-east-1:000000000000:database/db1"

	tests := []struct {
		name              string
		resourceARN       string
		enableHybrid      string
		policy            string
		wantPutPrincipals []string
		wantPutActions    []string
		wantPut           bool
		wantDelete        bool
	}{
		{
			name:         "cross account principal creates a policy based share",
			resourceARN:  sharedResourceARN,
			enableHybrid: "TRUE",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"AWS":["arn:aws:iam::111122223333:role/Dev"]},` +
				`"Action":["glue:GetDatabase"]}]}`,
			wantPut:           true,
			wantPutPrincipals: []string{"arn:aws:iam::111122223333:role/Dev"},
			wantPutActions:    []string{"glue:GetDatabase"},
		},
		{
			name:         "same account principal only removes a stale share",
			resourceARN:  sharedResourceARN,
			enableHybrid: "TRUE",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"AWS":["000000000000"]},"Action":["glue:GetDatabase"]}]}`,
			wantDelete: true,
		},
		{
			name:         "wildcard principal is not RAM shareable",
			resourceARN:  sharedResourceARN,
			enableHybrid: "TRUE",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":"*","Action":["glue:GetDatabase"]}]}`,
			wantDelete: true,
		},
		{
			name:         "enable hybrid not true skips the seam entirely",
			resourceARN:  sharedResourceARN,
			enableHybrid: "",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"AWS":["111122223333"]},"Action":["glue:GetDatabase"]}]}`,
		},
		{
			name:         "account level policy has no single resource to share",
			resourceARN:  "",
			enableHybrid: "TRUE",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"AWS":["111122223333"]},"Action":["glue:GetDatabase"]}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := glue.NewInMemoryBackend(testAccountID, testRegion)
			fake := &fakeResourceShareCreator{}
			backend.SetResourceShareCreator(fake)
			h := glue.NewHandler(backend)

			rec := doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
				"PolicyInJson": tt.policy,
				"ResourceArn":  tt.resourceARN,
				"EnableHybrid": tt.enableHybrid,
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			fake.mu.Lock()
			defer fake.mu.Unlock()

			if tt.wantPut {
				require.Len(t, fake.putCalls, 1)
				assert.Equal(t, tt.resourceARN, fake.putCalls[0].resourceARN)
				assert.Equal(t, tt.wantPutPrincipals, fake.putCalls[0].principals)
				assert.Equal(t, tt.wantPutActions, fake.putCalls[0].actions)
			} else {
				assert.Empty(t, fake.putCalls)
			}

			if tt.wantDelete {
				require.Len(t, fake.deleteCalls, 1)
				assert.Equal(t, tt.resourceARN, fake.deleteCalls[0])
			} else {
				assert.Empty(t, fake.deleteCalls)
			}
		})
	}
}

func TestDeleteResourcePolicy_RAMPolicyShareSeam(t *testing.T) {
	t.Parallel()

	const resourceARN = "arn:aws:glue:us-east-1:000000000000:database/db1"

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	fake := &fakeResourceShareCreator{}
	backend.SetResourceShareCreator(fake)
	h := glue.NewHandler(backend)

	rec := doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"AWS":["111122223333"]},"Action":["glue:GetDatabase"]}]}`,
		"ResourceArn":  resourceARN,
		"EnableHybrid": "TRUE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "DeleteResourcePolicy", map[string]any{"ResourceArn": resourceARN})
	require.Equal(t, http.StatusOK, rec.Code)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Len(t, fake.deleteCalls, 1)
	assert.Equal(t, resourceARN, fake.deleteCalls[0])
}
