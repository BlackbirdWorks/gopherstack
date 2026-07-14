package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// --- Trigger ON_DEMAND semantics -------------------------------------------------
//
// Per AWS docs (about-triggers.html): "On-demand triggers never enter the ACTIVATED
// or DEACTIVATED state. They always remain in the CREATED state," and firing one
// runs its actions immediately.

func triggerState(t *testing.T, h *glue.Handler, name string) string {
	t.Helper()

	rec := doGlueRequest(t, h, "GetTrigger", map[string]any{"Name": name})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Trigger struct {
			State string `json:"State"`
		} `json:"Trigger"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out.Trigger.State
}

func Test_StartTrigger_OnDemand_StaysCreatedAndFiresJobRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateJob", map[string]any{
		"Name":    "ondemand-job",
		"Role":    "role1",
		"Command": map[string]any{"Name": "glueetl"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "CreateTrigger", map[string]any{
		"Name": "ondemand-trigger",
		"Type": "ON_DEMAND",
		"Actions": []map[string]any{
			{"JobName": "ondemand-job", "Arguments": map[string]any{"--x": "1"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "CREATED", triggerState(t, h, "ondemand-trigger"))

	rec = doGlueRequest(t, h, "StartTrigger", map[string]any{"Name": "ondemand-trigger"})
	require.Equal(t, http.StatusOK, rec.Code)

	// On-demand triggers never enter ACTIVATED — they remain CREATED.
	assert.Equal(t, "CREATED", triggerState(t, h, "ondemand-trigger"))

	// Firing the trigger must have started a run of the job it targets.
	rec = doGlueRequest(t, h, "GetJobRuns", map[string]any{"JobName": "ondemand-job"})
	require.Equal(t, http.StatusOK, rec.Code)

	var runsOut struct {
		JobRuns []struct {
			JobRunState string `json:"JobRunState"`
		} `json:"JobRuns"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &runsOut))
	require.Len(t, runsOut.JobRuns, 1)
	assert.NotEmpty(t, runsOut.JobRuns[0].JobRunState)
}

func Test_StopTrigger_OnDemand_StaysCreated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "od-stop", "Type": "ON_DEMAND"})

	rec := doGlueRequest(t, h, "StopTrigger", map[string]any{"Name": "od-stop"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "CREATED", triggerState(t, h, "od-stop"))
}

func Test_StartTrigger_Scheduled_StillActivates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "sched1", "Type": "SCHEDULED"})

	rec := doGlueRequest(t, h, "StartTrigger", map[string]any{"Name": "sched1"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ACTIVATED", triggerState(t, h, "sched1"))
}

func Test_CreateTrigger_StartOnCreation_ActivatesScheduledTrigger(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateTrigger", map[string]any{
		"Name":            "auto-start",
		"Type":            "SCHEDULED",
		"Schedule":        "cron(0 12 * * ? *)",
		"StartOnCreation": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ACTIVATED", triggerState(t, h, "auto-start"))
}

func Test_CreateTrigger_StartOnCreation_IgnoredForOnDemand(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateTrigger", map[string]any{
		"Name":            "auto-start-od",
		"Type":            "ON_DEMAND",
		"StartOnCreation": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// AWS: "True is not supported for ON_DEMAND triggers" — they stay CREATED.
	assert.Equal(t, "CREATED", triggerState(t, h, "auto-start-od"))
}

// --- Resource policy optimistic-concurrency conditions ---------------------------

func Test_PutResourcePolicy_ExistsConditions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// MUST_EXIST against a policy that doesn't exist yet -> EntityNotFoundException.
	rec := doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson":          `{"Version":"2012-10-17"}`,
		"PolicyExistsCondition": "MUST_EXIST",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "EntityNotFoundException")

	// NOT_EXIST creates it fine.
	rec = doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson":          `{"Version":"2012-10-17"}`,
		"PolicyExistsCondition": "NOT_EXIST",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// NOT_EXIST again, now that it exists -> ConditionCheckFailureException.
	rec = doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson":          `{"Version":"2012-10-17"}`,
		"PolicyExistsCondition": "NOT_EXIST",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ConditionCheckFailureException")

	// MUST_EXIST now succeeds.
	rec = doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson":          `{"Version":"2012-10-17","Id":"2"}`,
		"PolicyExistsCondition": "MUST_EXIST",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func Test_PutResourcePolicy_HashCondition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson": `{"Version":"2012-10-17"}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		PolicyHash string `json:"PolicyHash"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.PolicyHash)

	// Stale hash -> ConditionCheckFailureException, and the policy is untouched.
	rec = doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson":        `{"Version":"stale-write"}`,
		"PolicyHashCondition": "not-the-real-hash",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ConditionCheckFailureException")

	// Correct hash succeeds.
	rec = doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson":        `{"Version":"2012-10-17","Id":"2"}`,
		"PolicyHashCondition": out.PolicyHash,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func Test_DeleteResourcePolicy_HashCondition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson": `{"Version":"2012-10-17"}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		PolicyHash string `json:"PolicyHash"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// Wrong hash -> rejected, policy stays.
	rec = doGlueRequest(t, h, "DeleteResourcePolicy", map[string]any{
		"PolicyHashCondition": "wrong-hash",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ConditionCheckFailureException")

	rec = doGlueRequest(t, h, "GetResourcePolicy", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	// Correct hash deletes it.
	rec = doGlueRequest(t, h, "DeleteResourcePolicy", map[string]any{
		"PolicyHashCondition": out.PolicyHash,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetResourcePolicy", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Live-pointer aliasing (data race) fixes --------------------------------------

func Test_CreateSchema_ReturnedCopyNotAliasedByLaterUpdate(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	defer b.Close()

	_, err := b.CreateRegistry("reg1", "", nil)
	require.NoError(t, err)

	created, err := b.CreateSchema("reg1", "sch1", "AVRO", "NONE", "", nil)
	require.NoError(t, err)
	require.Equal(t, "NONE", created.Compatibility)

	// UpdateSchema must not retroactively change a struct already handed back to a
	// caller from CreateSchema.
	require.NoError(t, b.UpdateSchema("reg1", "sch1", "BACKWARD", ""))
	assert.Equal(t, "NONE", created.Compatibility, "CreateSchema's returned copy must not alias backend state")

	fresh, err := b.DescribeSchema("reg1", "sch1")
	require.NoError(t, err)
	assert.Equal(t, "BACKWARD", fresh.Compatibility)
}

func Test_IntegrationResourceProperty_GetReturnsIndependentCopy(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	defer b.Close()

	const resourceArn = "arn:aws:glue:us-east-1:000000000000:integration/test"

	_, err := b.CreateIntegrationResourceProperty(
		resourceArn,
		map[string]string{"k": "v1"},
		nil,
	)
	require.NoError(t, err)

	got, err := b.GetIntegrationResourceProperty(resourceArn)
	require.NoError(t, err)
	require.Equal(t, "v1", got.SourceProperties["k"])

	// UpdateIntegrationResourceProperty must not mutate a map already handed back
	// to a caller from Get/CreateIntegrationResourceProperty.
	_, err = b.UpdateIntegrationResourceProperty(resourceArn, map[string]string{"k": "v2"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "v1", got.SourceProperties["k"], "GetIntegrationResourceProperty must return an independent copy")

	fresh, err := b.GetIntegrationResourceProperty(resourceArn)
	require.NoError(t, err)
	assert.Equal(t, "v2", fresh.SourceProperties["k"])
}
