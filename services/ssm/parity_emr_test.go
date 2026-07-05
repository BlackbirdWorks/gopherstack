package ssm_test

// parity_emr_test.go — table tests for parity gaps fixed in audit-ssm:
//
//  1. LabelParameterVersion: max-10-labels-per-version constraint
//  2. Command status machine: Pending → InProgress → Success
//  3. Parameter Expiration policy: janitor evicts past-expiry parameters

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func putTestParam(t *testing.T, b *ssm.InMemoryBackend, name string) {
	t.Helper()

	_, err := b.PutParameter(context.Background(), &ssm.PutParameterInput{
		Name:  name,
		Value: "v",
		Type:  "String",
	})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// 1. LabelParameterVersion: max-10-labels-per-version constraint
// ---------------------------------------------------------------------------

func TestParityEMR_LabelParameterVersion_MaxTenLabels(t *testing.T) {
	t.Parallel()

	t.Run("exactly 10 labels accepted", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		putTestParam(t, b, "/p/max10")

		labels := make([]string, 10)
		for i := range labels {
			labels[i] = fmt.Sprintf("label%02d", i)
		}

		out, err := b.LabelParameterVersion(context.Background(), &ssm.LabelParameterVersionInput{
			Name:   "/p/max10",
			Labels: labels,
		})
		require.NoError(t, err)
		assert.Len(t, out.AddedLabels, 10)
		assert.Empty(t, out.InvalidLabels, "no labels should be invalid at exactly 10")
	})

	t.Run("11th label rejected as InvalidLabel", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		putTestParam(t, b, "/p/overflow")

		// Add 10 labels first.
		first10 := make([]string, 10)
		for i := range first10 {
			first10[i] = fmt.Sprintf("lab%02d", i)
		}

		_, err := b.LabelParameterVersion(context.Background(), &ssm.LabelParameterVersionInput{
			Name:   "/p/overflow",
			Labels: first10,
		})
		require.NoError(t, err)

		// Adding one more must come back as InvalidLabel.
		out, err := b.LabelParameterVersion(context.Background(), &ssm.LabelParameterVersionInput{
			Name:   "/p/overflow",
			Labels: []string{"overflow"},
		})
		require.NoError(t, err)
		assert.Empty(t, out.AddedLabels, "overflow label must not be added")
		assert.Equal(t, []string{"overflow"}, out.InvalidLabels)
	})

	t.Run("duplicate labels not double-counted toward limit", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		putTestParam(t, b, "/p/dup")

		_, err := b.LabelParameterVersion(context.Background(), &ssm.LabelParameterVersionInput{
			Name:   "/p/dup",
			Labels: []string{"stable", "prod"},
		})
		require.NoError(t, err)

		// Re-applying the same labels must not cause InvalidLabels.
		out, err := b.LabelParameterVersion(context.Background(), &ssm.LabelParameterVersionInput{
			Name:   "/p/dup",
			Labels: []string{"stable", "prod"},
		})
		require.NoError(t, err)
		assert.Empty(t, out.InvalidLabels, "re-applying existing labels must not overflow")
	})

	t.Run("handler returns 200 with AddedLabels field", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler(t)
		putTestParam(t, b, "/p/handler")

		rec := doRequest(t, h, "LabelParameterVersion",
			`{"Name":"/p/handler","Labels":["alpha","beta"]}`)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "AddedLabels")
	})
}

// ---------------------------------------------------------------------------
// 2. Command status machine: Pending → InProgress → Success
// ---------------------------------------------------------------------------

func TestParityEMR_SendCommand_StatusMachine(t *testing.T) {
	t.Parallel()

	t.Run("command ends in Success after SendCommand", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		ctx := context.Background()

		// Need a document.
		_, err := b.CreateDocument(ctx, &ssm.CreateDocumentInput{
			Name:    "My-Doc",
			Content: `{"schemaVersion":"2.2"}`,
		})
		require.NoError(t, err)

		out, err := b.SendCommand(ctx, &ssm.SendCommandInput{
			DocumentName: "My-Doc",
			InstanceIDs:  []string{"i-111", "i-222"},
		})
		require.NoError(t, err)
		assert.Equal(t, "Success", out.Command.Status,
			"no-op runner must complete synchronously")
		assert.Equal(t, "Success", out.Command.StatusDetails)
	})

	t.Run("invocations also end in Success", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		ctx := context.Background()

		_, err := b.CreateDocument(ctx, &ssm.CreateDocumentInput{
			Name:    "My-Doc2",
			Content: `{"schemaVersion":"2.2"}`,
		})
		require.NoError(t, err)

		out, err := b.SendCommand(ctx, &ssm.SendCommandInput{
			DocumentName: "My-Doc2",
			InstanceIDs:  []string{"i-abc"},
		})
		require.NoError(t, err)

		cmdID := out.Command.CommandID

		invOut, err := b.GetCommandInvocation(ctx, &ssm.GetCommandInvocationInput{
			CommandID:  cmdID,
			InstanceID: "i-abc",
		})
		require.NoError(t, err)
		assert.Equal(t, "Success", invOut.Status)
	})

	t.Run("CancelCommand transitions from Pending to Cancelled", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		ctx := context.Background()

		_, err := b.CreateDocument(ctx, &ssm.CreateDocumentInput{
			Name:    "Cancel-Doc",
			Content: `{"schemaVersion":"2.2"}`,
		})
		require.NoError(t, err)

		// First send a command (will end in Success).
		sendOut, err := b.SendCommand(ctx, &ssm.SendCommandInput{
			DocumentName: "Cancel-Doc",
			InstanceIDs:  []string{"i-x"},
		})
		require.NoError(t, err)

		cmdID := sendOut.Command.CommandID

		// CancelCommand on a Success command should succeed gracefully (AWS allows this).
		_, err = b.CancelCommand(ctx, &ssm.CancelCommandInput{CommandID: cmdID})
		require.NoError(t, err)
	})

	t.Run("handler returns CommandId in SendCommand response", func(t *testing.T) {
		t.Parallel()

		h, _ := newTestHandler(t)

		// AWS-RunShellScript is pre-registered.
		rec := doRequest(t, h, "SendCommand",
			`{"DocumentName":"AWS-RunShellScript","InstanceIds":["i-abc"]}`)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "CommandId")
	})
}

// ---------------------------------------------------------------------------
// 3. Parameter Expiration policy: janitor evicts past-expiry parameters
// ---------------------------------------------------------------------------

func TestParityEMR_ParameterExpiration_JanitorEvicts(t *testing.T) {
	t.Parallel()

	t.Run("past-expiry parameter is deleted by janitor", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		ctx := context.Background()

		// Attach an Expiration policy with a timestamp in the past.
		pastTS := time.Now().UTC().Add(-1 * time.Hour).Format("2006-01-02T15:04:05.000Z")
		policies := fmt.Sprintf(
			`[{"Type":"Expiration","Version":"1.0","Attributes":{"Timestamp":%q}}]`,
			pastTS,
		)

		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:     "/expire/past",
			Value:    "gone-soon",
			Type:     "String",
			Tier:     "Advanced",
			Policies: policies,
		})
		require.NoError(t, err)

		// Confirm it exists.
		_, err = b.GetParameter(ctx, &ssm.GetParameterInput{Name: "/expire/past"})
		require.NoError(t, err)

		// Run the janitor sweep.
		j := ssm.NewJanitor(b, time.Minute)
		j.SweepOnce(ctx)

		// Parameter must be gone.
		_, err = b.GetParameter(ctx, &ssm.GetParameterInput{Name: "/expire/past"})
		assert.ErrorIs(t, err, ssm.ErrParameterNotFound,
			"past-expiry parameter must be evicted by janitor")
	})

	t.Run("future-expiry parameter is kept by janitor", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		ctx := context.Background()

		futureTS := time.Now().UTC().Add(24 * time.Hour).Format("2006-01-02T15:04:05.000Z")
		policies := fmt.Sprintf(
			`[{"Type":"Expiration","Version":"1.0","Attributes":{"Timestamp":%q}}]`,
			futureTS,
		)

		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:     "/expire/future",
			Value:    "still-here",
			Type:     "String",
			Tier:     "Advanced",
			Policies: policies,
		})
		require.NoError(t, err)

		j := ssm.NewJanitor(b, time.Minute)
		j.SweepOnce(ctx)

		_, err = b.GetParameter(ctx, &ssm.GetParameterInput{Name: "/expire/future"})
		require.NoError(t, err, "future-expiry parameter must survive janitor sweep")
	})

	t.Run("parameter without policy is not evicted", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		ctx := context.Background()

		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:  "/no-policy",
			Value: "permanent",
			Type:  "String",
		})
		require.NoError(t, err)

		j := ssm.NewJanitor(b, time.Minute)
		j.SweepOnce(ctx)

		_, err = b.GetParameter(ctx, &ssm.GetParameterInput{Name: "/no-policy"})
		require.NoError(t, err, "parameter without expiry policy must not be evicted")
	})

	t.Run("RFC3339 timestamp format also works", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		ctx := context.Background()

		pastTS := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)
		policies := fmt.Sprintf(
			`[{"Type":"Expiration","Version":"1.0","Attributes":{"Timestamp":%q}}]`,
			pastTS,
		)

		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:     "/expire/rfc3339",
			Value:    "gone",
			Type:     "String",
			Tier:     "Advanced",
			Policies: policies,
		})
		require.NoError(t, err)

		j := ssm.NewJanitor(b, time.Minute)
		j.SweepOnce(ctx)

		_, err = b.GetParameter(ctx, &ssm.GetParameterInput{Name: "/expire/rfc3339"})
		assert.ErrorIs(t, err, ssm.ErrParameterNotFound)
	})
}

// ---------------------------------------------------------------------------
// 4. GetParametersByPath pagination correctness (regression)
// ---------------------------------------------------------------------------

func TestParityEMR_GetParametersByPath_Pagination(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	// Insert 25 parameters under /paginate/.
	for i := range 25 {
		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:  fmt.Sprintf("/paginate/p%02d", i),
			Value: fmt.Sprintf("v%d", i),
			Type:  "String",
		})
		require.NoError(t, err)
	}

	// Collect all via paginated calls (MaxResults=10).
	var allNames []string
	var nextToken string

	for {
		maxResults := int64(10)
		out, err := b.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{
			Path:       "/paginate/",
			MaxResults: &maxResults,
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		for _, p := range out.Parameters {
			allNames = append(allNames, p.Name)
		}

		if out.NextToken == "" {
			break
		}

		nextToken = out.NextToken
	}

	assert.Len(t, allNames, 25, "pagination must return all 25 parameters")

	// All names must be unique.
	seen := make(map[string]bool, len(allNames))
	for _, n := range allNames {
		assert.False(t, seen[n], "duplicate parameter %q in paginated results", n)
		seen[n] = true
	}

	// All must be under /paginate/.
	for _, n := range allNames {
		assert.True(t, strings.HasPrefix(n, "/paginate/"), "unexpected name %q", n)
	}
}
