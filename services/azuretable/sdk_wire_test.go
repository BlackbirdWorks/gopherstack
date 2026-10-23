package azuretable_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

func newSDKService(t *testing.T) (*aztables.ServiceClient, *azuretable.InMemoryBackend) {
	t.Helper()

	backend := azuretable.NewInMemoryBackend()
	e := echo.New()
	e.Any("/*", azuretable.NewHandler(backend).Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	svc, err := aztables.NewServiceClientWithNoCredential(srv.URL+"/"+testAccount,
		&aztables.ClientOptions{Retry: policy.RetryOptions{MaxRetries: -1}})
	require.NoError(t, err)

	return svc, backend
}

func sdkEntity(t *testing.T, pk, rk string, extra map[string]any) []byte {
	t.Helper()

	m := map[string]any{"PartitionKey": pk, "RowKey": rk}
	maps.Copy(m, extra)

	b, err := json.Marshal(m)
	require.NoError(t, err)

	return b
}

func TestTableNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		table string
		want  int
	}{
		{name: "valid", table: "MyTable1", want: http.StatusCreated},
		{name: "too short", table: "ab", want: http.StatusBadRequest},
		{name: "too long", table: strings.Repeat("a", 64), want: http.StatusBadRequest},
		{name: "leading digit", table: "1table", want: http.StatusBadRequest},
		{name: "hyphen", table: "my-table", want: http.StatusBadRequest},
		{name: "reserved", table: "tables", want: http.StatusBadRequest},
		{name: "reserved mixed case", table: "Tables", want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := []byte(fmt.Sprintf(`{"TableName":%q}`, tt.table))
			rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", body)

			assert.Equal(t, tt.want, rec.Code)

			if tt.want == http.StatusBadRequest {
				assert.Equal(t, "InvalidResourceName", rec.Header().Get("X-Ms-Error-Code"))
			}
		})
	}
}

func TestQueryEntitiesContinuationSDK(t *testing.T) {
	t.Parallel()

	svc, _ := newSDKService(t)
	ctx := t.Context()
	_, err := svc.CreateTable(ctx, "paged", nil)
	require.NoError(t, err)

	client := svc.NewClient("paged")

	const total = 7

	for i := range total {
		_, err = client.AddEntity(ctx, sdkEntity(t, "p", fmt.Sprintf("r%02d", i), nil), nil)
		require.NoError(t, err)
	}

	pager := client.NewListEntitiesPager(&aztables.ListEntitiesOptions{Top: new(int32(3))})

	var sizes []int

	seen := 0

	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)

		sizes = append(sizes, len(page.Entities))
		seen += len(page.Entities)
	}

	assert.Equal(t, []int{3, 3, 1}, sizes)
	assert.Equal(t, total, seen)
}

func TestQueryEntitiesPageCap(t *testing.T) {
	t.Parallel()

	b := azuretable.NewInMemoryBackend()
	require.NoError(t, b.CreateTable("big"))

	for i := range 1005 {
		_, err := b.InsertEntity("big", "p", fmt.Sprintf("r%05d", i), nil)
		require.NoError(t, err)
	}

	page, err := b.QueryEntitiesPage("big", nil, 0, "", "")
	require.NoError(t, err)
	assert.Len(t, page.Entities, 1000)
	assert.True(t, page.HasMore)
	assert.Equal(t, "r01000", page.NextRowKey)

	rest, err := b.QueryEntitiesPage("big", nil, 0, page.NextPartitionKey, page.NextRowKey)
	require.NoError(t, err)
	assert.Len(t, rest.Entities, 5)
	assert.False(t, rest.HasMore)
}

func TestListTablesContinuationSDK(t *testing.T) {
	t.Parallel()

	svc, _ := newSDKService(t)
	ctx := t.Context()

	for _, n := range []string{"tablea", "tableb", "tablec", "tabled", "tablee"} {
		_, err := svc.CreateTable(ctx, n, nil)
		require.NoError(t, err)
	}

	pager := svc.NewListTablesPager(&aztables.ListTablesOptions{Top: new(int32(2))})

	var names []string

	pages := 0

	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)

		pages++

		for _, tbl := range page.Tables {
			names = append(names, *tbl.Name)
		}
	}

	assert.Equal(t, 3, pages)
	assert.Equal(t, []string{"tablea", "tableb", "tablec", "tabled", "tablee"}, names)
}

func TestEdmDoubleAnnotation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accept     string
		wantInBody string
		wantAnnot  bool
	}{
		{name: "minimal", accept: "application/json;odata=minimalmetadata", wantAnnot: true, wantInBody: `"D":5`},
		{name: "full", accept: "application/json;odata=fullmetadata", wantAnnot: true, wantInBody: `"D":5`},
		{name: "none", accept: "application/json;odata=nometadata", wantAnnot: false, wantInBody: `"D":5`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"dbl"}`))
			doRequest(t, h, http.MethodPost, "/"+testAccount+"/dbl", sdkEntity(t, "p", "r", map[string]any{
				"D": 5.0, "D@odata.type": "Edm.Double", "F": 1.5, "N": 7,
			}))

			req := httptest.NewRequest(http.MethodGet, "/"+testAccount+"/dbl(PartitionKey='p',RowKey='r')", http.NoBody)
			req.Header.Set("Accept", tt.accept)

			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

			body := rec.Body.String()
			assert.Contains(t, body, tt.wantInBody)
			assert.Equal(t, tt.wantAnnot, strings.Contains(body, `"D@odata.type":"Edm.Double"`))
			assert.NotContains(t, body, `"F@odata.type"`)
			assert.NotContains(t, body, `"N@odata.type"`)
		})
	}
}

func TestEdmDoubleRoundTripSDK(t *testing.T) {
	t.Parallel()

	svc, _ := newSDKService(t)
	ctx := t.Context()
	_, err := svc.CreateTable(ctx, "dbltbl", nil)
	require.NoError(t, err)

	client := svc.NewClient("dbltbl")
	ent := aztables.EDMEntity{
		PartitionKey: "p", RowKey: "r",
		Properties: map[string]any{"D": 5.0},
	}
	raw, err := json.Marshal(ent)
	require.NoError(t, err)
	_, err = client.AddEntity(ctx, raw, nil)
	require.NoError(t, err)

	got, err := client.GetEntity(ctx, "p", "r", nil)
	require.NoError(t, err)

	var back aztables.EDMEntity
	require.NoError(t, json.Unmarshal(got.Value, &back))
	assert.InDelta(t, 5.0, back.Properties["D"], 0)
}

func TestTransactionSDK(t *testing.T) {
	t.Parallel()

	svc, backend := newSDKService(t)
	ctx := t.Context()
	_, err := svc.CreateTable(ctx, "batchtbl", nil)
	require.NoError(t, err)

	client := svc.NewClient("batchtbl")

	_, err = client.SubmitTransaction(ctx, []aztables.TransactionAction{
		{ActionType: aztables.TransactionTypeAdd, Entity: sdkEntity(t, "p", "a", map[string]any{"V": "1"})},
		{ActionType: aztables.TransactionTypeAdd, Entity: sdkEntity(t, "p", "b", map[string]any{"V": "1"})},
		{ActionType: aztables.TransactionTypeInsertReplace, Entity: sdkEntity(t, "p", "c", nil)},
	}, nil)
	require.NoError(t, err)

	_, err = client.SubmitTransaction(ctx, []aztables.TransactionAction{
		{ActionType: aztables.TransactionTypeAdd, Entity: sdkEntity(t, "p", "d", nil)},
		{ActionType: aztables.TransactionTypeAdd, Entity: sdkEntity(t, "p", "a", nil)},
	}, nil)
	require.Error(t, err, "duplicate insert fails the transaction")

	_, getErr := backend.GetEntity("batchtbl", "p", "d")
	require.ErrorIs(t, getErr, azuretable.ErrEntityNotFound, "failed transaction rolls back")

	_, err = client.SubmitTransaction(ctx, []aztables.TransactionAction{
		{ActionType: aztables.TransactionTypeDelete, Entity: sdkEntity(t, "p", "a", nil)},
		{ActionType: aztables.TransactionTypeUpdateMerge, Entity: sdkEntity(t, "p", "b", map[string]any{"W": "2"})},
	}, nil)
	require.NoError(t, err)

	_, getErr = backend.GetEntity("batchtbl", "p", "a")
	require.ErrorIs(t, getErr, azuretable.ErrEntityNotFound)

	b, getErr := backend.GetEntity("batchtbl", "p", "b")
	require.NoError(t, getErr)
	assert.Contains(t, b.Properties, "V")
	assert.Contains(t, b.Properties, "W")

	_, err = client.SubmitTransaction(ctx, []aztables.TransactionAction{
		{ActionType: aztables.TransactionTypeAdd, Entity: sdkEntity(t, "p1", "x", nil)},
		{ActionType: aztables.TransactionTypeAdd, Entity: sdkEntity(t, "p2", "y", nil)},
	}, nil)
	require.Error(t, err, "different partitions are rejected")
}

func TestTableACLSDKAndPersistence(t *testing.T) {
	t.Parallel()

	svc, backend := newSDKService(t)
	ctx := t.Context()
	_, err := svc.CreateTable(ctx, "acltbl", nil)
	require.NoError(t, err)

	client := svc.NewClient("acltbl")
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	expiry := start.Add(24 * time.Hour)

	_, err = client.SetAccessPolicy(ctx, &aztables.SetAccessPolicyOptions{TableACL: []*aztables.SignedIdentifier{
		{
			ID:           new("policy1"),
			AccessPolicy: &aztables.AccessPolicy{Start: &start, Expiry: &expiry, Permission: new("raud")},
		},
	}})
	require.NoError(t, err)

	got, err := client.GetAccessPolicy(ctx, nil)
	require.NoError(t, err)
	require.Len(t, got.SignedIdentifiers, 1)
	assert.Equal(t, "policy1", *got.SignedIdentifiers[0].ID)
	assert.Equal(t, "raud", *got.SignedIdentifiers[0].AccessPolicy.Permission)
	assert.True(t, got.SignedIdentifiers[0].AccessPolicy.Expiry.Equal(expiry))

	restored := azuretable.NewInMemoryBackend()
	require.NoError(t, restored.Restore(ctx, backend.Snapshot(ctx)))

	ids, err := restored.GetTableACL("acltbl")
	require.NoError(t, err)
	require.Len(t, ids, 1)
	assert.Equal(t, "policy1", ids[0].ID)

	_, err = svc.NewClient("missing").GetAccessPolicy(ctx, nil)
	require.Error(t, err)

	tooMany := make([]*aztables.SignedIdentifier, 6)
	for i := range tooMany {
		tooMany[i] = &aztables.SignedIdentifier{
			ID:           new(fmt.Sprintf("p%d", i)),
			AccessPolicy: &aztables.AccessPolicy{Start: &start, Expiry: &expiry, Permission: new("r")},
		}
	}

	_, err = client.SetAccessPolicy(ctx, &aztables.SetAccessPolicyOptions{TableACL: tooMany})
	require.Error(t, err)
}
