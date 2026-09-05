package workspaces_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wssdk "github.com/aws/aws-sdk-go-v2/service/workspaces"
	"github.com/stretchr/testify/require"
)

func TestConnectClientAddInCRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name       string
		addInName  string
		resourceID string
		url        string
	}{
		{
			name:       "basic addon",
			addInName:  "MyAddIn",
			resourceID: "d-123",
			url:        "https://example.com",
		},
		{name: "second addon", addInName: "AddIn2", resourceID: "d-456", url: "https://other.com"},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			// Create
			rec := doTargetRequest(t, h, "CreateConnectClientAddIn", map[string]any{
				"Name":       tc.addInName,
				"ResourceId": tc.resourceID,
				"URL":        tc.url,
			})
			if rec.Code != http.StatusOK {
				t.Fatalf("create: expected 200, got %d: %s", rec.Code, rec.Body)
			}

			var createOut map[string]string
			decodeJSON(t, rec.Body.Bytes(), &createOut)

			addInID := createOut["AddInId"]
			if addInID == "" {
				t.Fatal("expected non-empty AddInId")
			}

			// Describe
			rec2 := doTargetRequest(t, h, "DescribeConnectClientAddIns", map[string]any{
				"ResourceId": tc.resourceID,
			})
			if rec2.Code != http.StatusOK {
				t.Fatalf("describe: expected 200, got %d", rec2.Code)
			}

			var descOut struct {
				AddIns []map[string]string `json:"AddIns"`
			}
			decodeJSON(t, rec2.Body.Bytes(), &descOut)

			if len(descOut.AddIns) != 1 {
				t.Fatalf("expected 1 add-in, got %d", len(descOut.AddIns))
			}

			// Update
			rec3 := doTargetRequest(t, h, "UpdateConnectClientAddIn", map[string]any{
				"AddInId":    addInID,
				"ResourceId": tc.resourceID,
				"Name":       "Updated",
				"URL":        "https://updated.example.com",
			})
			if rec3.Code != http.StatusOK {
				t.Fatalf("update: expected 200, got %d", rec3.Code)
			}

			// Delete
			rec4 := doTargetRequest(t, h, "DeleteConnectClientAddIn", map[string]any{
				"AddInId":    addInID,
				"ResourceId": tc.resourceID,
			})
			if rec4.Code != http.StatusOK {
				t.Fatalf("delete: expected 200, got %d", rec4.Code)
			}

			// Describe after delete
			rec5 := doTargetRequest(t, h, "DescribeConnectClientAddIns", map[string]any{
				"ResourceId": tc.resourceID,
			})
			var afterDel struct {
				AddIns []any `json:"AddIns"`
			}
			decodeJSON(t, rec5.Body.Bytes(), &afterDel)

			if len(afterDel.AddIns) != 0 {
				t.Fatalf("expected 0 add-ins after delete, got %d", len(afterDel.AddIns))
			}
		})
	}
}

// TestDescribeConnectClientAddIns_Pagination proves the op pages through
// every add-in exactly once instead of returning them all on a single page
// with no cursor.
func TestDescribeConnectClientAddIns_Pagination(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	const resourceID = "d-00000000"

	names := []string{"addin-a", "addin-b", "addin-c"}
	for _, n := range names {
		_, err := client.CreateConnectClientAddIn(ctx, &wssdk.CreateConnectClientAddInInput{
			Name:       aws.String(n),
			ResourceId: aws.String(resourceID),
			URL:        aws.String("https://example.com/" + n),
		})
		require.NoError(t, err)
	}

	page1, err := client.DescribeConnectClientAddIns(ctx, &wssdk.DescribeConnectClientAddInsInput{
		ResourceId: aws.String(resourceID),
		MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, page1.AddIns, 2)
	require.NotNil(t, page1.NextToken, "first page must return a cursor when more add-ins remain")

	page2, err := client.DescribeConnectClientAddIns(ctx, &wssdk.DescribeConnectClientAddInsInput{
		ResourceId: aws.String(resourceID),
		MaxResults: aws.Int32(2),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.AddIns, 1)
	require.Empty(t, aws.ToString(page2.NextToken))

	seen := map[string]bool{}
	for _, a := range page1.AddIns {
		seen[aws.ToString(a.AddInId)] = true
	}

	for _, a := range page2.AddIns {
		id := aws.ToString(a.AddInId)
		require.False(t, seen[id], "add-in %s returned on both pages", id)
		seen[id] = true
	}

	require.Len(t, seen, len(names))
}
