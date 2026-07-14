package memorydb_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// doRequestAsync is doRequest without the require.* assertions doRequest
// makes internally on marshal/handler failure -- require.FailNow from a
// non-test goroutine is unsafe (testifylint: go-require), so concurrent
// callers use this instead and assert on the result back in the main
// goroutine.
func doRequestAsync(h *memorydb.Handler, op string, body any) (*httptest.ResponseRecorder, error) {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonMemoryDB."+op)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err = h.Handler()(c); err != nil {
		return nil, err
	}

	return rec, nil
}

// -- Timestamp wire-shape regression tests ---------------------------------
//
// Real aws-sdk-go-v2/service/memorydb (awsjson1.1) serializes every TStamp
// shape as a JSON number of epoch seconds, never an RFC3339 string. A field
// emitted as a string where the SDK expects a number breaks client-side
// deserialization outright (see aws-sdk-go-v2's deserializers.go: "expected
// TStamp to be a JSON Number, got string instead").

func TestWireEpoch_DescribeEvents_DateIsNumber(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "epoch-cluster",
		"NodeType":    "db.t4g.small",
	})

	rec := doRequest(t, h, "DescribeEvents", map[string]any{
		"SourceName": "epoch-cluster",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	events, _ := resp["Events"].([]any)
	require.NotEmpty(t, events, "expected at least one event for the created cluster")

	ev, _ := events[0].(map[string]any)
	dateVal, ok := ev["Date"]
	require.True(t, ok, "Date field missing from event")

	_, isNumber := dateVal.(float64)
	assert.True(t, isNumber, "Event.Date must serialize as a JSON number (epoch seconds), got %T: %v", dateVal, dateVal)
}

func TestWireEpoch_DescribeEvents_StartTimeAcceptsEpochNumber(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "epoch-filter-cluster",
		"NodeType":    "db.t4g.small",
	})

	// Real clients send StartTime as a raw JSON number, not a quoted string.
	rec := doRequest(t, h, "DescribeEvents", map[string]any{
		"SourceName": "epoch-filter-cluster",
		"StartTime":  0,
	})
	require.Equal(t, http.StatusOK, rec.Code,
		"DescribeEvents must accept an epoch-seconds StartTime the way a real SDK client sends it")

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	events, _ := resp["Events"].([]any)
	assert.NotEmpty(t, events, "StartTime=0 (epoch start) should not filter out the cluster-creation event")

	// A StartTime far in the future must filter every event out.
	rec = doRequest(t, h, "DescribeEvents", map[string]any{
		"SourceName": "epoch-filter-cluster",
		"StartTime":  4102444800, // 2100-01-01T00:00:00Z
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp = nil
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	events, _ = resp["Events"].([]any)
	assert.Empty(t, events, "a future StartTime should filter out all past events")
}

func TestWireEpoch_DescribeServiceUpdates_DatesAreNumbers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DescribeServiceUpdates", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	updates, _ := resp["ServiceUpdates"].([]any)
	require.NotEmpty(t, updates)

	for _, raw := range updates {
		su, _ := raw.(map[string]any)

		_, releaseIsNumber := su["ReleaseDate"].(float64)
		assert.True(t, releaseIsNumber, "ServiceUpdate.ReleaseDate must be a JSON number, got %T", su["ReleaseDate"])

		_, autoIsNumber := su["AutoUpdateStartDate"].(float64)
		assert.True(t, autoIsNumber,
			"ServiceUpdate.AutoUpdateStartDate must be a JSON number, got %T", su["AutoUpdateStartDate"])
	}
}

func TestWireEpoch_ReservedNode_StartTimeIsNumber(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "PurchaseReservedNodesOffering", map[string]any{
		"ReservedNodesOfferingId": "aaa00000-1111-2222-3333-444444444444",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	rn, _ := resp["ReservedNode"].(map[string]any)
	require.NotNil(t, rn)

	_, isNumber := rn["StartTime"].(float64)
	assert.True(t, isNumber,
		"ReservedNode.StartTime must be a JSON number, got %T: %v", rn["StartTime"], rn["StartTime"])
}

func TestWireEpoch_Snapshot_CreationTimeIsNumber(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "snap-epoch-cluster",
		"NodeType":    "db.t4g.small",
	})

	rec := doRequest(t, h, "CreateSnapshot", map[string]any{
		"ClusterName":  "snap-epoch-cluster",
		"SnapshotName": "snap-epoch-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	snap, _ := resp["Snapshot"].(map[string]any)
	require.NotNil(t, snap)

	_, isNumber := snap["SnapshotCreationTime"].(float64)
	assert.True(t, isNumber,
		"Snapshot.SnapshotCreationTime must be a JSON number, got %T: %v",
		snap["SnapshotCreationTime"], snap["SnapshotCreationTime"])
}

// -- User Authentication.Type wire-shape regression ------------------------
//
// Real AWS's Authentication.Type output enum only defines "password",
// "no-password", and "iam" (aws-sdk-go-v2/service/memorydb/types.AuthenticationType).
// "no-password-required" is not a valid output value.

func TestWireAuthType_DefaultUserIsNoPassword(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateUser", map[string]any{
		"UserName":     "default-auth-user",
		"AccessString": "on ~* +@all",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	user, _ := resp["User"].(map[string]any)
	auth, _ := user["Authentication"].(map[string]any)
	assert.Equal(t, "no-password", auth["Type"],
		"a user created without AuthenticationMode.Type must report Authentication.Type=no-password on the wire")
}

func TestWireAuthType_UpdateUserValidatesType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateUser", map[string]any{
		"UserName":     "update-auth-user",
		"AccessString": "on ~* +@all",
		"AuthenticationMode": map[string]any{
			"Type":      "password",
			"Passwords": []string{"initialpassword1"},
		},
	})

	// UpdateUser must reject the same invalid combination CreateUser rejects:
	// iam auth with passwords supplied.
	rec := doRequest(t, h, "UpdateUser", map[string]any{
		"UserName": "update-auth-user",
		"AuthenticationMode": map[string]any{
			"Type":      "iam",
			"Passwords": []string{"shouldnotbeallowed"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"UpdateUser must reject AuthenticationMode.Type=iam combined with Passwords, like CreateUser does")

	// A valid switch to no-password-required aliases to "no-password" on the wire.
	rec = doRequest(t, h, "UpdateUser", map[string]any{
		"UserName": "update-auth-user",
		"AuthenticationMode": map[string]any{
			"Type": "no-password-required",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	user, _ := resp["User"].(map[string]any)
	auth, _ := user["Authentication"].(map[string]any)
	assert.Equal(t, "no-password", auth["Type"])
}

// -- Returned-pointer isolation concurrency smoke tests ---------------------
//
// Several backend Create*/Copy*/Export* methods used to return the live
// table entry instead of a defensive clone, unlike the rest of this file's
// read paths (see the "Deep-copy helpers" section in backend.go). The
// current wire converters (toSnapshotObject, toParameterGroupObject, ...)
// don't happen to serialize the affected Tags/Parameters maps, so this
// wasn't independently reproducible as a failing -race case today; fixed
// anyway for consistency with the established clone-on-return convention,
// since any converter that starts including those maps would otherwise
// reintroduce a live race against concurrent TagResource/Update* calls on
// the same resource. These tests exercise that concurrent traffic under
// -race as a smoke test/guard.

// doRequest calls require.* internally, which must only run on the test's
// own goroutine, so concurrent callers below use doRequestAsync and collect
// each response's status code over a channel to assert on afterward, in the
// main test goroutine.

func TestRace_CreateSnapshotVsTagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "race-cluster",
		"NodeType":    "db.t4g.small",
	})

	const n = 20

	statuses := make(chan int, n)

	var wg sync.WaitGroup

	for i := range n {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			rec, err := doRequestAsync(h, "CreateSnapshot", map[string]any{
				"ClusterName":  "race-cluster",
				"SnapshotName": snapshotNameForIndex(i),
			})
			if err != nil {
				statuses <- -1

				return
			}

			statuses <- rec.Code
		}(i)
	}

	wg.Wait()
	close(statuses)

	for status := range statuses {
		assert.Equal(t, http.StatusOK, status)
	}

	arns := collectSnapshotARNs(t, h)

	var tagWg sync.WaitGroup

	for _, snapshotARN := range arns {
		tagWg.Add(1)

		go func(snapshotARN string) {
			defer tagWg.Done()

			_, _ = doRequestAsync(h, "TagResource", map[string]any{
				"ResourceArn": snapshotARN,
				"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
			})
		}(snapshotARN)
	}

	tagWg.Wait()
}

func TestRace_CreateParameterGroupVsUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "race-pg",
		"Family":             "memorydb_redis7",
	})

	var wg sync.WaitGroup

	for range 20 {
		wg.Go(func() {
			_, _ = doRequestAsync(h, "UpdateParameterGroup", map[string]any{
				"ParameterGroupName": "race-pg",
				"ParameterNameValues": []map[string]string{
					{"ParameterName": "maxmemory-policy", "ParameterValue": "allkeys-lru"},
				},
			})
		})
	}

	wg.Wait()
}

func snapshotNameForIndex(i int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"

	return "race-snap-" + string(letters[i%len(letters)]) + string(letters[(i/len(letters))%len(letters)])
}

func collectSnapshotARNs(t *testing.T, h *memorydb.Handler) []string {
	t.Helper()

	rec := doRequest(t, h, "DescribeSnapshots", map[string]any{"ClusterName": "race-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	snaps, _ := resp["Snapshots"].([]any)

	arns := make([]string, 0, len(snaps))

	for _, raw := range snaps {
		s, _ := raw.(map[string]any)
		if a, ok := s["ARN"].(string); ok {
			arns = append(arns, a)
		}
	}

	return arns
}
