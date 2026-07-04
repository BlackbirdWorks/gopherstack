package efs_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// doParityREST fires an HTTP request at the EFS handler and returns the recorder.
func doParityREST(
	t *testing.T,
	h *efs.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

// parityCreateFS creates a file system and returns its FileSystemId.
func parityCreateFS(t *testing.T, h *efs.Handler, token string) string {
	t.Helper()

	rec := doParityREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": token,
	})
	require.Equal(t, http.StatusCreated, rec.Code, "CreateFileSystem: %s", rec.Body.String())

	var out struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out.FileSystemID
}

// newParityEFSHandler returns a fresh Handler for parity tests.
func newParityEFSHandler(t *testing.T) *efs.Handler {
	t.Helper()

	return efs.NewHandler(efs.NewInMemoryBackend("123456789012", "us-east-1"))
}

// --- 1. CreateMountTarget populates VpcID, AvailabilityZoneName, AvailabilityZoneID ---

func TestParityEMR_CreateMountTarget_PopulatesVpcAndAZ(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		subnetID string
	}{
		{name: "standard subnet format", subnetID: "subnet-abc12345"},
		{name: "non-standard subnet", subnetID: "custom-subnet-01"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newParityEFSHandler(t)
			fsID := parityCreateFS(t, h, "parity-mt-vpc-"+tc.subnetID)

			rec := doParityREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
				"FileSystemId": fsID,
				"SubnetId":     tc.subnetID,
			})
			require.Equal(t, http.StatusOK, rec.Code, "CreateMountTarget: %s", rec.Body.String())

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			vpcID, _ := out["VpcId"].(string)
			azName, _ := out["AvailabilityZoneName"].(string)
			azID, _ := out["AvailabilityZoneId"].(string)

			assert.True(t, len(vpcID) > 4 && vpcID[:4] == "vpc-",
				"VpcId %q should start with 'vpc-'", vpcID)
			assert.NotEmpty(t, azName, "AvailabilityZoneName must be non-empty")
			assert.NotEmpty(t, azID, "AvailabilityZoneId must be non-empty")
			assert.True(t, len(azName) > 0 && azName[len(azName)-1] >= 'a' && azName[len(azName)-1] <= 'z',
				"AvailabilityZoneName %q should end with a letter", azName)
		})
	}
}

// --- 2. VpcID is stable per subnet ---

func TestParityEMR_CreateMountTarget_VpcIDStablePerSubnet(t *testing.T) {
	t.Parallel()

	h := newParityEFSHandler(t)
	fsID1 := parityCreateFS(t, h, "parity-vpc-stable-1")
	fsID2 := parityCreateFS(t, h, "parity-vpc-stable-2")

	const subnetID = "subnet-deadbeef"

	rec1 := doParityREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID1,
		"SubnetId":     subnetID,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var mt1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &mt1))

	rec2 := doParityREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID2,
		"SubnetId":     subnetID,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var mt2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &mt2))

	assert.Equal(t, mt1["VpcId"], mt2["VpcId"],
		"same subnet should yield same VpcId across different file systems")
}

// --- 3. One-Zone FS: mount target inherits AZ from file system ---

func TestParityEMR_CreateMountTarget_OneZone_InheritsAZ(t *testing.T) {
	t.Parallel()

	h := newParityEFSHandler(t)

	const az = "us-east-1c"

	rec := doParityREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken":        "parity-onezone-fs",
		"AvailabilityZoneName": az,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var fsOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fsOut))
	fsID := fsOut["FileSystemId"].(string)

	mtRec := doParityREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID,
		"SubnetId":     "subnet-cafebabe",
	})
	require.Equal(t, http.StatusOK, mtRec.Code)

	var mt map[string]any
	require.NoError(t, json.Unmarshal(mtRec.Body.Bytes(), &mt))

	assert.Equal(t, az, mt["AvailabilityZoneName"],
		"mount target should inherit FS AvailabilityZoneName for One Zone storage class")
}

// --- 4. File system lifecycle: creating → available ---

func TestParityEMR_FileSystem_CreatingToAvailableLifecycle(t *testing.T) {
	t.Parallel()

	b := efs.NewInMemoryBackend("123456789012", "us-east-1")
	efs.SetFSActivationDelay(b, 80*time.Millisecond)
	h := efs.NewHandler(b)

	rec := doParityREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "parity-lifecycle-token",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Equal(t, "creating", out["LifeCycleState"],
		"newly created FS should be in 'creating' state immediately after CreateFileSystem")

	fsID := out["FileSystemId"].(string)

	require.Eventually(t, func() bool {
		descRec := doParityREST(t, h, http.MethodGet,
			"/2015-02-01/file-systems?FileSystemId="+fsID, nil)
		if descRec.Code != http.StatusOK {
			return false
		}

		var descOut struct {
			FileSystems []struct {
				LifeCycleState string `json:"LifeCycleState"`
			} `json:"FileSystems"`
		}
		if err := json.Unmarshal(descRec.Body.Bytes(), &descOut); err != nil || len(descOut.FileSystems) == 0 {
			return false
		}

		return descOut.FileSystems[0].LifeCycleState == "available"
	}, 500*time.Millisecond, 20*time.Millisecond,
		"FS should transition to 'available' within 500ms")
}

// --- 5. CreationToken idempotency: identical → 200, different params → 409 ---

func TestParityEMR_CreateFileSystem_CreationTokenIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		firstBody  map[string]any
		secondBody map[string]any
		name       string
		wantCode   int
	}{
		{
			name:       "identical params → 200 idempotent success",
			firstBody:  map[string]any{"CreationToken": "idem-1", "PerformanceMode": "generalPurpose"},
			secondBody: map[string]any{"CreationToken": "idem-1", "PerformanceMode": "generalPurpose"},
			wantCode:   http.StatusOK,
		},
		{
			name:      "different params → 409 conflict",
			firstBody: map[string]any{"CreationToken": "idem-2", "ThroughputMode": "bursting"},
			secondBody: map[string]any{
				"CreationToken":                "idem-2",
				"ThroughputMode":               "provisioned",
				"ProvisionedThroughputInMibps": 128,
			},
			wantCode: http.StatusConflict,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newParityEFSHandler(t)

			rec1 := doParityREST(t, h, http.MethodPost, "/2015-02-01/file-systems", tc.firstBody)
			require.Equal(t, http.StatusCreated, rec1.Code, "first create: %s", rec1.Body.String())

			rec2 := doParityREST(t, h, http.MethodPost, "/2015-02-01/file-systems", tc.secondBody)
			assert.Equal(t, tc.wantCode, rec2.Code)
		})
	}
}

// --- 6. DeleteFileSystem blocked by mount targets or access points ---

func TestParityEMR_DeleteFileSystem_BlockedByDependencies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupMount bool
		setupAP    bool
		wantCode   int
	}{
		{name: "blocked by mount target", setupMount: true, wantCode: http.StatusConflict},
		{name: "blocked by access point", setupAP: true, wantCode: http.StatusConflict},
		{name: "succeeds when empty", wantCode: http.StatusNoContent},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newParityEFSHandler(t)
			fsID := parityCreateFS(t, h, "parity-delete-"+tc.name)

			if tc.setupMount {
				rec := doParityREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId": fsID,
					"SubnetId":     "subnet-aabbccdd",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			if tc.setupAP {
				rec := doParityREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
					"FileSystemId": fsID,
					"ClientToken":  "tok-ap-block",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			delRec := doParityREST(t, h, http.MethodDelete, "/2015-02-01/file-systems/"+fsID, nil)
			assert.Equal(t, tc.wantCode, delRec.Code)
		})
	}
}

// --- 7. CreateMountTarget O(1) subnet conflict detection ---

func TestParityEMR_CreateMountTarget_SubnetConflict(t *testing.T) {
	t.Parallel()

	h := newParityEFSHandler(t)
	fsID := parityCreateFS(t, h, "parity-subnet-conflict")

	const subnetID = "subnet-11223344"

	rec1 := doParityREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID,
		"SubnetId":     subnetID,
	})
	require.Equal(t, http.StatusOK, rec1.Code, "first mount target should succeed")

	rec2 := doParityREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID,
		"SubnetId":     subnetID,
	})
	assert.Equal(t, http.StatusConflict, rec2.Code, "duplicate subnet in same FS should conflict")

	rec3 := doParityREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID,
		"SubnetId":     "subnet-different0",
	})
	assert.Equal(t, http.StatusOK, rec3.Code, "different subnet in same FS should succeed")
}

// --- 8. DescribeFileSystems pagination with marker ---

func TestParityEMR_DescribeFileSystems_PaginationMarker(t *testing.T) {
	t.Parallel()

	h := newParityEFSHandler(t)

	// paginate semantics: marker = first item of next page, skipped on resume.
	// 10 items, pageSize=3: page1=[0..2] marker=items[3],
	// page2=[4..6] marker=items[7], page3=[8..9] no marker.
	const total = 10
	for i := range total {
		parityCreateFS(t, h, fmt.Sprintf("parity-page-token-%02d", i))
	}

	rec1 := doParityREST(t, h, http.MethodGet, "/2015-02-01/file-systems?MaxItems=3", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	type fsPage struct {
		NextMarker  string `json:"NextMarker"`
		FileSystems []struct {
			FileSystemID string `json:"FileSystemId"`
		} `json:"FileSystems"`
	}

	var page1 fsPage
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &page1))
	require.Len(t, page1.FileSystems, 3)
	require.NotEmpty(t, page1.NextMarker)

	rec2 := doParityREST(t, h, http.MethodGet,
		"/2015-02-01/file-systems?MaxItems=3&Marker="+page1.NextMarker, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 fsPage
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
	require.Len(t, page2.FileSystems, 3)
	require.NotEmpty(t, page2.NextMarker)

	rec3 := doParityREST(t, h, http.MethodGet,
		"/2015-02-01/file-systems?MaxItems=3&Marker="+page2.NextMarker, nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var page3 fsPage
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &page3))
	require.Len(t, page3.FileSystems, 2, "last page has items[8..9]")
	assert.Empty(t, page3.NextMarker, "no more pages after last item")

	badRec := doParityREST(t, h, http.MethodGet,
		"/2015-02-01/file-systems?Marker=nonexistent-marker-id", nil)
	assert.Equal(t, http.StatusBadRequest, badRec.Code, "invalid marker should return 400")
}

// --- 9. DescribeMountTargets pagination ---

func TestParityEMR_DescribeMountTargets_Pagination(t *testing.T) {
	t.Parallel()

	h := newParityEFSHandler(t)
	fsID := parityCreateFS(t, h, "parity-mt-page")

	const total = 5
	for i := range total {
		rec := doParityREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
			"FileSystemId": fsID,
			"SubnetId":     fmt.Sprintf("subnet-%08d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code, "mount target %d: %s", i, rec.Body.String())
	}

	rec1 := doParityREST(t, h, http.MethodGet,
		"/2015-02-01/mount-targets?FileSystemId="+fsID+"&MaxItems=3", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	type mtPage struct {
		NextMarker   string `json:"NextMarker"`
		MountTargets []struct {
			MountTargetID string `json:"MountTargetId"`
		} `json:"MountTargets"`
	}

	var pg1 mtPage
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &pg1))
	require.Len(t, pg1.MountTargets, 3)
	require.NotEmpty(t, pg1.NextMarker)

	rec2 := doParityREST(t, h, http.MethodGet,
		"/2015-02-01/mount-targets?FileSystemId="+fsID+"&MaxItems=3&Marker="+pg1.NextMarker, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var pg2 mtPage
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &pg2))
	// 5 items, pageSize=3: marker=items[3], page2 starts after skip → items[4] only.
	assert.Len(t, pg2.MountTargets, 1)
	assert.Empty(t, pg2.NextMarker)

	seen := make(map[string]bool)
	for _, mt := range pg1.MountTargets {
		seen[mt.MountTargetID] = true
	}

	for _, mt := range pg2.MountTargets {
		assert.False(t, seen[mt.MountTargetID], "mount target %s appears in both pages", mt.MountTargetID)
	}
}

// --- 10. DeleteMountTarget removes subnet index so same subnet can be re-used ---

func TestParityEMR_DeleteMountTarget_CleansSubnetIndex(t *testing.T) {
	t.Parallel()

	h := newParityEFSHandler(t)
	fsID := parityCreateFS(t, h, "parity-mt-idx-cleanup")

	const subnet = "subnet-deadcafe"

	rec1 := doParityREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID,
		"SubnetId":     subnet,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var mt1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &mt1))
	mtID := mt1["MountTargetId"].(string)

	delRec := doParityREST(t, h, http.MethodDelete, "/2015-02-01/mount-targets/"+mtID, nil)
	require.Equal(t, http.StatusNoContent, delRec.Code)

	rec2 := doParityREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID,
		"SubnetId":     subnet,
	})
	assert.Equal(t, http.StatusOK, rec2.Code,
		"re-create in same subnet after delete should succeed: %s", rec2.Body.String())
}

// --- 11. DeleteAccessPoint removes apByFS index so DeleteFileSystem can proceed ---

func TestParityEMR_DeleteAccessPoint_CleansAPIndex(t *testing.T) {
	t.Parallel()

	h := newParityEFSHandler(t)
	fsID := parityCreateFS(t, h, "parity-ap-idx-cleanup")

	apRec := doParityREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
		"FileSystemId": fsID,
		"ClientToken":  "tok-cleanup-1",
	})
	require.Equal(t, http.StatusOK, apRec.Code)

	var apOut map[string]any
	require.NoError(t, json.Unmarshal(apRec.Body.Bytes(), &apOut))
	apID := apOut["AccessPointId"].(string)

	blockRec := doParityREST(t, h, http.MethodDelete, "/2015-02-01/file-systems/"+fsID, nil)
	assert.Equal(t, http.StatusConflict, blockRec.Code, "delete blocked while AP exists")

	delAP := doParityREST(t, h, http.MethodDelete, "/2015-02-01/access-points/"+apID, nil)
	require.Equal(t, http.StatusNoContent, delAP.Code)

	delFS := doParityREST(t, h, http.MethodDelete, "/2015-02-01/file-systems/"+fsID, nil)
	assert.Equal(t, http.StatusNoContent, delFS.Code,
		"delete should succeed after AP removed: %s", delFS.Body.String())
}
