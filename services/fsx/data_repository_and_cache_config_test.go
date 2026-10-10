package fsx_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataRepositoryAssociation_S3Configuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		s3         map[string]any
		update     map[string]any
		name       string
		wantExport []any
		wantImport []any
		wantStatus int
	}{
		{
			name: "both policies",
			s3: map[string]any{
				"AutoExportPolicy": map[string]any{"Events": []string{"NEW", "CHANGED"}},
				"AutoImportPolicy": map[string]any{"Events": []string{"DELETED"}},
			},
			wantExport: []any{"NEW", "CHANGED"}, wantImport: []any{"DELETED"}, wantStatus: http.StatusOK,
		},
		{
			name: "update replaces only given policy",
			s3: map[string]any{
				"AutoExportPolicy": map[string]any{"Events": []string{"NEW"}},
				"AutoImportPolicy": map[string]any{"Events": []string{"NEW"}},
			},
			update:     map[string]any{"AutoImportPolicy": map[string]any{"Events": []string{"CHANGED"}}},
			wantExport: []any{"NEW"}, wantImport: []any{"CHANGED"}, wantStatus: http.StatusOK,
		},
		{
			name:       "bad event",
			s3:         map[string]any{"AutoExportPolicy": map[string]any{"Events": []string{"RENAMED"}}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doFSxRequest(t, h, "CreateDataRepositoryAssociation", map[string]any{
				"FileSystemId": createFS(t, h, "LUSTRE"), "FileSystemPath": "/a", "DataRepositoryPath": "s3://b",
				"S3": tt.s3,
			})
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus != http.StatusOK {
				return
			}

			id := responseID(t, rec.Body.Bytes(), "Association", "AssociationId")

			if tt.update != nil {
				up := doFSxRequest(t, h, "UpdateDataRepositoryAssociation", map[string]any{
					"AssociationId": id, "S3": tt.update,
				})
				require.Equal(t, http.StatusOK, up.Code, up.Body.String())
			}

			desc := doFSxRequest(
				t,
				h,
				"DescribeDataRepositoryAssociations",
				map[string]any{"AssociationIds": []string{id}},
			)
			require.Equal(t, http.StatusOK, desc.Code)

			var out struct {
				Associations []struct {
					S3 struct {
						AutoExportPolicy struct {
							Events []any `json:"Events"`
						} `json:"AutoExportPolicy"`
						AutoImportPolicy struct {
							Events []any `json:"Events"`
						} `json:"AutoImportPolicy"`
					} `json:"S3"`
				} `json:"Associations"`
			}

			require.NoError(t, json.Unmarshal(desc.Body.Bytes(), &out))
			require.Len(t, out.Associations, 1)
			assert.Equal(t, tt.wantExport, out.Associations[0].S3.AutoExportPolicy.Events)
			assert.Equal(t, tt.wantImport, out.Associations[0].S3.AutoImportPolicy.Events)
		})
	}
}

func TestDeleteDataRepositoryAssociation_DeleteDataInFileSystem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		flag any
		name string
	}{
		{name: "true", flag: true},
		{name: "false", flag: false},
		{name: "unset", flag: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			created := doFSxRequest(t, h, "CreateDataRepositoryAssociation", map[string]any{
				"FileSystemId": createFS(t, h, "LUSTRE"), "FileSystemPath": "/a", "DataRepositoryPath": "s3://b",
			})
			id := responseID(t, created.Body.Bytes(), "Association", "AssociationId")

			body := map[string]any{"AssociationId": id}
			if tt.flag != nil {
				body["DeleteDataInFileSystem"] = tt.flag
			}

			rec := doFSxRequest(t, h, "DeleteDataRepositoryAssociation", body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var out map[string]any

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, "DELETING", out["Lifecycle"])
			assert.Equal(t, tt.flag, out["DeleteDataInFileSystem"])
		})
	}
}

func TestCreateFileCache_AssociationsAndCopyTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		copyTags bool
		wantTags int
	}{
		{name: "copy tags", copyTags: true, wantTags: 1},
		{name: "no copy", copyTags: false, wantTags: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doFSxRequest(t, h, "CreateFileCache", map[string]any{
				"FileCacheType": "LUSTRE", "FileCacheTypeVersion": "2.12", "SubnetIds": []string{"subnet-1"},
				"StorageCapacity": 1200, "LustreConfiguration": fileCacheLustreConfigBody(),
				"SecurityGroupIds":                     []string{"sg-0123456789abcdef0"},
				"CopyTagsToDataRepositoryAssociations": tt.copyTags,
				"Tags":                                 []map[string]string{{"Key": "env", "Value": "prod"}},
				"DataRepositoryAssociations": []map[string]any{{
					"FileCachePath": "/ns1", "DataRepositoryPath": "nfs://10.0.0.1/export",
					"DataRepositorySubdirectories": []string{"a", "b"},
					"NFS":                          map[string]any{"Version": "NFS3", "DnsIps": []string{"10.0.0.2"}},
				}},
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var created struct {
				FileCache struct {
					CopyTags *bool    `json:"CopyTagsToDataRepositoryAssociations"`
					ID       string   `json:"FileCacheId"`
					DRAIDs   []string `json:"DataRepositoryAssociationIds"`
				} `json:"FileCache"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
			require.Len(t, created.FileCache.DRAIDs, 1)
			require.NotNil(t, created.FileCache.CopyTags)
			assert.Equal(t, tt.copyTags, *created.FileCache.CopyTags)

			desc := doFSxRequest(t, h, "DescribeDataRepositoryAssociations", map[string]any{
				"Filters": []map[string]any{{"Name": "file-cache-id", "Values": []string{created.FileCache.ID}}},
			})
			require.Equal(t, http.StatusOK, desc.Code, desc.Body.String())

			var assoc struct {
				Associations []struct {
					FileCacheID   string   `json:"FileCacheId"`
					FileCachePath string   `json:"FileCachePath"`
					FileSystemID  string   `json:"FileSystemId"`
					Subdirs       []string `json:"DataRepositorySubdirectories"`
					NFS           struct {
						Version string   `json:"Version"`
						DNSIPs  []string `json:"DnsIps"`
					} `json:"NFS"`
					Tags []any `json:"Tags"`
				} `json:"Associations"`
			}

			require.NoError(t, json.Unmarshal(desc.Body.Bytes(), &assoc))
			require.Len(t, assoc.Associations, 1)

			a := assoc.Associations[0]
			assert.Equal(t, created.FileCache.ID, a.FileCacheID)
			assert.Equal(t, "/ns1", a.FileCachePath)
			assert.Empty(t, a.FileSystemID)
			assert.Equal(t, []string{"a", "b"}, a.Subdirs)
			assert.Equal(t, "NFS3", a.NFS.Version)
			assert.Equal(t, []string{"10.0.0.2"}, a.NFS.DNSIPs)
			assert.Len(t, a.Tags, tt.wantTags)

			del := doFSxRequest(t, h, "DeleteFileCache", map[string]any{"FileCacheId": created.FileCache.ID})
			require.Equal(t, http.StatusOK, del.Code)

			after := doFSxRequest(t, h, "DescribeDataRepositoryAssociations", map[string]any{})
			assert.NotContains(t, after.Body.String(), created.FileCache.DRAIDs[0])
		})
	}
}

func TestCreateFileCache_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extra map[string]any
		name  string
	}{
		{name: "malformed security group", extra: map[string]any{"SecurityGroupIds": []string{"nope"}}},
		{name: "dra missing path", extra: map[string]any{
			"DataRepositoryAssociations": []map[string]any{{"FileCachePath": "/x"}},
		}},
		{name: "dra bad nfs version", extra: map[string]any{
			"DataRepositoryAssociations": []map[string]any{{
				"FileCachePath": "/x", "DataRepositoryPath": "nfs://h/e", "NFS": map[string]any{"Version": "NFS4"},
			}},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{
				"FileCacheType": "LUSTRE", "FileCacheTypeVersion": "2.12", "SubnetIds": []string{"subnet-1"},
				"StorageCapacity": 1200, "LustreConfiguration": fileCacheLustreConfigBody(),
			}
			maps.Copy(body, tt.extra)

			rec := doFSxRequest(t, newTestHandler(t), "CreateFileCache", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
}

func TestUpdateFileCache_WeeklyMaintenance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		value      string
		wantStatus int
	}{
		{name: "valid", value: "7:23:59", wantStatus: http.StatusOK},
		{name: "weekday zero", value: "0:01:00", wantStatus: http.StatusBadRequest},
		{name: "hour 24", value: "1:24:00", wantStatus: http.StatusBadRequest},
		{name: "minute 60", value: "1:01:60", wantStatus: http.StatusBadRequest},
		{name: "wrong shape", value: "Mon 05:00", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doFSxRequest(t, h, "UpdateFileCache", map[string]any{
				"FileCacheId":         createFileCache(t, h, "LUSTRE"),
				"LustreConfiguration": map[string]any{"WeeklyMaintenanceStartTime": tt.value},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

func TestUpdateFileSystem_StorageType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		storage    string
		wantStatus int
	}{
		{name: "ssd", storage: "SSD", wantStatus: http.StatusOK},
		{name: "hdd", storage: "HDD", wantStatus: http.StatusOK},
		{name: "unknown", storage: "TAPE", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doFSxRequest(t, h, "UpdateFileSystem", map[string]any{
				"FileSystemId": createFS(t, h, "LUSTRE"), "StorageType": tt.storage,
			})
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus == http.StatusOK {
				assert.Equal(t, tt.storage, decodeField(t, rec, "FileSystem")["StorageType"])
			}
		})
	}
}

func TestCreateFileSystem_SubnetIDs(t *testing.T) {
	t.Parallel()

	one := []string{"subnet-0123456789abcdef0"}
	two := []string{"subnet-0123456789abcdef0", "subnet-0123456789abcdef1"}

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{name: "missing", body: map[string]any{"FileSystemType": "LUSTRE"}, wantStatus: http.StatusBadRequest},
		{
			name: "empty", body: map[string]any{"FileSystemType": "LUSTRE", "SubnetIds": []string{}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "lustre two", body: map[string]any{"FileSystemType": "LUSTRE", "SubnetIds": two},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "windows single az two",
			body: map[string]any{
				"FileSystemType": "WINDOWS", "SubnetIds": two,
				"WindowsConfiguration": map[string]any{"ThroughputCapacity": 8},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "windows multi az one",
			body: map[string]any{
				"FileSystemType": "WINDOWS", "SubnetIds": one,
				"WindowsConfiguration": map[string]any{"ThroughputCapacity": 8, "DeploymentType": "MULTI_AZ_1"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "windows multi az two",
			body: map[string]any{
				"FileSystemType": "WINDOWS", "SubnetIds": two,
				"WindowsConfiguration": map[string]any{"ThroughputCapacity": 8, "DeploymentType": "MULTI_AZ_1"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "ontap multi az one",
			body: map[string]any{
				"FileSystemType": "ONTAP", "SubnetIds": one,
				"OntapConfiguration": map[string]any{"DeploymentType": "MULTI_AZ_1", "ThroughputCapacity": 128},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "lustre one",
			body:       map[string]any{"FileSystemType": "LUSTRE", "SubnetIds": one},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doFSxRequest(t, newTestHandler(t), "CreateFileSystem", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

func TestCreateFileSystemFromBackup_RequiresSubnetIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doFSxRequest(
		t,
		h,
		"CreateFileSystemFromBackup",
		map[string]any{"BackupId": createFSandBackup(t, h, "LUSTRE")},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
}
