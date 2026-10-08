package fsx_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fsxTarget = "AWSSimbaAPIService_v20180301."

func adBody() map[string]any {
	return map[string]any{
		"DomainName": "corp.example.com", "DnsIps": []string{"10.0.0.2"},
		"UserName": "svc", "Password": "hunter2", "FileSystemAdministratorsGroup": "Admins",
	}
}

func TestStorageVirtualMachine_ActiveDirectory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ad         map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "self managed",
			ad:         map[string]any{"NetBiosName": "SVM1", "SelfManagedActiveDirectoryConfiguration": adBody()},
			wantStatus: http.StatusOK,
		},
		{name: "netbios only", ad: map[string]any{"NetBiosName": "SVM1"}, wantStatus: http.StatusOK},
		{name: "missing netbios", ad: map[string]any{}, wantStatus: http.StatusBadRequest},
		{
			name: "missing dns", wantStatus: http.StatusBadRequest,
			ad: map[string]any{
				"NetBiosName": "SVM1",
				"SelfManagedActiveDirectoryConfiguration": map[string]any{"DomainName": "corp.example.com"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doFSxRequest(t, h, "CreateStorageVirtualMachine", map[string]any{
				"FileSystemId": createFS(t, h, "ONTAP"), "Name": "svm",
				"SvmAdminPassword": "Passw0rd!", "ActiveDirectoryConfiguration": tt.ad,
			})
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus != http.StatusOK {
				return
			}

			assert.NotContains(t, rec.Body.String(), "hunter2")
			assert.NotContains(t, rec.Body.String(), "Passw0rd!")

			svm := decodeField(t, rec, "StorageVirtualMachine")
			cfg, _ := svm["ActiveDirectoryConfiguration"].(map[string]any)
			require.NotNil(t, cfg)
			assert.Equal(t, "SVM1", cfg["NetBiosName"])

			sm, _ := cfg["SelfManagedActiveDirectoryConfiguration"].(map[string]any)
			if tt.ad["SelfManagedActiveDirectoryConfiguration"] == nil {
				assert.Nil(t, sm)

				return
			}

			assert.Equal(t, "corp.example.com", sm["DomainName"])
			assert.Equal(t, "svc", sm["UserName"])
			assert.Equal(t, "Admins", sm["FileSystemAdministratorsGroup"])
			assert.NotContains(t, sm, "Password")
		})
	}
}

func TestStorageVirtualMachine_UpdateActiveDirectory(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	create := doFSxRequest(t, h, "CreateStorageVirtualMachine", map[string]any{
		"FileSystemId": createFS(t, h, "ONTAP"), "Name": "svm",
		"ActiveDirectoryConfiguration": map[string]any{
			"NetBiosName": "SVM1", "SelfManagedActiveDirectoryConfiguration": adBody(),
		},
	})
	id := responseID(t, create.Body.Bytes(), "StorageVirtualMachine", "StorageVirtualMachineId")

	rec := doFSxRequest(t, h, "UpdateStorageVirtualMachine", map[string]any{
		"StorageVirtualMachineId": id,
		"ActiveDirectoryConfiguration": map[string]any{
			"SelfManagedActiveDirectoryConfiguration": map[string]any{
				"UserName": "rotated",
				"DnsIps":   []string{"10.0.0.9"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	cfg := decodeField(t, rec, "StorageVirtualMachine")["ActiveDirectoryConfiguration"].(map[string]any)
	assert.Equal(t, "SVM1", cfg["NetBiosName"])

	sm := cfg["SelfManagedActiveDirectoryConfiguration"].(map[string]any)
	assert.Equal(t, "rotated", sm["UserName"])
	assert.Equal(t, "corp.example.com", sm["DomainName"])
	assert.Equal(t, []any{"10.0.0.9"}, sm["DnsIps"])
}

func TestWindowsFileSystem_SelfManagedActiveDirectory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		windows    map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "self managed",
			windows:    map[string]any{"SelfManagedActiveDirectoryConfiguration": adBody()},
			wantStatus: http.StatusOK,
		},
		{
			name: "both directories", wantStatus: http.StatusBadRequest,
			windows: map[string]any{
				"ActiveDirectoryId": "d-1234567890", "SelfManagedActiveDirectoryConfiguration": adBody(),
			},
		},
		{
			name: "missing dns", wantStatus: http.StatusBadRequest,
			windows: map[string]any{
				"SelfManagedActiveDirectoryConfiguration": map[string]any{"DomainName": "corp.example.com"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.windows["ThroughputCapacity"] = 8
			rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{
				"FileSystemType": "WINDOWS", "SubnetIds": []string{"subnet-0123456789abcdef0"},
				"WindowsConfiguration": tt.windows,
			})
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus != http.StatusOK {
				return
			}

			assert.NotContains(t, rec.Body.String(), "hunter2")

			fs := decodeField(t, rec, "FileSystem")
			sm := fs["WindowsConfiguration"].(map[string]any)["SelfManagedActiveDirectoryConfiguration"].(map[string]any)
			assert.Equal(t, "corp.example.com", sm["DomainName"])

			up := doFSxRequest(t, h, "UpdateFileSystem", map[string]any{
				"FileSystemId": fs["FileSystemId"],
				"WindowsConfiguration": map[string]any{
					"SelfManagedActiveDirectoryConfiguration": map[string]any{"DomainName": "new.example.com"},
				},
			})
			got := decodeField(t, up, "FileSystem")["WindowsConfiguration"].(map[string]any)
			sm = got["SelfManagedActiveDirectoryConfiguration"].(map[string]any)
			assert.Equal(t, "new.example.com", sm["DomainName"])
			assert.Equal(t, "svc", sm["UserName"])
		})
	}
}

func TestCopyBackup_SourceRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sourceRegion string
		sourceBackup string
		wantStatus   int
		fromEU       bool
	}{
		{name: "cross region", sourceRegion: "eu-west-1", fromEU: true, wantStatus: http.StatusOK},
		{name: "wrong region", sourceRegion: "ap-south-1", fromEU: true, wantStatus: http.StatusBadRequest},
		{name: "malformed region", sourceRegion: "mars", fromEU: true, wantStatus: http.StatusBadRequest},
		{name: "same region id missing", sourceBackup: "backup-0000000000000000", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)

			bkID := tt.sourceBackup
			if tt.fromEU {
				fsRec := regionCall(t, h, "eu-west-1", fsxTarget+"CreateFileSystem",
					`{"FileSystemType":"LUSTRE","StorageCapacity":1200,"SubnetIds":["subnet-0123456789abcdef0"]}`)
				fsID := fsRec["FileSystem"].(map[string]any)["FileSystemId"].(string)

				bk := regionCall(t, h, "eu-west-1", fsxTarget+"CreateBackup", `{"FileSystemId":"`+fsID+`"}`)
				bkID = bk["Backup"].(map[string]any)["BackupId"].(string)
			}

			body := `{"SourceBackupId":"` + bkID + `"`
			if tt.sourceRegion != "" {
				body += `,"SourceRegion":"` + tt.sourceRegion + `"`
			}

			code, out := regionDo(t, h, "us-east-1", fsxTarget+"CopyBackup", body+"}")
			require.Equal(t, tt.wantStatus, code, out)

			if tt.wantStatus != http.StatusOK {
				return
			}

			copyBk := decode(t, out)["Backup"].(map[string]any)
			assert.NotEqual(t, bkID, copyBk["BackupId"])
			assert.Contains(t, copyBk["ResourceARN"], "us-east-1")
			assert.Len(t, regionCall(t, h, "us-east-1", fsxTarget+"DescribeBackups", `{}`)["Backups"], 1)
		})
	}
}
