package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataSync_UpdateLocationS3 covers UpdateLocationS3.
func TestDataSync_UpdateLocationS3(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	locArn := createTestLocationS3(t, h)

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "update subdirectory and storage class",
			body: map[string]any{
				"LocationArn":    locArn,
				"Subdirectory":   "/new-subdir",
				"S3StorageClass": "STANDARD_IA",
				"S3Config":       map[string]any{"BucketAccessRoleArn": "arn:aws:iam::000000000000:role/NewRole"},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing LocationArn returns 400",
			body:     map[string]any{"Subdirectory": "/x"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not found returns 400",
			body: map[string]any{
				"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, "UpdateLocationS3", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestDataSync_UpdateTaskExecution covers UpdateTaskExecution.
func TestDataSync_UpdateTaskExecution(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	srcArn := createTestLocationS3(t, h)
	dstArn := createTestLocationS3(t, h)
	taskArn := createTestTask(t, h, srcArn, dstArn)

	rec := doRequest(t, h, "StartTaskExecution", map[string]any{"TaskArn": taskArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execArn := startResp["TaskExecutionArn"].(string)

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "update existing execution",
			body:     map[string]any{"TaskExecutionArn": execArn},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing TaskExecutionArn returns 400",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not found returns 400",
			body: map[string]any{
				"TaskExecutionArn": "arn:aws:datasync:us-east-1:000000000000:task/notexist/execution/notexist",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, "UpdateTaskExecution", tc.body) //nolint:govet // existing issue.
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestDataSync_AzureBlob covers the AzureBlob location lifecycle.
func TestDataSync_AzureBlob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, "CreateLocationAzureBlob", map[string]any{
		"ContainerUrl": "https://myaccount.blob.core.windows.net/mycontainer",
		"Subdirectory": "/data",
		"BlobType":     "BLOCK",
		"AccessTier":   "HOT",
		"SasConfiguration": map[string]any{
			"Token": "sv=2020-08-04&ss=b&srt=sco&sp=rwdlacupx&se=2023-01-01T00:00:00Z",
		},
		"AgentArns": []string{"arn:aws:datasync:us-east-1:000000000000:agent/agent1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn, ok := createResp["LocationArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, locArn)

	// Describe
	rec = doRequest(t, h, "DescribeLocationAzureBlob", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, locArn, descResp["LocationArn"])
	assert.Equal(t, "BLOCK", descResp["BlobType"])
	assert.Equal(t, "HOT", descResp["AccessTier"])
	assert.Contains(t, descResp["LocationUri"].(string), "azure-blob://")
	assert.NotNil(t, descResp["SasConfiguration"])

	// Update
	rec = doRequest(t, h, "UpdateLocationAzureBlob", map[string]any{
		"LocationArn":  locArn,
		"Subdirectory": "/updated",
		"AccessTier":   "COOL",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Describe after update
	rec = doRequest(t, h, "DescribeLocationAzureBlob", map[string]any{"LocationArn": locArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "COOL", descResp["AccessTier"])

	// Not found
	rec = doRequest(t, h, "DescribeLocationAzureBlob", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Missing required field
	rec = doRequest(t, h, "CreateLocationAzureBlob", map[string]any{"Subdirectory": "/x"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDataSync_Efs covers the EFS location lifecycle.
func TestDataSync_Efs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, "CreateLocationEfs", map[string]any{
		"EfsFilesystemArn":    "arn:aws:elasticfilesystem:us-east-1:000000000000:file-system/fs-12345678",
		"Subdirectory":        "/data",
		"InTransitEncryption": "TLS1_2",
		"Ec2Config": map[string]any{
			"SubnetArn":         "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-12345",
			"SecurityGroupArns": []string{"arn:aws:ec2:us-east-1:000000000000:security-group/sg-12345"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationEfs", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, locArn, descResp["LocationArn"])
	assert.Equal(t, "TLS1_2", descResp["InTransitEncryption"])
	assert.Contains(t, descResp["LocationUri"].(string), "efs://")
	assert.NotNil(t, descResp["Ec2Config"])

	// Update
	rec = doRequest(t, h, "UpdateLocationEfs", map[string]any{
		"LocationArn":         locArn,
		"Subdirectory":        "/updated",
		"InTransitEncryption": "NONE",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing EfsFilesystemArn
	rec = doRequest(t, h, "CreateLocationEfs", map[string]any{"Subdirectory": "/x"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationEfs", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDataSync_FsxLustre covers the FSx Lustre location lifecycle.
func TestDataSync_FsxLustre(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, "CreateLocationFsxLustre", map[string]any{
		"FsxFilesystemArn":  "arn:aws:fsx:us-east-1:000000000000:file-system/fs-lustre01",
		"Subdirectory":      "/data",
		"SecurityGroupArns": []string{"arn:aws:ec2:us-east-1:000000000000:security-group/sg-abc"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationFsxLustre", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "lustre://")
	assert.Len(t, descResp["SecurityGroupArns"], 1)

	// Update
	rec = doRequest(t, h, "UpdateLocationFsxLustre", map[string]any{
		"LocationArn":  locArn,
		"Subdirectory": "/updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing FsxFilesystemArn
	rec = doRequest(t, h, "CreateLocationFsxLustre", map[string]any{"Subdirectory": "/x"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationFsxLustre", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDataSync_FsxOntap covers the FSx ONTAP location lifecycle.
func TestDataSync_FsxOntap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create with NFS protocol
	rec := doRequest(t, h, "CreateLocationFsxOntap", map[string]any{
		"StorageVirtualMachineArn": "arn:aws:fsx:us-east-1:000000000000:storage-virtual-machine/svm-01",
		"Subdirectory":             "/share",
		"SecurityGroupArns":        []string{"arn:aws:ec2:us-east-1:000000000000:security-group/sg-abc"},
		"Protocol": map[string]any{
			"NFS": map[string]any{
				"MountOptions": map[string]any{"Version": "NFS3"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationFsxOntap", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "ontap://")
	assert.NotNil(t, descResp["Protocol"])

	// Update
	rec = doRequest(t, h, "UpdateLocationFsxOntap", map[string]any{
		"LocationArn":  locArn,
		"Subdirectory": "/updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing StorageVirtualMachineArn
	rec = doRequest(t, h, "CreateLocationFsxOntap", map[string]any{"Subdirectory": "/x"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationFsxOntap", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDataSync_FsxOpenZfs covers the FSx OpenZFS location lifecycle.
func TestDataSync_FsxOpenZfs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, "CreateLocationFsxOpenZfs", map[string]any{
		"FsxFilesystemArn":  "arn:aws:fsx:us-east-1:000000000000:file-system/fs-openzfs01",
		"Subdirectory":      "/data",
		"SecurityGroupArns": []string{"arn:aws:ec2:us-east-1:000000000000:security-group/sg-xyz"},
		"Protocol": map[string]any{
			"NFS": map[string]any{
				"MountOptions": map[string]any{"Version": "AUTOMATIC"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationFsxOpenZfs", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "openzfs://")
	assert.NotEmpty(t, descResp["FsxFilesystemArn"])

	// Update
	rec = doRequest(t, h, "UpdateLocationFsxOpenZfs", map[string]any{
		"LocationArn":  locArn,
		"Subdirectory": "/updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing FsxFilesystemArn
	rec = doRequest(t, h, "CreateLocationFsxOpenZfs", map[string]any{"Subdirectory": "/x"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationFsxOpenZfs", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDataSync_FsxWindows covers the FSx Windows location lifecycle.
func TestDataSync_FsxWindows(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, "CreateLocationFsxWindows", map[string]any{
		"FsxFilesystemArn":  "arn:aws:fsx:us-east-1:000000000000:file-system/fs-windows01",
		"Subdirectory":      "/share",
		"Domain":            "CORP",
		"User":              "svcuser",
		"Password":          "s3cr3t",
		"SecurityGroupArns": []string{"arn:aws:ec2:us-east-1:000000000000:security-group/sg-win"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationFsxWindows", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "smb://")
	assert.Equal(t, "CORP", descResp["Domain"])
	assert.Equal(t, "svcuser", descResp["User"])
	// Password not returned in describe
	assert.Nil(t, descResp["Password"])

	// Update
	rec = doRequest(t, h, "UpdateLocationFsxWindows", map[string]any{
		"LocationArn":  locArn,
		"Domain":       "NEWDOMAIN",
		"User":         "newuser",
		"Password":     "newpass",
		"Subdirectory": "/updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	rec = doRequest(t, h, "DescribeLocationFsxWindows", map[string]any{"LocationArn": locArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "NEWDOMAIN", descResp["Domain"])

	// Missing FsxFilesystemArn
	rec = doRequest(t, h, "CreateLocationFsxWindows", map[string]any{
		"User": "u", "Password": "p",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationFsxWindows", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDataSync_Hdfs covers the HDFS location lifecycle.
func TestDataSync_Hdfs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create with SIMPLE auth
	rec := doRequest(t, h, "CreateLocationHdfs", map[string]any{
		"NameNodes": []any{
			map[string]any{"Hostname": "namenode1.example.com", "Port": 8020},
		},
		"AuthenticationType": "SIMPLE",
		"SimpleUser":         "hadoop",
		"Subdirectory":       "/user/data",
		"BlockSize":          int64(134217728),
		"ReplicationFactor":  int32(3),
		"AgentArns": []string{
			"arn:aws:datasync:us-east-1:000000000000:agent/agent1",
		},
		"QopConfiguration": map[string]any{
			"DataTransferProtection": "PRIVACY",
			"RpcProtection":          "PRIVACY",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationHdfs", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "hdfs://")
	assert.Equal(t, "SIMPLE", descResp["AuthenticationType"])
	assert.Equal(t, "hadoop", descResp["SimpleUser"])
	assert.Len(t, descResp["NameNodes"], 1)
	assert.NotNil(t, descResp["QopConfiguration"])

	// Update
	rec = doRequest(t, h, "UpdateLocationHdfs", map[string]any{
		"LocationArn": locArn,
		"SimpleUser":  "newuser",
		"NameNodes": []any{
			map[string]any{"Hostname": "namenode2.example.com", "Port": 8020},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	rec = doRequest(t, h, "DescribeLocationHdfs", map[string]any{"LocationArn": locArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "newuser", descResp["SimpleUser"])
	nameNodes := descResp["NameNodes"].([]any)
	require.Len(t, nameNodes, 1)
	assert.Equal(t, "namenode2.example.com", nameNodes[0].(map[string]any)["Hostname"])

	// Missing NameNodes
	rec = doRequest(t, h, "CreateLocationHdfs", map[string]any{
		"AuthenticationType": "SIMPLE",
		"SimpleUser":         "hadoop",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationHdfs", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDataSync_Nfs covers the NFS location lifecycle.
func TestDataSync_Nfs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, "CreateLocationNfs", map[string]any{
		"ServerHostname": "nfs.example.com",
		"Subdirectory":   "/exports/data",
		"MountOptions":   map[string]any{"Version": "NFS4_0"},
		"OnPremConfig": map[string]any{
			"AgentArns": []string{"arn:aws:datasync:us-east-1:000000000000:agent/agent1"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationNfs", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "nfs://")
	assert.Equal(t, "nfs.example.com", descResp["ServerHostname"])
	assert.NotNil(t, descResp["MountOptions"])
	assert.NotNil(t, descResp["OnPremConfig"])

	// Update
	rec = doRequest(t, h, "UpdateLocationNfs", map[string]any{
		"LocationArn":  locArn,
		"Subdirectory": "/exports/updated",
		"MountOptions": map[string]any{"Version": "NFS4_1"},
		"OnPremConfig": map[string]any{
			"AgentArns": []string{"arn:aws:datasync:us-east-1:000000000000:agent/agent2"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	rec = doRequest(t, h, "DescribeLocationNfs", map[string]any{"LocationArn": locArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	mo := descResp["MountOptions"].(map[string]any)
	assert.Equal(t, "NFS4_1", mo["Version"])

	// Missing ServerHostname
	rec = doRequest(t, h, "CreateLocationNfs", map[string]any{"Subdirectory": "/x"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationNfs", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDataSync_ObjectStorage covers the ObjectStorage location lifecycle.
func TestDataSync_ObjectStorage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, "CreateLocationObjectStorage", map[string]any{
		"ServerHostname": "s3.example.com",
		"ServerProtocol": "HTTPS",
		"ServerPort":     int32(443),
		"BucketName":     "my-bucket",
		"Subdirectory":   "/data",
		"AccessKey":      "AKIAIOSFODNN7EXAMPLE",
		"SecretKey":      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"AgentArns": []string{
			"arn:aws:datasync:us-east-1:000000000000:agent/agent1",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationObjectStorage", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "object-storage://")
	assert.Equal(t, "s3.example.com", descResp["ServerHostname"])
	assert.Equal(t, "my-bucket", descResp["BucketName"])
	assert.Equal(t, "HTTPS", descResp["ServerProtocol"])
	// SecretKey not returned
	assert.Nil(t, descResp["SecretKey"])

	// Update
	rec = doRequest(t, h, "UpdateLocationObjectStorage", map[string]any{
		"LocationArn":    locArn,
		"Subdirectory":   "/updated",
		"ServerProtocol": "HTTP",
		"ServerPort":     int32(80),
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	rec = doRequest(t, h, "DescribeLocationObjectStorage", map[string]any{"LocationArn": locArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "HTTP", descResp["ServerProtocol"])

	// Missing ServerHostname
	rec = doRequest(t, h, "CreateLocationObjectStorage", map[string]any{"BucketName": "b"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Missing BucketName
	rec = doRequest(t, h, "CreateLocationObjectStorage", map[string]any{"ServerHostname": "s3.example.com"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationObjectStorage", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDataSync_Smb covers the SMB location lifecycle.
func TestDataSync_Smb(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, "CreateLocationSmb", map[string]any{
		"ServerHostname": "smb.example.com",
		"Subdirectory":   "/share/data",
		"Domain":         "CORP",
		"User":           "smbuser",
		"Password":       "smbpass",
		"MountOptions":   map[string]any{"Version": "SMB3"},
		"AgentArns": []string{
			"arn:aws:datasync:us-east-1:000000000000:agent/agent1",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationSmb", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "smb://")
	assert.Equal(t, "smb.example.com", descResp["ServerHostname"])
	assert.Equal(t, "CORP", descResp["Domain"])
	assert.Equal(t, "smbuser", descResp["User"])
	assert.NotNil(t, descResp["MountOptions"])
	// Password not returned
	assert.Nil(t, descResp["Password"])

	// Update
	rec = doRequest(t, h, "UpdateLocationSmb", map[string]any{
		"LocationArn":  locArn,
		"Subdirectory": "/share/updated",
		"Domain":       "NEWDOMAIN",
		"User":         "newuser",
		"Password":     "newpass",
		"MountOptions": map[string]any{"Version": "AUTOMATIC"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	rec = doRequest(t, h, "DescribeLocationSmb", map[string]any{"LocationArn": locArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "NEWDOMAIN", descResp["Domain"])
	mo := descResp["MountOptions"].(map[string]any)
	assert.Equal(t, "AUTOMATIC", mo["Version"])

	// Missing ServerHostname
	rec = doRequest(t, h, "CreateLocationSmb", map[string]any{
		"User": "u", "Password": "p",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationSmb", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
