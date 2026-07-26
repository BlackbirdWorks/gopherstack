package sagemaker_test

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func TestReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedModel bool
		wantCount int
	}{
		{
			name:      "reset clears models",
			seedModel: true,
			wantCount: 0,
		},
		{
			name:      "reset on empty backend",
			seedModel: false,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.seedModel {
				_, err := b.CreateModel(context.Background(),
					"test-model",
					"arn:aws:iam::000000000000:role/role",
					nil,
					nil,
					nil,
				)
				require.NoError(t, err)
			}

			b.Reset()

			assert.Equal(t, tt.wantCount, sagemaker.ModelCount(b))
		})
	}
}

func TestHandlerReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedModel bool
		wantCount int
	}{
		{
			name:      "handler reset clears models",
			seedModel: true,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.seedModel {
				rec := doSageMakerRequest(t, h, "CreateModel", map[string]any{
					"ModelName":        "my-model",
					"ExecutionRoleArn": "arn:aws:iam::000000000000:role/role",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			h.Reset()

			assert.Equal(t, tt.wantCount, sagemaker.ModelCount(h.Backend))
		})
	}
}

func TestRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		region     string
		wantRegion string
	}{
		{
			name:       "returns configured region",
			region:     "us-west-2",
			wantRegion: "us-west-2",
		},
		{
			name:       "returns us-east-1",
			region:     "us-east-1",
			wantRegion: "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", tt.region)
			assert.Equal(t, tt.wantRegion, b.Region())
		})
	}
}

func TestAccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		accountID     string
		wantAccountID string
	}{
		{
			name:          "returns configured account ID",
			accountID:     "123456789012",
			wantAccountID: "123456789012",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend(tt.accountID, "us-east-1")
			assert.Equal(t, tt.wantAccountID, b.AccountID())
		})
	}
}

func TestModelCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numModels int
		wantCount int
	}{
		{name: "zero models", numModels: 0, wantCount: 0},
		{name: "one model", numModels: 1, wantCount: 1},
		{name: "three models", numModels: 3, wantCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numModels {
				_, err := b.CreateModel(context.Background(),
					fmt.Sprintf("model-%d", i),
					"arn:aws:iam::000000000000:role/role",
					nil, nil, nil,
				)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCount, sagemaker.ModelCount(b))
		})
	}
}

func TestEndpointConfigCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numConfs  int
		wantCount int
	}{
		{name: "zero configs", numConfs: 0, wantCount: 0},
		{name: "two configs", numConfs: 2, wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numConfs {
				_, err := b.CreateEndpointConfig(context.Background(), fmt.Sprintf("cfg-%d", i), nil, nil)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCount, sagemaker.EndpointConfigCount(b))
		})
	}
}

func TestAssociationCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numAssocs int
		wantCount int
	}{
		{name: "no associations", numAssocs: 0, wantCount: 0},
		{name: "one association", numAssocs: 1, wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numAssocs {
				src := fmt.Sprintf("arn:aws:sagemaker:us-east-1:000000000000:trial/t%d", i)
				dst := fmt.Sprintf("arn:aws:sagemaker:us-east-1:000000000000:artifact/a%d", i)
				_, err := b.AddAssociation(context.Background(), src, dst, "ContributedTo", nil)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCount, sagemaker.AssociationCount(b))
		})
	}
}

func TestActionCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numActs   int
		wantCount int
	}{
		{name: "no actions", numActs: 0, wantCount: 0},
		{name: "two actions", numActs: 2, wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numActs {
				b.AddActionInternal(context.Background(), fmt.Sprintf("action-%d", i), "ModelDeployment")
			}

			assert.Equal(t, tt.wantCount, sagemaker.ActionCount(b))
		})
	}
}

func TestAlgorithmCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numAlgos  int
		wantCount int
	}{
		{name: "no algorithms", numAlgos: 0, wantCount: 0},
		{name: "one algorithm", numAlgos: 1, wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numAlgos {
				b.AddAlgorithmInternal(context.Background(), "algo-"+strconv.Itoa(i))
			}

			assert.Equal(t, tt.wantCount, sagemaker.AlgorithmCount(b))
		})
	}
}

func TestClusterCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		numClusters int
		wantCount   int
	}{
		{name: "no clusters", numClusters: 0, wantCount: 0},
		{name: "one cluster", numClusters: 1, wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numClusters {
				b.AddClusterInternal(context.Background(), "cluster-"+strconv.Itoa(i))
			}

			assert.Equal(t, tt.wantCount, sagemaker.ClusterCount(b))
		})
	}
}

func TestModelPackageCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numPkgs   int
		wantCount int
	}{
		{name: "no packages", numPkgs: 0, wantCount: 0},
		{name: "two packages", numPkgs: 2, wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.numPkgs {
				arnStr := fmt.Sprintf(
					"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-%d",
					i,
				)
				b.AddModelPackageInternal(context.Background(), &sagemaker.ModelPackage{
					ModelPackageName:   fmt.Sprintf("pkg-%d", i),
					ModelPackageArn:    arnStr,
					ModelPackageStatus: "Approved",
				})
			}

			assert.Equal(t, tt.wantCount, sagemaker.ModelPackageCount(b))
		})
	}
}

func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantLen int
	}{
		{
			name:    "returns 403 operations",
			wantLen: 403,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			assert.Equal(t, tt.wantLen, sagemaker.HandlerOpsLen(h))
		})
	}
}

func TestUnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		op       string
		wantCode int
	}{
		{
			name:     "unknown op returns 400",
			op:       "DoSomethingUnknown",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, tt.op, map[string]any{})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestNilAppContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx     *service.AppContext
		name    string
		wantErr bool
	}{
		{
			name:    "nil context returns error",
			ctx:     nil,
			wantErr: true,
		},
		{
			name:    "non-nil context returns handler",
			ctx:     &service.AppContext{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &sagemaker.Provider{}
			h, err := p.Init(tt.ctx)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, h)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, h)
			}
		})
	}
}
