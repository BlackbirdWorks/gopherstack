package rds_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestBlueGreen_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	bg, err := b.CreateBlueGreenDeployment("bg1", "arn:aws:rds:us-east-1:123:db:source-db")
	require.NoError(t, err)
	assert.Equal(t, "bg1", bg.BlueGreenDeploymentName)
	assert.NotEmpty(t, bg.BlueGreenDeploymentIdentifier)

	bgs, err := b.DescribeBlueGreenDeployments("")
	require.NoError(t, err)
	require.Len(t, bgs, 1)
}

func TestBlueGreen_Switchover(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	bg, err := b.CreateBlueGreenDeployment("bg2", "arn:aws:rds:us-east-1:123:db:source-db")
	require.NoError(t, err)

	switched, err := b.SwitchoverBlueGreenDeployment(bg.BlueGreenDeploymentIdentifier)
	require.NoError(t, err)
	assert.NotEmpty(t, switched.BlueGreenDeploymentIdentifier)
}

func TestBlueGreen_DeleteNotFound(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.DeleteBlueGreenDeployment("nonexistent-id")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrBlueGreenDeploymentNotFound)
}

func TestBlueGreen_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":                  {"CreateBlueGreenDeployment"},
		"Version":                 {"2014-10-31"},
		"BlueGreenDeploymentName": {"http-bg"},
		"Source":                  {"arn:aws:rds:us-east-1:000:db:src"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":  {"DescribeBlueGreenDeployments"},
		"Version": {"2014-10-31"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-bg")
}

func TestBlueGreenDeployment_TargetGenerated(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	dep, err := b.CreateBlueGreenDeployment("my-bgd", "arn:aws:rds:us-east-1:123:cluster:source")
	require.NoError(t, err)

	assert.NotEmpty(t, dep.Target, "Target should be set for blue/green deployment")
	assert.Contains(t, dep.Target, "source-green", "Target should be derived from source")
}

func TestBlueGreenDeployment_TargetViaHandler(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()

	rec := postRDSForm(t, h,
		"Action=CreateBlueGreenDeployment&Version=2014-10-31"+
			"&BlueGreenDeploymentName=bgd-test"+
			"&Source=arn:aws:rds:us-east-1:123:cluster:my-cluster")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "<Target>", "Target field should be in XML response")
	assert.Contains(t, respStr, "green")
}

func TestPersistence_BlueGreenTarget(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateBlueGreenDeployment("bgd-snap", "arn:aws:rds:us-east-1:123:cluster:src")
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	deployments, err := b2.DescribeBlueGreenDeployments("")
	require.NoError(t, err)
	require.Len(t, deployments, 1)

	assert.NotEmpty(t, deployments[0].Target, "Target should survive round-trip")
	assert.Contains(t, deployments[0].Target, "green")
}

func TestRDS_BlueGreenDeployments(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()

	// DescribeBlueGreenDeployments (list, should be empty)
	rec := postRDSForm(t, h, url.Values{
		"Action": []string{"DescribeBlueGreenDeployments"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
}

// TestRDSBackend_CreateBlueGreenDeployment tests CreateBlueGreenDeployment.
func TestRDSBackend_CreateBlueGreenDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		setup     func(b *rds.InMemoryBackend)
		name      string
		deplName  string
		source    string
		wantErr   bool
	}{
		{
			name:     "success",
			setup:    func(_ *rds.InMemoryBackend) {},
			deplName: "my-deployment",
			source:   "arn:aws:rds:us-east-1:000000000000:db:my-db",
		},
		{
			name: "duplicate_name",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateBlueGreenDeployment("my-deployment", "arn:aws:rds:us-east-1:000000000000:db:my-db")
			},
			deplName:  "my-deployment",
			source:    "arn:aws:rds:us-east-1:000000000000:db:my-db",
			wantErr:   true,
			wantErrIs: rds.ErrBlueGreenDeploymentAlreadyExists,
		},
		{
			name:      "empty_name",
			setup:     func(_ *rds.InMemoryBackend) {},
			deplName:  "",
			source:    "arn:aws:rds:us-east-1:000000000000:db:my-db",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "empty_source",
			setup:     func(_ *rds.InMemoryBackend) {},
			deplName:  "my-deployment",
			source:    "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			d, err := b.CreateBlueGreenDeployment(tt.deplName, tt.source)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.deplName, d.BlueGreenDeploymentName)
			assert.Equal(t, "available", d.Status)
			assert.Equal(t, tt.source, d.Source)
		})
	}
}

func TestDescribeBlueGreenDeployments(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		wantCount int
		useID     bool
		wantErr   bool
	}{
		{name: "all", useID: false, wantCount: 2},
		{name: "by ID", useID: true, wantCount: 1},
		{name: "not found", wantErr: true, wantErrIs: rds.ErrBlueGreenDeploymentNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "not found" {
				_, err := b.DescribeBlueGreenDeployments("nonexistent-id")
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			d1, err := b.CreateBlueGreenDeployment("bg-1", "arn:aws:rds:us-east-1:123456789012:cluster:source-1")
			require.NoError(t, err)
			_, err = b.CreateBlueGreenDeployment("bg-2", "arn:aws:rds:us-east-1:123456789012:cluster:source-2")
			require.NoError(t, err)
			if tt.useID {
				got, err2 := b.DescribeBlueGreenDeployments(d1.BlueGreenDeploymentIdentifier)
				require.NoError(t, err2)
				assert.Len(t, got, 1)

				return
			}
			got, err := b.DescribeBlueGreenDeployments("")
			require.NoError(t, err)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestDeleteBlueGreenDeployment(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		wantErr   bool
	}{
		{name: "success"},
		{name: "not found", wantErr: true, wantErrIs: rds.ErrBlueGreenDeploymentNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.wantErr {
				_, err := b.DeleteBlueGreenDeployment("nonexistent-id")
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			d, err := b.CreateBlueGreenDeployment("bg-test", "arn:aws:rds:us-east-1:123456789012:cluster:source")
			require.NoError(t, err)
			got, err := b.DeleteBlueGreenDeployment(d.BlueGreenDeploymentIdentifier)
			require.NoError(t, err)
			assert.Equal(t, d.BlueGreenDeploymentIdentifier, got.BlueGreenDeploymentIdentifier)
		})
	}
}

func TestSwitchoverBlueGreenDeployment(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		wantErr   bool
	}{
		{name: "success sets status switchover-completed"},
		{name: "not found", wantErr: true, wantErrIs: rds.ErrBlueGreenDeploymentNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.wantErr {
				_, err := b.SwitchoverBlueGreenDeployment("nonexistent-id")
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			d, err := b.CreateBlueGreenDeployment("bg-test", "arn:aws:rds:us-east-1:123456789012:cluster:source")
			require.NoError(t, err)
			got, err := b.SwitchoverBlueGreenDeployment(d.BlueGreenDeploymentIdentifier)
			require.NoError(t, err)
			assert.Equal(t, "switchover-completed", got.Status)
		})
	}
}
