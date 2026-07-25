package verifiedpermissions_test

import (
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBackend() *verifiedpermissions.InMemoryBackend {
	return verifiedpermissions.NewInMemoryBackend("123456789012", "us-east-1")
}

func seedPolicyStore(
	t *testing.T,
	b *verifiedpermissions.InMemoryBackend,
	desc string,
) *verifiedpermissions.PolicyStore {
	t.Helper()

	ps, err := b.CreatePolicyStore(desc, nil, "OFF", "", "")
	require.NoError(t, err)

	return ps
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numStores int
	}{
		{name: "reset empty backend", numStores: 0},
		{name: "reset with data", numStores: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			for range tt.numStores {
				_, err := b.CreatePolicyStore("desc", nil, "OFF", "", "")
				require.NoError(t, err)
			}

			stores, _ := b.ListPolicyStores("", 0)
			assert.Len(t, stores, tt.numStores)

			b.Reset()

			stores, _ = b.ListPolicyStores("", 0)
			assert.Empty(t, stores)
		})
	}
}

func TestBackend_AccountID(t *testing.T) {
	t.Parallel()

	b := verifiedpermissions.NewInMemoryBackend("111222333444", "eu-west-1")
	assert.Equal(t, "111222333444", b.AccountID())
}

func TestBackend_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(*verifiedpermissions.InMemoryBackend)
		name   string
		wantPS int
		wantP  int
		wantT  int
		wantIS int
	}{
		{
			name: "AddPolicyStoreInternal",
			setup: func(b *verifiedpermissions.InMemoryBackend) {
				b.AddPolicyStoreInternal(&verifiedpermissions.PolicyStore{
					PolicyStoreID: "ps-seed",
					Arn:           "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store/ps-seed",
					Description:   "seeded",
					CreatedDate:   time.Now(),
					LastUpdated:   time.Now(),
				})
			},
			wantPS: 1,
		},
		{
			name: "AddPolicyInternal",
			setup: func(b *verifiedpermissions.InMemoryBackend) {
				b.AddPolicyStoreInternal(&verifiedpermissions.PolicyStore{
					PolicyStoreID: "ps-seed",
					Arn:           "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store/ps-seed",
					CreatedDate:   time.Now(),
					LastUpdated:   time.Now(),
				})
				b.AddPolicyInternal(&verifiedpermissions.Policy{
					PolicyID:      "pol-seed",
					PolicyStoreID: "ps-seed",
					PolicyType:    "STATIC",
					Statement:     "permit(principal,action,resource);",
					CreatedDate:   time.Now(),
					LastUpdated:   time.Now(),
				})
			},
			wantPS: 1,
			wantP:  1,
		},
		{
			name: "AddPolicyTemplateInternal",
			setup: func(b *verifiedpermissions.InMemoryBackend) {
				b.AddPolicyStoreInternal(&verifiedpermissions.PolicyStore{
					PolicyStoreID: "ps-seed",
					Arn:           "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store/ps-seed",
					CreatedDate:   time.Now(),
					LastUpdated:   time.Now(),
				})
				b.AddPolicyTemplateInternal(&verifiedpermissions.PolicyTemplate{
					PolicyTemplateID: "tmpl-seed",
					PolicyStoreID:    "ps-seed",
					Statement:        "permit(principal,action,resource);",
					CreatedDate:      time.Now(),
					LastUpdated:      time.Now(),
				})
			},
			wantPS: 1,
			wantT:  1,
		},
		{
			name: "AddIdentitySourceInternal",
			setup: func(b *verifiedpermissions.InMemoryBackend) {
				b.AddPolicyStoreInternal(&verifiedpermissions.PolicyStore{
					PolicyStoreID: "ps-seed",
					Arn:           "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store/ps-seed",
					CreatedDate:   time.Now(),
					LastUpdated:   time.Now(),
				})
				b.AddIdentitySourceInternal(&verifiedpermissions.IdentitySource{
					IdentitySourceID: "is-seed",
					PolicyStoreID:    "ps-seed",
					UserPoolArn:      "arn:aws:cognito-idp:us-east-1:123456789012:userpool/pool",
					CreatedDate:      time.Now(),
					LastUpdated:      time.Now(),
				})
			},
			wantPS: 1,
			wantIS: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			tt.setup(b)

			assert.Equal(t, tt.wantPS, verifiedpermissions.PolicyStoreCount(b))
			assert.Equal(t, tt.wantP, verifiedpermissions.PolicyCount(b))
			assert.Equal(t, tt.wantT, verifiedpermissions.PolicyTemplateCount(b))
			assert.Equal(t, tt.wantIS, verifiedpermissions.IdentitySourceCount(b))
		})
	}
}

func TestBackend_ExportHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*verifiedpermissions.InMemoryBackend)
		name      string
		wantPS    int
		wantPol   int
		wantTmpl  int
		wantIS    int
		wantSch   int
		wantARN   int
		wantOpsGE int
	}{
		{
			name:      "empty backend",
			setup:     func(*verifiedpermissions.InMemoryBackend) {},
			wantPS:    0,
			wantPol:   0,
			wantTmpl:  0,
			wantIS:    0,
			wantSch:   0,
			wantARN:   0,
			wantOpsGE: 30,
		},
		{
			name: "one policy store with policy and template",
			setup: func(b *verifiedpermissions.InMemoryBackend) {
				ps, _ := b.CreatePolicyStore("desc", nil, "OFF", "", "")
				_, _ = b.CreatePolicy(
					ps.PolicyStoreID,
					verifiedpermissions.CreatePolicyParams{
						PolicyType: "STATIC",
						Statement:  "permit(principal,action,resource);",
					},
				)
				_, _ = b.CreatePolicyTemplate(ps.PolicyStoreID, "tmpl", "permit(principal,action,resource);", "")
				_, _ = b.CreateIdentitySource(
					ps.PolicyStoreID,
					"User",
					verifiedpermissions.IdentitySourceConfig{
						UserPoolArn: "arn:aws:cognito-idp:us-east-1:123456789012:userpool/pool",
					}, "",
				)
				_, _ = b.PutSchema(ps.PolicyStoreID, `{"ns":{}}`)
			},
			wantPS:    1,
			wantPol:   1,
			wantTmpl:  1,
			wantIS:    1,
			wantSch:   1,
			wantARN:   4,
			wantOpsGE: 30,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			tt.setup(b)
			h := verifiedpermissions.NewHandler(b)

			assert.Equal(t, tt.wantPS, verifiedpermissions.PolicyStoreCount(b))
			assert.Equal(t, tt.wantPol, verifiedpermissions.PolicyCount(b))
			assert.Equal(t, tt.wantTmpl, verifiedpermissions.PolicyTemplateCount(b))
			assert.Equal(t, tt.wantIS, verifiedpermissions.IdentitySourceCount(b))
			assert.Equal(t, tt.wantSch, verifiedpermissions.SchemaCount(b))
			assert.Equal(t, tt.wantARN, verifiedpermissions.ARNIndexSize(b))
			assert.GreaterOrEqual(t, verifiedpermissions.HandlerOpsLen(h), tt.wantOpsGE)
		})
	}
}
