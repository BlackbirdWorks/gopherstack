package bedrockagent_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

type vpcEnv struct {
	backend *bedrockagent.InMemoryBackend
	client  *bedrockagentsdk.Client
	kbID    string
}

func newVpcEnv(t *testing.T) *vpcEnv {
	t.Helper()

	backend := bedrockagent.NewTestBackend(rtTestRegion, rtTestAccountID)
	h := bedrockagent.NewTestHandler(backend)
	h.AccountID = rtTestAccountID
	h.DefaultRegion = rtTestRegion

	kb, err := backend.CreateKnowledgeBase(t.Context(), bedrockagent.KnowledgeBaseConfig{
		Name: "vpc-kb", RoleARN: "arn:aws:iam::123456789012:role/kb-role",
	})
	require.NoError(t, err)

	return &vpcEnv{backend: backend, client: newRoundTripClient(t, h), kbID: kb.KnowledgeBaseID}
}

func (e *vpcEnv) input() *bedrockagentsdk.CreateVpcConfigurationInput {
	return &bedrockagentsdk.CreateVpcConfigurationInput{
		KnowledgeBaseId: aws.String(e.kbID),
		Name:            aws.String("conn"),
		Description:     aws.String("private db"),
		VpcId:           aws.String("vpc-0123456789abcdef0"),
		SubnetIds:       []string{"subnet-0123456789abcdef0"},
		ResourceTarget:  aws.String("10.0.1.25"),
		Port:            aws.Int32(5432),
		Protocol:        types.VpcProtocolHttps,
		TlsServerName:   aws.String("db.internal.example.com"),
		HostHeader:      aws.String("db.internal.example.com"),
		ResolutionMode:  types.VpcResolutionModeInVpc,
	}
}

func TestVpcConfiguration_Lifecycle(t *testing.T) {
	t.Parallel()

	env := newVpcEnv(t)
	ctx := t.Context()

	created, err := env.client.CreateVpcConfiguration(ctx, env.input())
	require.NoError(t, err)
	assert.Equal(t, types.VpcConfigurationStatusCreating, created.Status)

	id := created.VpcConfigurationId
	require.NotEmpty(t, aws.ToString(id))

	got, err := env.client.GetVpcConfiguration(ctx, &bedrockagentsdk.GetVpcConfigurationInput{
		KnowledgeBaseId: aws.String(env.kbID), VpcConfigurationId: id,
	})
	require.NoError(t, err)

	cfg := got.VpcConfiguration
	require.NotNil(t, cfg)
	assert.Equal(t, types.VpcConfigurationStatusCreated, cfg.Status)
	assert.Equal(t, id, cfg.VpcConfigurationId)
	assert.Equal(t, "conn", aws.ToString(cfg.Name))
	assert.Equal(t, "private db", aws.ToString(cfg.Description))
	assert.Equal(t, "vpc-0123456789abcdef0", aws.ToString(cfg.VpcId))
	assert.Equal(t, []string{"subnet-0123456789abcdef0"}, cfg.SubnetIds)
	assert.Equal(t, "10.0.1.25", aws.ToString(cfg.ResourceTarget))
	assert.Equal(t, int32(5432), aws.ToInt32(cfg.Port))
	assert.Equal(t, types.VpcProtocolHttps, cfg.Protocol)
	assert.Equal(t, types.VpcResolutionModeInVpc, cfg.ResolutionMode)
	assert.Equal(t, "db.internal.example.com", aws.ToString(cfg.TlsServerName))
	assert.Equal(t, "db.internal.example.com", aws.ToString(cfg.HostHeader))
	require.NotNil(t, cfg.CreatedAt)
	require.NotNil(t, cfg.UpdatedAt)

	deleted, err := env.client.DeleteVpcConfiguration(ctx, &bedrockagentsdk.DeleteVpcConfigurationInput{
		KnowledgeBaseId: aws.String(env.kbID), VpcConfigurationId: id,
	})
	require.NoError(t, err)
	assert.Equal(t, types.VpcConfigurationStatusDeleting, deleted.Status)
	assert.Equal(t, id, deleted.VpcConfigurationId)

	_, err = env.client.GetVpcConfiguration(ctx, &bedrockagentsdk.GetVpcConfigurationInput{
		KnowledgeBaseId: aws.String(env.kbID), VpcConfigurationId: id,
	})
	var nf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
}

func TestVpcConfiguration_ClientTokenIdempotent(t *testing.T) {
	t.Parallel()

	env := newVpcEnv(t)
	in := env.input()
	in.ClientToken = aws.String("token-1")

	first, err := env.client.CreateVpcConfiguration(t.Context(), in)
	require.NoError(t, err)

	second, err := env.client.CreateVpcConfiguration(t.Context(), in)
	require.NoError(t, err)
	assert.Equal(t, first.VpcConfigurationId, second.VpcConfigurationId)

	listed, err := env.client.ListVpcConfigurations(t.Context(), &bedrockagentsdk.ListVpcConfigurationsInput{
		KnowledgeBaseId: aws.String(env.kbID),
	})
	require.NoError(t, err)
	assert.Len(t, listed.Items, 1)
}

func TestVpcConfiguration_List(t *testing.T) {
	t.Parallel()

	env := newVpcEnv(t)
	ctx := t.Context()

	for _, name := range []string{"a", "b", "c"} {
		in := env.input()
		in.Name = aws.String(name)
		_, err := env.client.CreateVpcConfiguration(ctx, in)
		require.NoError(t, err)
	}

	tests := []struct {
		name   string
		status types.VpcConfigurationStatus
		want   int
	}{
		{name: "all", want: 3},
		{name: "created", status: types.VpcConfigurationStatusCreated, want: 3},
		{name: "creating", status: types.VpcConfigurationStatusCreating, want: 0},
		{name: "deleting", status: types.VpcConfigurationStatusDeleting, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := env.client.ListVpcConfigurations(ctx, &bedrockagentsdk.ListVpcConfigurationsInput{
				KnowledgeBaseId: aws.String(env.kbID), StatusFilter: tc.status,
			})
			require.NoError(t, err)
			assert.Len(t, out.Items, tc.want)
		})
	}

	var names []string

	p := bedrockagentsdk.NewListVpcConfigurationsPaginator(env.client, &bedrockagentsdk.ListVpcConfigurationsInput{
		KnowledgeBaseId: aws.String(env.kbID), MaxResults: aws.Int32(2),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		require.NoError(t, err)

		for _, it := range page.Items {
			names = append(names, aws.ToString(it.Name))
		}
	}

	assert.ElementsMatch(t, []string{"a", "b", "c"}, names)
}

func TestVpcConfiguration_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(in *bedrockagentsdk.CreateVpcConfigurationInput)
		name   string
	}{
		{name: "port zero", mutate: func(in *bedrockagentsdk.CreateVpcConfigurationInput) { in.Port = aws.Int32(0) }},
		{
			name:   "port too large",
			mutate: func(in *bedrockagentsdk.CreateVpcConfigurationInput) { in.Port = aws.Int32(70000) },
		},
		{
			name:   "no subnets",
			mutate: func(in *bedrockagentsdk.CreateVpcConfigurationInput) { in.SubnetIds = []string{} },
		},
		{
			name:   "https without tls name",
			mutate: func(in *bedrockagentsdk.CreateVpcConfigurationInput) { in.TlsServerName = nil },
		},
		{
			name: "tls name with port",
			mutate: func(in *bedrockagentsdk.CreateVpcConfigurationInput) {
				in.TlsServerName = aws.String("db.example.com:443")
			},
		},
		{
			name:   "loopback target",
			mutate: func(in *bedrockagentsdk.CreateVpcConfigurationInput) { in.ResourceTarget = aws.String("127.0.0.1") },
		},
		{
			name:   "link local target",
			mutate: func(in *bedrockagentsdk.CreateVpcConfigurationInput) { in.ResourceTarget = aws.String("169.254.1.1") },
		},
		{
			name:   "wildcard target",
			mutate: func(in *bedrockagentsdk.CreateVpcConfigurationInput) { in.ResourceTarget = aws.String("0.0.0.0") },
		},
		{
			name:   "multicast target",
			mutate: func(in *bedrockagentsdk.CreateVpcConfigurationInput) { in.ResourceTarget = aws.String("224.0.0.1") },
		},
		{
			name:   "ipv6 target",
			mutate: func(in *bedrockagentsdk.CreateVpcConfigurationInput) { in.ResourceTarget = aws.String("fd00::1") },
		},
		{
			name:   "public target",
			mutate: func(in *bedrockagentsdk.CreateVpcConfigurationInput) { in.ResourceTarget = aws.String("8.8.8.8") },
		},
		{name: "bad protocol", mutate: func(in *bedrockagentsdk.CreateVpcConfigurationInput) { in.Protocol = "FTP" }},
		{
			name:   "bad resolution mode",
			mutate: func(in *bedrockagentsdk.CreateVpcConfigurationInput) { in.ResolutionMode = "NOPE" },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newVpcEnv(t)
			in := env.input()
			tc.mutate(in)

			_, err := env.client.CreateVpcConfiguration(t.Context(), in)

			var ve *types.ValidationException
			require.ErrorAs(t, err, &ve)
		})
	}
}

func TestVpcConfiguration_NotFound(t *testing.T) {
	t.Parallel()

	env := newVpcEnv(t)
	ctx := t.Context()
	missing := aws.String("does-not-exist")

	tests := []struct {
		call func() error
		name string
	}{
		{name: "create unknown kb", call: func() error {
			in := env.input()
			in.KnowledgeBaseId = missing
			_, err := env.client.CreateVpcConfiguration(ctx, in)

			return err
		}},
		{name: "get unknown kb", call: func() error {
			_, err := env.client.GetVpcConfiguration(ctx, &bedrockagentsdk.GetVpcConfigurationInput{
				KnowledgeBaseId: missing, VpcConfigurationId: missing,
			})

			return err
		}},
		{name: "get unknown config", call: func() error {
			_, err := env.client.GetVpcConfiguration(ctx, &bedrockagentsdk.GetVpcConfigurationInput{
				KnowledgeBaseId: aws.String(env.kbID), VpcConfigurationId: missing,
			})

			return err
		}},
		{name: "list unknown kb", call: func() error {
			_, err := env.client.ListVpcConfigurations(
				ctx,
				&bedrockagentsdk.ListVpcConfigurationsInput{KnowledgeBaseId: missing},
			)

			return err
		}},
		{name: "delete unknown config", call: func() error {
			_, err := env.client.DeleteVpcConfiguration(ctx, &bedrockagentsdk.DeleteVpcConfigurationInput{
				KnowledgeBaseId: aws.String(env.kbID), VpcConfigurationId: missing,
			})

			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var nf *types.ResourceNotFoundException
			require.ErrorAs(t, tc.call(), &nf)
		})
	}
}

func TestVpcConfiguration_PersistAndCascade(t *testing.T) {
	t.Parallel()

	env := newVpcEnv(t)
	ctx := t.Context()

	created, err := env.client.CreateVpcConfiguration(ctx, env.input())
	require.NoError(t, err)

	restored := bedrockagent.NewTestBackend(rtTestRegion, rtTestAccountID)
	require.NoError(t, restored.Restore(ctx, env.backend.Snapshot(ctx)))

	got, err := restored.GetVpcConfiguration(ctx, env.kbID, aws.ToString(created.VpcConfigurationId))
	require.NoError(t, err)
	assert.Equal(t, "conn", got.Name)

	require.NoError(t, env.backend.DeleteKnowledgeBase(ctx, env.kbID))

	_, err = env.backend.GetVpcConfiguration(ctx, env.kbID, aws.ToString(created.VpcConfigurationId))
	require.ErrorIs(t, err, bedrockagent.ErrNotFound)
}

func TestVpcConfiguration_TransitionsOverTime(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		b := bedrockagent.NewTestBackend(rtTestRegion, rtTestAccountID)
		b.SetLifecycleDelay(10 * time.Second)

		kb, err := b.CreateKnowledgeBase(ctx, bedrockagent.KnowledgeBaseConfig{Name: "kb"})
		require.NoError(t, err)

		cfg, err := b.CreateVpcConfiguration(ctx, kb.KnowledgeBaseID, bedrockagent.VpcConfigurationConfig{
			Name: "c", VpcID: "vpc-1", SubnetIDs: []string{"subnet-1"}, ResourceTarget: "10.0.0.5",
			Port: 80, Protocol: "HTTP", ResolutionMode: "IN_VPC",
		})
		require.NoError(t, err)
		assert.Equal(t, "CREATING", cfg.Status)

		time.Sleep(11 * time.Second)

		got, err := b.GetVpcConfiguration(ctx, kb.KnowledgeBaseID, cfg.VpcConfigurationID)
		require.NoError(t, err)
		assert.Equal(t, "CREATED", got.Status)

		del, err := b.DeleteVpcConfiguration(ctx, kb.KnowledgeBaseID, cfg.VpcConfigurationID)
		require.NoError(t, err)
		assert.Equal(t, "DELETING", del.Status)

		got, err = b.GetVpcConfiguration(ctx, kb.KnowledgeBaseID, cfg.VpcConfigurationID)
		require.NoError(t, err)
		assert.Equal(t, "DELETING", got.Status)

		time.Sleep(11 * time.Second)

		_, err = b.GetVpcConfiguration(ctx, kb.KnowledgeBaseID, cfg.VpcConfigurationID)
		require.ErrorIs(t, err, bedrockagent.ErrNotFound)
	})
}
