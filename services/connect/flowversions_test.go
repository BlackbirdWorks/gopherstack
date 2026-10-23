package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContactFlowVersionsAndAliases(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "flowver"))
	flow := aws.String(f.newFlow(t, aws.ToString(inst), "versioned"))

	v1, err := f.client.ListContactFlowVersions(
		t.Context(),
		&sdk.ListContactFlowVersionsInput{InstanceId: inst, ContactFlowId: flow},
	)
	require.NoError(t, err)
	require.Len(t, v1.ContactFlowVersionSummaryList, 1, "creation publishes version 1")
	assert.Equal(t, int64(1), aws.ToInt64(v1.ContactFlowVersionSummaryList[0].Version))

	_, err = f.client.CreateContactFlowVersion(t.Context(), &sdk.CreateContactFlowVersionInput{
		InstanceId: inst, ContactFlowId: flow, FlowContentSha256: aws.String("stale"),
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	_, err = f.client.CreateContactFlowVersion(t.Context(), &sdk.CreateContactFlowVersionInput{
		InstanceId: inst, ContactFlowId: aws.String("gone"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	v2, err := f.client.CreateContactFlowVersion(t.Context(), &sdk.CreateContactFlowVersionInput{
		InstanceId: inst, ContactFlowId: flow, Description: aws.String("second"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), aws.ToInt64(v2.Version))

	desc, err := f.client.DescribeContactFlow(
		t.Context(),
		&sdk.DescribeContactFlowInput{InstanceId: inst, ContactFlowId: flow},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(2), aws.ToInt64(desc.ContactFlow.Version))

	alias, err := f.client.CreateContactFlowAlias(t.Context(), &sdk.CreateContactFlowAliasInput{
		InstanceId: inst, ContactFlowId: flow, AliasName: aws.String("live"), ContactFlowVersion: aws.Int64(1),
	})
	require.NoError(t, err)

	aliasTests := []struct {
		name    string
		in      sdk.CreateContactFlowAliasInput
		errCode string
	}{
		{
			name:    "duplicate",
			in:      sdk.CreateContactFlowAliasInput{AliasName: aws.String("live"), ContactFlowVersion: aws.Int64(2)},
			errCode: "DuplicateResourceException",
		},
		{
			name:    "missing version",
			in:      sdk.CreateContactFlowAliasInput{AliasName: aws.String("x"), ContactFlowVersion: aws.Int64(9)},
			errCode: "ResourceNotFoundException",
		},
	}
	for _, tt := range aliasTests {
		in := tt.in
		in.InstanceId, in.ContactFlowId = inst, flow
		_, aerr := f.client.CreateContactFlowAlias(t.Context(), &in)
		assert.Equal(t, tt.errCode, apiCode(aerr), tt.name)
	}

	_, err = f.client.DeleteContactFlowVersion(t.Context(), &sdk.DeleteContactFlowVersionInput{
		InstanceId: inst, ContactFlowId: flow, ContactFlowVersion: aws.Int64(1),
	})
	assert.Equal(t, "ResourceInUseException", apiCode(err), "aliased version")

	_, err = f.client.UpdateContactFlowAlias(t.Context(), &sdk.UpdateContactFlowAliasInput{
		InstanceId:         inst,
		ContactFlowId:      flow,
		AliasId:            alias.Id,
		ContactFlowVersion: aws.Int64(2),
		Name:               aws.String("prod"),
	})
	require.NoError(t, err)

	got, err := f.client.DescribeContactFlowAlias(t.Context(), &sdk.DescribeContactFlowAliasInput{
		InstanceId: inst, ContactFlowId: flow, AliasId: alias.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, "prod", aws.ToString(got.ContactFlowAlias.Name))
	assert.Equal(t, int64(2), aws.ToInt64(got.ContactFlowAlias.Version))
	assert.Equal(t, aws.ToString(flow), aws.ToString(got.ContactFlowAlias.ContactFlowId))

	dst := f.restored(t)
	list, err := dst.client.ListContactFlowAliases(
		t.Context(),
		&sdk.ListContactFlowAliasesInput{InstanceId: inst, ContactFlowId: flow},
	)
	require.NoError(t, err)
	require.Len(t, list.ContactFlowAliasSummaryList, 1)
	assert.Equal(t, "prod", aws.ToString(list.ContactFlowAliasSummaryList[0].AliasName))

	_, err = f.client.DeleteContactFlowVersion(t.Context(), &sdk.DeleteContactFlowVersionInput{
		InstanceId: inst, ContactFlowId: flow, ContactFlowVersion: aws.Int64(1),
	})
	require.NoError(t, err)

	_, err = f.client.DeleteContactFlowAlias(
		t.Context(),
		&sdk.DeleteContactFlowAliasInput{InstanceId: inst, ContactFlowId: flow, AliasId: alias.Id},
	)
	require.NoError(t, err)
	_, err = f.client.DescribeContactFlowAlias(
		t.Context(),
		&sdk.DescribeContactFlowAliasInput{InstanceId: inst, ContactFlowId: flow, AliasId: alias.Id},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.DeleteContactFlow(t.Context(), &sdk.DeleteContactFlowInput{InstanceId: inst, ContactFlowId: flow})
	require.NoError(t, err)
	_, err = f.client.ListContactFlowVersions(
		t.Context(),
		&sdk.ListContactFlowVersionsInput{InstanceId: inst, ContactFlowId: flow},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestContactFlowModuleVersionsAndAliases(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "modver"))

	mod, err := f.client.CreateContactFlowModule(t.Context(), &sdk.CreateContactFlowModuleInput{
		InstanceId: inst, Name: aws.String("mod"), Content: aws.String(validFlow),
	})
	require.NoError(t, err)

	v2, err := f.client.CreateContactFlowModuleVersion(t.Context(), &sdk.CreateContactFlowModuleVersionInput{
		InstanceId: inst, ContactFlowModuleId: mod.Id, Description: aws.String("two"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), aws.ToInt64(v2.Version))
	assert.Equal(t, aws.ToString(mod.Arn), aws.ToString(v2.ContactFlowModuleArn))

	versions, err := f.client.ListContactFlowModuleVersions(t.Context(),
		&sdk.ListContactFlowModuleVersionsInput{InstanceId: inst, ContactFlowModuleId: mod.Id})
	require.NoError(t, err)
	require.Len(t, versions.ContactFlowModuleVersionSummaryList, 2)
	assert.Equal(t, "two", aws.ToString(versions.ContactFlowModuleVersionSummaryList[1].VersionDescription))

	alias, err := f.client.CreateContactFlowModuleAlias(t.Context(), &sdk.CreateContactFlowModuleAliasInput{
		InstanceId:               inst,
		ContactFlowModuleId:      mod.Id,
		AliasName:                aws.String("live"),
		ContactFlowModuleVersion: aws.Int64(2),
	})
	require.NoError(t, err)

	_, err = f.client.DeleteContactFlowModuleVersion(t.Context(), &sdk.DeleteContactFlowModuleVersionInput{
		InstanceId: inst, ContactFlowModuleId: mod.Id, ContactFlowModuleVersion: aws.Int64(2),
	})
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	_, err = f.client.UpdateContactFlowModuleAlias(t.Context(), &sdk.UpdateContactFlowModuleAliasInput{
		InstanceId: inst, ContactFlowModuleId: mod.Id, AliasId: alias.Id, ContactFlowModuleVersion: aws.Int64(1),
	})
	require.NoError(t, err)

	dst := f.restored(t)
	got, err := dst.client.DescribeContactFlowModuleAlias(t.Context(), &sdk.DescribeContactFlowModuleAliasInput{
		InstanceId: inst, ContactFlowModuleId: mod.Id, AliasId: alias.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), aws.ToInt64(got.ContactFlowModuleAlias.Version))
	assert.Equal(t, aws.ToString(mod.Id), aws.ToString(got.ContactFlowModuleAlias.ContactFlowModuleId))

	list, err := f.client.ListContactFlowModuleAliases(t.Context(),
		&sdk.ListContactFlowModuleAliasesInput{InstanceId: inst, ContactFlowModuleId: mod.Id})
	require.NoError(t, err)
	assert.Len(t, list.ContactFlowModuleAliasSummaryList, 1)

	_, err = f.client.DeleteContactFlowModuleVersion(t.Context(), &sdk.DeleteContactFlowModuleVersionInput{
		InstanceId: inst, ContactFlowModuleId: mod.Id, ContactFlowModuleVersion: aws.Int64(2),
	})
	require.NoError(t, err)

	_, err = f.client.DeleteContactFlowModuleAlias(t.Context(), &sdk.DeleteContactFlowModuleAliasInput{
		InstanceId: inst, ContactFlowModuleId: mod.Id, AliasId: alias.Id,
	})
	require.NoError(t, err)
	_, err = f.client.DeleteContactFlowModuleAlias(t.Context(), &sdk.DeleteContactFlowModuleAliasInput{
		InstanceId: inst, ContactFlowModuleId: mod.Id, AliasId: alias.Id,
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestDefaultContactFlows(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "defaultflows"))

	list, err := f.client.ListContactFlows(t.Context(), &sdk.ListContactFlowsInput{InstanceId: inst})
	require.NoError(t, err)

	got := map[string]string{}
	for _, s := range list.ContactFlowSummaryList {
		got[aws.ToString(s.Name)] = string(s.ContactFlowType)

		assert.Equal(t, "PUBLISHED", string(s.ContactFlowStatus))
		assert.Equal(t, "ACTIVE", string(s.ContactFlowState))
	}

	assert.Equal(t, map[string]string{
		"Default agent hold":                             "AGENT_HOLD",
		"Default agent transfer":                         "AGENT_TRANSFER",
		"Default agent whisper":                          "AGENT_WHISPER",
		"Default customer hold":                          "CUSTOMER_HOLD",
		"Default customer queue":                         "CUSTOMER_QUEUE",
		"Default customer whisper":                       "CUSTOMER_WHISPER",
		"Default outbound":                               "OUTBOUND_WHISPER",
		"Default queue transfer":                         "QUEUE_TRANSFER",
		"Sample inbound flow (first contact experience)": "CONTACT_FLOW",
	}, got)

	versions, err := f.client.ListContactFlowVersions(t.Context(), &sdk.ListContactFlowVersionsInput{
		InstanceId: inst, ContactFlowId: list.ContactFlowSummaryList[0].Id,
	})
	require.NoError(t, err)
	assert.Len(t, versions.ContactFlowVersionSummaryList, 1)

	_, err = f.client.CreateContactFlow(t.Context(), &sdk.CreateContactFlowInput{
		InstanceId: inst, Name: aws.String("Default agent hold"), Type: "AGENT_HOLD", Content: aws.String(validFlow),
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))
}
