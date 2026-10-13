package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanRoomsSchemaFromGlueWiring(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	g := glue.NewFromConfig(fx.cfg)
	cr := cleanrooms.NewFromConfig(fx.cfg)

	_, err := g.CreateDatabase(
		t.Context(),
		&glue.CreateDatabaseInput{DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String("db")}},
	)
	require.NoError(t, err)

	_, err = g.CreateTable(t.Context(), &glue.CreateTableInput{
		DatabaseName: aws.String("db"),
		TableInput: &gluetypes.TableInput{
			Name: aws.String("people"),
			StorageDescriptor: &gluetypes.StorageDescriptor{Columns: []gluetypes.Column{
				{Name: aws.String("id"), Type: aws.String("bigint")},
				{Name: aws.String("email"), Type: aws.String("string")},
			}},
		},
	})
	require.NoError(t, err)

	collab, err := cr.CreateCollaboration(t.Context(), &cleanrooms.CreateCollaborationInput{
		Name:                   aws.String("c"),
		Description:            aws.String("d"),
		CreatorDisplayName:     aws.String("me"),
		CreatorMemberAbilities: []crtypes.MemberAbility{crtypes.MemberAbilityCanQuery},
		Members:                []crtypes.MemberSpecification{},
		QueryLogStatus:         crtypes.CollaborationQueryLogStatusDisabled,
	})
	require.NoError(t, err)

	mem, err := cr.CreateMembership(t.Context(), &cleanrooms.CreateMembershipInput{
		CollaborationIdentifier: collab.Collaboration.Id, QueryLogStatus: crtypes.MembershipQueryLogStatusDisabled,
	})
	require.NoError(t, err)

	ct, err := cr.CreateConfiguredTable(t.Context(), &cleanrooms.CreateConfiguredTableInput{
		Name: aws.String(
			"ct",
		),
		AllowedColumns: []string{"id", "email"},
		AnalysisMethod: crtypes.AnalysisMethodDirectQuery,
		TableReference: &crtypes.TableReferenceMemberGlue{Value: crtypes.GlueTableReference{
			DatabaseName: aws.String("db"), TableName: aws.String("people"), Region: crtypes.CommercialRegionUsEast1,
		}},
	})
	require.NoError(t, err)

	_, err = cr.CreateConfiguredTableAssociation(t.Context(), &cleanrooms.CreateConfiguredTableAssociationInput{
		Name: aws.String("people-assoc"), MembershipIdentifier: mem.Membership.Id,
		ConfiguredTableIdentifier: ct.ConfiguredTable.Id, RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
	})
	require.NoError(t, err)

	got, err := cr.GetSchema(t.Context(), &cleanrooms.GetSchemaInput{
		CollaborationIdentifier: collab.Collaboration.Id, Name: aws.String("people-assoc"),
	})
	require.NoError(t, err)
	require.Len(t, got.Schema.Columns, 2)
	assert.Equal(t, "bigint", aws.ToString(got.Schema.Columns[0].Type))
	assert.Equal(t, "string", aws.ToString(got.Schema.Columns[1].Type))
}
