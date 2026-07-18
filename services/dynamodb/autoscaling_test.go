package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateTableReplicaAutoScaling_TableNotFound(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)

	_, err := db.UpdateTableReplicaAutoScaling(t.Context(), &sdk.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String("NoTable"),
	})
	require.Error(t, err)
}

func TestUpdateTableReplicaAutoScaling_EmptyName(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)

	_, err := db.UpdateTableReplicaAutoScaling(t.Context(), &sdk.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String(""),
	})
	require.Error(t, err)
}

func TestUpdateTableReplicaAutoScaling_ReturnsDescription(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)
	createSimplePPRTable(t, db, "ASTable")

	out, err := db.UpdateTableReplicaAutoScaling(
		t.Context(),
		&sdk.UpdateTableReplicaAutoScalingInput{
			TableName: aws.String("ASTable"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.TableAutoScalingDescription)
	assert.Equal(t, "ASTable", aws.ToString(out.TableAutoScalingDescription.TableName))
}
