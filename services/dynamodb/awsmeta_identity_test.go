package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// TestImportTable_UsesAwsmetaIdentity verifies the backend builds ARNs from the
// per-request awsmeta identity (region + X-Amz-Account-Id override) rather than
// only the startup defaults.
func TestImportTable_UsesAwsmetaIdentity(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	ctx := awsmeta.Set(t.Context(), &awsmeta.Metadata{
		Region:  "eu-central-1",
		Account: "111122223333",
	})

	out, err := db.ImportTable(ctx, &sdk.ImportTableInput{
		S3BucketSource:          &ddbtypes.S3BucketSource{S3Bucket: aws.String("b")},
		TableCreationParameters: importCreationParams("IdentityTbl"),
	})
	require.NoError(t, err)

	arn := aws.ToString(out.ImportTableDescription.TableArn)
	assert.Contains(t, arn, "eu-central-1", "ARN should use awsmeta region: %s", arn)
	assert.Contains(t, arn, "111122223333", "ARN should use awsmeta account: %s", arn)
}
