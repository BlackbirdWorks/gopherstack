package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
)

func TestWireFirehoseGlue(t *testing.T) {
	t.Parallel()

	glueBk := gluebackend.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := glueBk.CreateDatabase(gluebackend.DatabaseInput{Name: "db"}, nil)
	require.NoError(t, err)

	_, err = glueBk.CreateTable("db", gluebackend.TableInput{
		Name: "t",
		StorageDescriptor: gluebackend.StorageDescriptor{
			Columns: []gluebackend.Column{{Name: "id", Type: "bigint"}, {Name: "name", Type: "string"}},
		},
	})
	require.NoError(t, err)

	fhBk := firehosebackend.NewInMemoryBackend("000000000000", "us-east-1")
	wireFirehoseGlue(firehosebackend.NewHandler(fhBk), gluebackend.NewHandler(glueBk))

	src := &firehoseGlueSchema{backend: glueBk}
	cols, err := src.TableColumns("", "db", "t")
	require.NoError(t, err)
	assert.Equal(t, []firehosebackend.SchemaColumn{{Name: "id", Type: "bigint"}, {Name: "name", Type: "string"}}, cols)

	_, err = src.TableColumns("", "db", "ghost")
	require.Error(t, err)

	wireFirehoseGlue(nil, nil)
}
