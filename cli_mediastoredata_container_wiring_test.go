package main

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/mediastore"
	"github.com/aws/aws-sdk-go-v2/service/mediastoredata"
	msdtypes "github.com/aws/aws-sdk-go-v2/service/mediastoredata/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMediaStoreDataContainerWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		container string
		create    bool
		wantErr   bool
	}{
		{name: "existing_container", container: "media1", create: true},
		{name: "missing_container", container: "ghost", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)

			if tt.create {
				_, err := mediastore.NewFromConfig(fx.cfg).CreateContainer(t.Context(),
					&mediastore.CreateContainerInput{ContainerName: aws.String(tt.container)})
				require.NoError(t, err)
			}

			c := mediastoredata.NewFromConfig(fx.cfg, func(o *mediastoredata.Options) {
				o.BaseEndpoint = aws.String("http://" + tt.container + ".data.mediastore.us-east-1.amazonaws.com")
			})

			_, err := c.PutObject(t.Context(), &mediastoredata.PutObjectInput{
				Path: aws.String("a/b.txt"), Body: strings.NewReader("hi"),
			})

			if tt.wantErr {
				var nf *msdtypes.ContainerNotFoundException
				require.ErrorAs(t, err, &nf)

				return
			}

			require.NoError(t, err)

			out, err := c.ListItems(t.Context(), &mediastoredata.ListItemsInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, out.Items)
		})
	}
}
