package servicecatalog_test

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/servicecatalog"
)

func TestProductSourceConnectionLogsWouldSync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		conn *servicecatalog.SourceConnectionRequest
		name string
		want bool
	}{
		{
			name: "with connection",
			conn: &servicecatalog.SourceConnectionRequest{
				Type: "CODESTAR",
				ConnectionParameters: &servicecatalog.SourceConnectionParameters{
					CodeStar: &servicecatalog.CodeStarParameters{
						ConnectionArn: "arn:aws:codestar-connections:us-east-1:000000000000:connection/c1",
						Repository:    "org/repo", Branch: "main", ArtifactPath: "t.yaml",
					},
				},
			},
			want: true,
		},
		{name: "without connection"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)

			var buf bytes.Buffer

			ctx := logger.Save(t.Context(), slog.New(slog.NewTextHandler(&buf, nil)))
			_, err := f.backend.CreateProduct(ctx, &servicecatalog.CreateProductInput{
				Name: "p", Owner: "o", ProductType: "CLOUD_FORMATION_TEMPLATE", IdempotencyToken: token(t),
				SourceConnection: tt.conn,
				ProvisioningArtifactParameters: &servicecatalog.ProvisioningArtifactProperties{
					Name: "v1", Type: "CLOUD_FORMATION_TEMPLATE",
					Info: map[string]string{"LoadTemplateFromURL": templateURL},
				},
			})
			require.NoError(t, err)

			if tt.want {
				assert.Contains(t, buf.String(), "would sync CodeStar repository")
				assert.Contains(t, buf.String(), "org/repo")

				return
			}

			assert.NotContains(t, buf.String(), "would sync")
		})
	}
}
