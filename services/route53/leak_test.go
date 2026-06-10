package route53_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// TestDeleteHealthCheck_ReleasesTags verifies that deleting a health check
// drops its handler-level tags, so the tags map cannot retain entries for
// resources that no longer exist.
func TestDeleteHealthCheck_ReleasesTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tagAdds   string
		wantAfter int
	}{
		{
			name: "tagged health check",
			tagAdds: `<?xml version="1.0" encoding="UTF-8"?>
<ChangeTagsForResourceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <AddTags>
    <Tag><Key>env</Key><Value>test</Value></Tag>
  </AddTags>
</ChangeTagsForResourceRequest>`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := route53.NewInMemoryBackend()
			h := route53.NewHandler(backend)

			baseline := route53.TagResourceCount(h)

			hc, err := backend.CreateHealthCheck("ref-1", route53.HealthCheckConfig{
				Type:                     route53.HealthCheckTypeHTTP,
				FullyQualifiedDomainName: "example.com",
				Port:                     80,
			})
			require.NoError(t, err)

			tagRec := send(t, h, http.MethodPost, "/2013-04-01/tags/healthcheck/"+hc.ID, tc.tagAdds)
			require.Equal(t, http.StatusOK, tagRec.Code)
			require.Greater(t, route53.TagResourceCount(h), baseline,
				"tag should be recorded before delete")

			delRec := send(t, h, http.MethodDelete, "/2013-04-01/healthcheck/"+hc.ID, "")
			require.Equal(t, http.StatusOK, delRec.Code)

			require.Equal(t, baseline, route53.TagResourceCount(h),
				"health-check tags must be released when the health check is deleted")
		})
	}
}
