package amp_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/aws/aws-sdk-go-v2/service/amp/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func TestScraperWouldScrapeLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		update bool
		del    bool
		scrape int
		stop   int
	}{
		{name: "create", scrape: 1},
		{name: "update", update: true, scrape: 2},
		{name: "delete", del: true, scrape: 1, stop: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			wsARN := workspaceARN(t, f)

			var buf bytes.Buffer

			ctx := logger.Save(t.Context(), slog.New(slog.NewTextHandler(&buf, nil)))

			c, err := f.client.CreateScraper(t.Context(), scraperInput(wsARN))
			require.NoError(t, err)

			f.backend.Tick(ctx)
			assert.NotContains(t, buf.String(), "would scrape")

			f.settle()
			f.backend.Tick(ctx)
			f.backend.Tick(ctx)

			if tt.update {
				_, err = f.client.UpdateScraper(t.Context(),
					&ampsdk.UpdateScraperInput{ScraperId: c.ScraperId, Alias: aws.String("renamed")})
				require.NoError(t, err)
				f.settle()
				f.backend.Tick(ctx)
			}

			if tt.del {
				_, err = f.backend.DeleteScraper(ctx, aws.ToString(c.ScraperId))
				require.NoError(t, err)

				d, derr := f.client.DescribeScraper(t.Context(), &ampsdk.DescribeScraperInput{ScraperId: c.ScraperId})
				require.NoError(t, derr)
				assert.Equal(t, types.ScraperStatusCodeDeleting, d.Scraper.Status.StatusCode)

				f.settle()
				f.backend.Tick(ctx)
			}

			out := buf.String()
			assert.Equal(t, tt.scrape, strings.Count(out, "would scrape source"))
			assert.Equal(t, tt.stop, strings.Count(out, "would stop scraping source"))
			assert.Contains(t, out, eksARN)
		})
	}
}
