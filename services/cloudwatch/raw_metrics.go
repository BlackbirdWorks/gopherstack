package cloudwatch

import (
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"
)

// RawMetric is one single-value datapoint in the LocalStack /_aws/cloudwatch/metrics/raw shape.
type RawMetric struct {
	Namespace  string  `json:"ns"`
	Name       string  `json:"n"`
	Dimensions *string `json:"d"`
	Account    string  `json:"account"`
	Region     string  `json:"region"`
	Value      float64 `json:"v"`
	Timestamp  int64   `json:"t"`
}

// RawMetrics returns every single-value datapoint; statistic-set and values-array points are omitted.
func (b *InMemoryBackend) RawMetrics() []RawMetric {
	b.mu.RLock("RawMetrics")
	defer b.mu.RUnlock()

	out := make([]RawMetric, 0)

	for ns, series := range b.metrics {
		for _, rec := range series {
			for i := range rec.Points {
				pt := &rec.Points[i]
				if pt.HasStatisticSet || pt.HasValuesArray {
					continue
				}

				out = append(out, RawMetric{
					Namespace:  ns,
					Name:       rec.MetricName,
					Value:      pt.Value,
					Timestamp:  pt.Timestamp.Unix(),
					Dimensions: rawDimensions(rec.Dimensions),
					Account:    b.accountID,
					Region:     b.region,
				})
			}
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Timestamp != out[j].Timestamp {
			return out[i].Timestamp < out[j].Timestamp
		}

		return out[i].Namespace+out[i].Name < out[j].Namespace+out[j].Name
	})

	return out
}

// rawDimensions renders dims as tab-separated "Name=Value" pairs, or nil when empty.
func rawDimensions(dims []Dimension) *string {
	if len(dims) == 0 {
		return nil
	}

	parts := make([]string, 0, len(dims))
	for _, d := range dims {
		parts = append(parts, d.Name+"="+d.Value)
	}

	sort.Strings(parts)
	s := strings.Join(parts, "\t")

	return &s
}

// ServeRawMetrics serves GET /_aws/cloudwatch/metrics/raw.
func (h *Handler) ServeRawMetrics(c *echo.Context) error {
	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return c.JSON(http.StatusOK, map[string]any{"metrics": []RawMetric{}})
	}

	return c.JSON(http.StatusOK, map[string]any{"metrics": b.RawMetrics()})
}
