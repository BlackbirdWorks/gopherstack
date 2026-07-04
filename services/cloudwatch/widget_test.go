package cloudwatch_test

import (
	"bytes"
	"image/png"
	"testing"
	"time"

	cloudwatch "github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

func TestParseRelativeDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     string
		want   time.Duration
		wantOK bool
	}{
		{"minus 3 hours", "-PT3H", -3 * time.Hour, true},
		{"minus 90 min", "-PT90M", -90 * time.Minute, true},
		{"combined", "-PT1H30M", -(time.Hour + 30*time.Minute), true},
		{"one day", "-P1D", -24 * time.Hour, true},
		{"zero", "PT0H", 0, true},
		{"positive hours", "PT2H", 2 * time.Hour, true},
		{"seconds", "-PT45S", -45 * time.Second, true},
		{"not a duration", "banana", 0, false},
		{"missing P", "T3H", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := cloudwatch.ParseRelativeDurationForTest(tt.in)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Fatalf("duration = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRenderMetricWidgetPNGDimensions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		widget string
		wantW  int
		wantH  int
	}{
		{
			name:   "explicit dimensions",
			widget: `{"width":300,"height":200,"metrics":[["MyApp","Latency"]]}`,
			wantW:  300,
			wantH:  200,
		},
		{
			name:   "default dimensions",
			widget: `{"metrics":[["MyApp","Latency"]]}`,
			wantW:  600,
			wantH:  400,
		},
		{
			name:   "empty widget still renders",
			widget: ``,
			wantW:  600,
			wantH:  400,
		},
		{
			name:   "malformed json falls back to defaults",
			widget: `{not valid`,
			wantW:  600,
			wantH:  400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			raw, err := cloudwatch.RenderMetricWidgetPNGForTest(b, tt.widget, time.Now().UTC())
			if err != nil {
				t.Fatalf("render: %v", err)
			}

			img, err := png.Decode(bytes.NewReader(raw))
			if err != nil {
				t.Fatalf("decode png: %v", err)
			}
			bnd := img.Bounds()
			if bnd.Dx() != tt.wantW || bnd.Dy() != tt.wantH {
				t.Fatalf("dimensions = %dx%d, want %dx%d",
					bnd.Dx(), bnd.Dy(), tt.wantW, tt.wantH)
			}
		})
	}
}

func TestRenderMetricWidgetPNGPlotsData(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	now := time.Now().UTC()
	data := make([]cloudwatch.MetricDatum, 0, 6)
	for i := range 6 {
		data = append(data, cloudwatch.MetricDatum{
			MetricName: "Latency",
			Namespace:  "MyApp",
			Timestamp:  now.Add(time.Duration(-6+i) * time.Minute),
			Value:      float64((i%3)*10 + 5),
			Count:      1,
			Sum:        float64((i%3)*10 + 5),
			Min:        float64((i%3)*10 + 5),
			Max:        float64((i%3)*10 + 5),
		})
	}
	if _, err := b.PutMetricData("MyApp", data); err != nil {
		t.Fatalf("PutMetricData: %v", err)
	}

	widget := `{"width":400,"height":250,"title":"Latency",` +
		`"metrics":[["MyApp","Latency",{"stat":"Average","period":60}]]}`

	raw, err := cloudwatch.RenderMetricWidgetPNGForTest(b, widget, now)
	if err != nil {
		t.Fatalf("render: %v", err)
	}

	img, err := png.Decode(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("decode png: %v", err)
	}

	if img.Bounds().Dx() != 400 || img.Bounds().Dy() != 250 {
		t.Fatalf("dimensions = %v, want 400x250", img.Bounds())
	}

	// The plotted line must introduce coloured (non-grayscale) pixels; a blank or
	// purely axis/grid image would be grayscale only.
	coloured := 0
	for y := img.Bounds().Min.Y; y < img.Bounds().Max.Y; y++ {
		for x := img.Bounds().Min.X; x < img.Bounds().Max.X; x++ {
			r, g, bl, _ := img.At(x, y).RGBA()
			if r != g || g != bl {
				coloured++
			}
		}
	}
	if coloured == 0 {
		t.Fatal("expected coloured series pixels in the rendered plot, found none")
	}
}
