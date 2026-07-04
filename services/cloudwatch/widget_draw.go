package cloudwatch

import (
	"image"
	"image/color"
	"math"
	"strconv"
	"strings"
	"time"
)

// Widget layout constants (all in image pixels).
const (
	titleX          = widgetMarginLeft
	titleY          = 8
	axisLabelX      = 2
	axisLabelInset  = 2
	legendStep      = 90
	legendSwatchW   = 8
	legendSwatchH   = 6
	legendYOffset   = 8
	legendTextGap   = 12
	legendRightPad  = 80
	noDataTextInset = 10
	markerHalf      = 1
	halfDivisor     = 2
	bresenhamScale  = 2
	flatSeriesPad   = 1
)

// widgetTheme holds the colours used to render a widget. It replaces package-level
// colour globals so the renderer carries no mutable global state.
type widgetTheme struct {
	series     []color.RGBA
	background color.RGBA
	frame      color.RGBA
	grid       color.RGBA
	text       color.RGBA
}

// defaultWidgetTheme returns the standard light theme and series colour cycle.
func defaultWidgetTheme() widgetTheme {
	return widgetTheme{
		background: color.RGBA{0xff, 0xff, 0xff, 0xff},
		frame:      color.RGBA{0x33, 0x33, 0x33, 0xff},
		grid:       color.RGBA{0xe0, 0xe0, 0xe0, 0xff},
		text:       color.RGBA{0x22, 0x22, 0x22, 0xff},
		series: []color.RGBA{
			{0x1f, 0x77, 0xb4, 0xff},
			{0xff, 0x7f, 0x0e, 0xff},
			{0x2c, 0xa0, 0x2c, 0xff},
			{0xd6, 0x27, 0x28, 0xff},
			{0x94, 0x67, 0xbd, 0xff},
			{0x8c, 0x56, 0x4b, 0xff},
		},
	}
}

// drawWidget renders the full metric widget into an RGBA image: a titled, framed
// plot with horizontal gridlines, Y-axis min/max/mid labels, one coloured polyline
// per series, and a legend.
func drawWidget(
	width, height int,
	title string,
	series [][]widgetPoint,
	labels []string,
) *image.RGBA {
	theme := defaultWidgetTheme()

	img := image.NewRGBA(image.Rect(0, 0, width, height))
	fillRect(img, 0, 0, width, height, theme.background)

	plot := image.Rect(
		widgetMarginLeft,
		widgetMarginTop,
		width-widgetMarginRight,
		height-widgetMarginBottom,
	)

	// Guard against pathologically small canvases where the plot area collapses.
	if plot.Dx() < bresenhamScale || plot.Dy() < bresenhamScale {
		return img
	}

	if title != "" {
		drawText(img, strings.ToUpper(title), titleX, titleY, theme.text)
	}

	bounds := seriesBounds(series)

	drawGrid(img, plot, bounds, theme)

	drawRect(img, plot.Min.X, plot.Min.Y, plot.Max.X, plot.Max.Y, theme.frame)

	if !bounds.ok {
		drawText(
			img, "NO DATA",
			plot.Min.X+(plot.Dx()/halfDivisor)-noDataTextInset,
			plot.Min.Y+(plot.Dy()/halfDivisor),
			theme.text,
		)

		return img
	}

	for i, pts := range series {
		col := theme.series[i%len(theme.series)]
		drawSeries(img, plot, pts, bounds, col)
		drawLegendEntry(img, plot, width, height, i, labels, col, theme)
	}

	return img
}

// plotBounds is the resolved value/time extent of a set of series.
type plotBounds struct {
	minT, maxT time.Time
	minV, maxV float64
	ok         bool
}

// seriesBounds computes the global value and time extents across all series.
func seriesBounds(series [][]widgetPoint) plotBounds {
	b := plotBounds{minV: math.MaxFloat64, maxV: -math.MaxFloat64}

	for _, pts := range series {
		for _, p := range pts {
			b.ok = true
			b.minV = math.Min(b.minV, p.v)
			b.maxV = math.Max(b.maxV, p.v)
			if b.minT.IsZero() || p.t.Before(b.minT) {
				b.minT = p.t
			}
			if b.maxT.IsZero() || p.t.After(b.maxT) {
				b.maxT = p.t
			}
		}
	}

	if !b.ok {
		return plotBounds{}
	}

	// Pad a flat series so the line renders mid-plot rather than on an edge.
	if b.minV == b.maxV {
		b.minV -= flatSeriesPad
		b.maxV += flatSeriesPad
	}

	return b
}

// drawGrid renders horizontal gridlines and their Y-axis value labels.
func drawGrid(img *image.RGBA, plot image.Rectangle, bounds plotBounds, theme widgetTheme) {
	for i := range widgetGridLines + 1 {
		frac := float64(i) / float64(widgetGridLines)
		y := plot.Max.Y - int(frac*float64(plot.Dy()))
		drawHLine(img, plot.Min.X, plot.Max.X, y, theme.grid)

		if bounds.ok {
			val := bounds.minV + frac*(bounds.maxV-bounds.minV)
			drawText(img, formatAxisValue(val), axisLabelX, y-axisLabelInset, theme.text)
		}
	}
}

// drawSeries plots a single series as a connected polyline scaled to the plot area.
func drawSeries(
	img *image.RGBA,
	plot image.Rectangle,
	pts []widgetPoint,
	bounds plotBounds,
	col color.RGBA,
) {
	spanT := bounds.maxT.Sub(bounds.minT).Seconds()
	spanV := bounds.maxV - bounds.minV
	if spanV == 0 {
		spanV = 1
	}

	xOf := func(p widgetPoint) int {
		if spanT <= 0 {
			return plot.Min.X + plot.Dx()/halfDivisor
		}
		frac := p.t.Sub(bounds.minT).Seconds() / spanT

		return plot.Min.X + int(frac*float64(plot.Dx()))
	}
	yOf := func(p widgetPoint) int {
		frac := (p.v - bounds.minV) / spanV

		return plot.Max.Y - int(frac*float64(plot.Dy()))
	}

	if len(pts) == 1 {
		// A single point renders as a small marker.
		x, y := xOf(pts[0]), yOf(pts[0])
		fillRect(img, x-markerHalf, y-markerHalf, x+markerHalf+1, y+markerHalf+1, col)

		return
	}

	for i := 1; i < len(pts); i++ {
		drawLine(img, xOf(pts[i-1]), yOf(pts[i-1]), xOf(pts[i]), yOf(pts[i]), col)
	}
}

// drawLegendEntry renders a colour swatch and label for series i along the bottom.
func drawLegendEntry(
	img *image.RGBA,
	plot image.Rectangle,
	width, height, i int,
	labels []string,
	col color.RGBA,
	theme widgetTheme,
) {
	lx := plot.Min.X + i*legendStep
	ly := height - widgetMarginBottom + legendYOffset
	if lx+legendRightPad >= width {
		return
	}

	fillRect(img, lx, ly, lx+legendSwatchW, ly+legendSwatchH, col)

	label := ""
	if i < len(labels) {
		label = labels[i]
	}
	drawText(img, strings.ToUpper(label), lx+legendTextGap, ly, theme.text)
}

// formatAxisValue renders a Y-axis value compactly.
func formatAxisValue(v float64) string {
	const (
		intPrecision = 0
		gPrecision   = 4
		intLimit     = 1e6
	)

	if v == math.Trunc(v) && math.Abs(v) < intLimit {
		return strconv.FormatFloat(v, 'f', intPrecision, 64)
	}

	return strconv.FormatFloat(v, 'g', gPrecision, 64)
}

// --- primitive drawing helpers ---

func setPixel(img *image.RGBA, x, y int, col color.RGBA) {
	if !(image.Point{X: x, Y: y}).In(img.Bounds()) {
		return
	}
	img.SetRGBA(x, y, col)
}

func fillRect(img *image.RGBA, x0, y0, x1, y1 int, col color.RGBA) {
	for y := y0; y < y1; y++ {
		for x := x0; x < x1; x++ {
			setPixel(img, x, y, col)
		}
	}
}

func drawHLine(img *image.RGBA, x0, x1, y int, col color.RGBA) {
	for x := x0; x <= x1; x++ {
		setPixel(img, x, y, col)
	}
}

func drawVLine(img *image.RGBA, x, y0, y1 int, col color.RGBA) {
	for y := y0; y <= y1; y++ {
		setPixel(img, x, y, col)
	}
}

func drawRect(img *image.RGBA, x0, y0, x1, y1 int, col color.RGBA) {
	drawHLine(img, x0, x1, y0, col)
	drawHLine(img, x0, x1, y1, col)
	drawVLine(img, x0, y0, y1, col)
	drawVLine(img, x1, y0, y1, col)
}

// drawLine draws a line using Bresenham's algorithm.
func drawLine(img *image.RGBA, x0, y0, x1, y1 int, col color.RGBA) {
	dx := abs(x1 - x0)
	dy := -abs(y1 - y0)
	sx := 1
	if x0 > x1 {
		sx = -1
	}
	sy := 1
	if y0 > y1 {
		sy = -1
	}
	errAcc := dx + dy

	for {
		setPixel(img, x0, y0, col)
		if x0 == x1 && y0 == y1 {
			break
		}
		e2 := bresenhamScale * errAcc
		if e2 >= dy {
			errAcc += dy
			x0 += sx
		}
		if e2 <= dx {
			errAcc += dx
			y0 += sy
		}
	}
}

func abs(n int) int {
	if n < 0 {
		return -n
	}

	return n
}

// drawText renders a string at (x, y) using the built-in 3x5 bitmap font. y is the
// top of the glyph row.
func drawText(img *image.RGBA, s string, x, y int, col color.RGBA) {
	cursor := x
	for _, r := range s {
		glyph, ok := glyph3x5(r)
		if !ok {
			glyph, _ = glyph3x5('#')
		}
		for row := range glyphH {
			bits := glyph[row]
			for c := range glyphW {
				if bits&(1<<(glyphW-1-c)) != 0 {
					px := cursor + c*fontPixel
					py := y + row*fontPixel
					fillRect(img, px, py, px+fontPixel, py+fontPixel, col)
				}
			}
		}
		cursor += (glyphW + glyphSpace) * fontPixel
	}
}
