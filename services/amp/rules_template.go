package amp

import (
	"bytes"
	"fmt"
	"math"
	"regexp"
	"strings"
	"text/template"
	"unicode"
)

const (
	templatePrefix  = "{{$labels := .Labels}}{{$value := .Value}}{{$externalLabels := .ExternalLabels}}"
	maxTemplateSize = 4096
	maxExpanded     = 64 * 1024
	percent         = 100
	secondsPerMin   = 60
	minPerHour      = 60
	hoursPerDay     = 24
	siStep          = 1000.0
	iecStep         = 1024.0
)

var templateFuncs = template.FuncMap{ //nolint:gochecknoglobals // immutable lookup
	"humanize":           func(v float64) string { return humanizeScaled(v, siStep, siPrefixes) },
	"humanize1024":       func(v float64) string { return humanizeScaled(v, iecStep, iecPrefixes) },
	"humanizePercentage": func(v float64) string { return fmt.Sprintf("%.4g%%", v*percent) },
	"humanizeDuration":   humanizeDuration,
	"toUpper":            strings.ToUpper,
	"toLower":            strings.ToLower,
	"title":              titleCase,
	"match":              regexp.MatchString,
	"reReplaceAll": func(pattern, repl, s string) (string, error) {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return "", err
		}

		return re.ReplaceAllString(s, repl), nil
	},
}

var (
	siPrefixes  = []string{"", "k", "M", "G", "T", "P", "E", "Z", "Y"}         //nolint:gochecknoglobals // immutable
	iecPrefixes = []string{"", "ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi"} //nolint:gochecknoglobals // immutable
)

type templateData struct {
	Labels         map[string]string
	ExternalLabels map[string]string
	Value          float64
}

func titleCase(s string) string {
	prev := ' '

	return strings.Map(func(r rune) rune {
		out := r
		if unicode.IsSpace(prev) {
			out = unicode.ToTitle(r)
		}

		prev = r

		return out
	}, s)
}

func humanizeScaled(v, step float64, prefixes []string) string {
	if v == 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Sprintf("%.4g", v)
	}

	if math.Abs(v) >= 1 {
		i := 0
		for ; math.Abs(v) >= step && i < len(prefixes)-1; i++ {
			v /= step
		}

		return fmt.Sprintf("%.4g%s", v, prefixes[i])
	}

	return fmt.Sprintf("%.4g", v)
}

func humanizeDuration(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Sprintf("%.4g", v)
	}

	if v == 0 {
		return "0s"
	}

	if math.Abs(v) < 1 {
		return fmt.Sprintf("%.4gs", v)
	}

	secs := int64(math.Abs(v))
	sign := ""

	if v < 0 {
		sign = "-"
	}

	mins := secs / secondsPerMin
	hours := mins / minPerHour
	days := hours / hoursPerDay

	if days > 0 {
		return fmt.Sprintf("%s%dd %dh %dm %ds", sign, days, hours%hoursPerDay, mins%minPerHour, secs%secondsPerMin)
	}

	if hours > 0 {
		return fmt.Sprintf("%s%dh %dm %ds", sign, hours, mins%minPerHour, secs%secondsPerMin)
	}

	if mins > 0 {
		return fmt.Sprintf("%s%dm %ds", sign, mins, secs%secondsPerMin)
	}

	return fmt.Sprintf("%s%ds", sign, secs)
}

// expandTemplate renders a rule label or annotation; a template that fails to parse or run is returned verbatim.
func expandTemplate(text string, data templateData) string {
	if !strings.Contains(text, "{{") || len(text) > maxTemplateSize {
		return text
	}

	tmpl, err := template.New("rule").Funcs(templateFuncs).Option("missingkey=zero").Parse(templatePrefix + text)
	if err != nil {
		return text
	}

	var buf bytes.Buffer

	if xerr := tmpl.Execute(&limitedWriter{buf: &buf}, data); xerr != nil {
		return text
	}

	return buf.String()
}

type limitedWriter struct{ buf *bytes.Buffer }

func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.buf.Len()+len(p) > maxExpanded {
		return 0, errTemplateTooLarge
	}

	return w.buf.Write(p)
}
