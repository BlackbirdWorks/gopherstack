package codegurureviewer

import (
	"bytes"
	"errors"
	"io"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

const configPresentWithErrors = "PresentWithErrors"

type reviewerConfig struct {
	ExcludeFiles []string `yaml:"excludeFiles"`
}

// parseConfigFile reads aws-codeguru-reviewer.yml; ok is false when the file is not valid YAML of the documented shape.
func parseConfigFile(content []byte) (reviewerConfig, bool) {
	var cfg reviewerConfig

	dec := yaml.NewDecoder(bytes.NewReader(content))
	dec.KnownFields(false)

	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return reviewerConfig{}, false
	}

	for _, p := range cfg.ExcludeFiles {
		if _, err := compileGlob(p); err != nil {
			return reviewerConfig{}, false
		}
	}

	return cfg, true
}

// compileGlob turns a glob (** crosses directories, * and ? stay within one) into an anchored regexp.
func compileGlob(pattern string) (*regexp.Regexp, error) {
	var sb strings.Builder

	sb.WriteString("^")

	for i := 0; i < len(pattern); i++ {
		switch c := pattern[i]; c {
		case '*':
			switch {
			case strings.HasPrefix(pattern[i:], "**/"):
				sb.WriteString("(?:.*/)?")

				i += 2
			case strings.HasPrefix(pattern[i:], "**"):
				sb.WriteString(".*")

				i++
			default:
				sb.WriteString("[^/]*")
			}
		case '?':
			sb.WriteString("[^/]")
		default:
			sb.WriteString(regexp.QuoteMeta(string(c)))
		}
	}

	sb.WriteString("$")

	return regexp.Compile(sb.String())
}

// splitExcluded separates the files an excludeFiles pattern matches from the ones to analyze.
func splitExcluded(files []sourceFile, patterns []string) ([]sourceFile, []sourceFile) {
	var kept, excluded []sourceFile

	res := make([]*regexp.Regexp, 0, len(patterns))

	for _, p := range patterns {
		if re, err := compileGlob(strings.TrimPrefix(p, "/")); err == nil {
			res = append(res, re)
		}
	}

	for _, f := range files {
		skip := false

		for _, re := range res {
			if re.MatchString(f.Path) {
				skip = true

				break
			}
		}

		if skip {
			excluded = append(excluded, f)
		} else {
			kept = append(kept, f)
		}
	}

	return kept, excluded
}

func configFileContent(files []sourceFile) ([]byte, bool) {
	for _, f := range files {
		if f.Path == configFileName {
			return f.Content, true
		}
	}

	return nil, false
}
