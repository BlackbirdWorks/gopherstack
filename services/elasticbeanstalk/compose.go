package elasticbeanstalk

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"gopkg.in/yaml.v3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

var errManifestNotFound = errors.New("environment manifest not found")

const (
	envManifestFile    = "env.yaml"
	groupSuffix        = "+"
	maxManifestBytes   = 1 << 20
	maxSourceBundleLen = 64 << 20
)

// S3Reader is the slice of S3 needed to read application source bundles.
type S3Reader interface {
	GetObject(ctx context.Context, input *awss3.GetObjectInput) (*awss3.GetObjectOutput, error)
}

// siblingServices resolves S3 lazily because handlers are wired after every provider initialises.
type siblingServices interface {
	GetS3Handler() service.Registerable
}

// SetS3Backend wires the S3 backend used to read source bundles.
func (b *InMemoryBackend) SetS3Backend(r S3Reader) {
	b.mu.Lock("SetS3Backend")
	defer b.mu.Unlock()

	b.s3 = r
}

// SetAppConfig records the service.AppContext.Config for lazy sibling lookup.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.mu.Lock("SetAppConfig")
	defer b.mu.Unlock()

	b.appConfig = cfg
}

func (b *InMemoryBackend) s3Reader() (S3Reader, bool) {
	b.mu.RLock("s3Reader")
	defer b.mu.RUnlock()

	if b.s3 != nil {
		return b.s3, true
	}

	s, ok := b.appConfig.(siblingServices)
	if !ok {
		return nil, false
	}

	h, ok := s.GetS3Handler().(*s3backend.S3Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil, false
	}

	return h.Backend, true
}

// ComposeEnvironmentsParams holds ComposeEnvironments' inputs.
type ComposeEnvironmentsParams struct {
	ApplicationName string
	GroupName       string
	VersionLabels   []string
}

// envManifest is the subset of the env.yaml environment manifest used by ComposeEnvironments.
type envManifest struct {
	OptionSettings  any               `yaml:"OptionSettings"`
	EnvironmentLink map[string]string `yaml:"EnvironmentLinks"`
	EnvironmentName string            `yaml:"EnvironmentName"`
	SolutionStack   string            `yaml:"SolutionStack"`
	CName           string            `yaml:"CName"`
	Tier            struct {
		Name string `yaml:"Name"`
		Type string `yaml:"Type"`
	} `yaml:"EnvironmentTier"`
}

type composedEnv struct {
	manifest envManifest
	version  string
	name     string
	links    []EnvironmentLink
}

// ComposeEnvironments creates or updates the environments described by the env.yaml manifests in the
// source bundles of the given application versions.
func (b *InMemoryBackend) ComposeEnvironments(
	ctx context.Context, p ComposeEnvironmentsParams,
) ([]*Environment, error) {
	if len(p.VersionLabels) == 0 {
		return []*Environment{}, nil
	}

	plan, err := b.planCompose(ctx, p)
	if err != nil {
		return nil, err
	}

	out := make([]*Environment, 0, len(plan))

	for _, c := range plan {
		env, applyErr := b.applyComposed(ctx, p.ApplicationName, c)
		if applyErr != nil {
			return nil, applyErr
		}

		out = append(out, env)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].EnvironmentName < out[j].EnvironmentName })

	return out, nil
}

func (b *InMemoryBackend) planCompose(ctx context.Context, p ComposeEnvironmentsParams) ([]composedEnv, error) {
	reader, ok := b.s3Reader()
	if !ok {
		return nil, fmt.Errorf("%w: source bundles are unavailable: S3 is not configured", ErrInvalidParameter)
	}

	plan := make([]composedEnv, 0, len(p.VersionLabels))

	for _, label := range p.VersionLabels {
		ver, found := b.lookupVersion(ctx, p.ApplicationName, label)
		if !found {
			return nil, fmt.Errorf("%w: no Application Version named '%s' found", ErrInvalidParameter, label)
		}

		manifest, err := readEnvManifest(ctx, reader, ver)
		if err != nil {
			return nil, err
		}

		name, err := composedEnvName(manifest.EnvironmentName, p.GroupName)
		if err != nil {
			return nil, err
		}

		links := make([]EnvironmentLink, 0, len(manifest.EnvironmentLink))
		for linkName, target := range manifest.EnvironmentLink {
			resolved, linkErr := composedEnvName(target, p.GroupName)
			if linkErr != nil {
				return nil, linkErr
			}

			links = append(links, EnvironmentLink{LinkName: linkName, EnvironmentName: resolved})
		}

		sort.Slice(links, func(i, j int) bool { return links[i].LinkName < links[j].LinkName })

		plan = append(plan, composedEnv{manifest: manifest, version: label, name: name, links: links})
	}

	return plan, nil
}

func (b *InMemoryBackend) lookupVersion(ctx context.Context, appName, label string) (*ApplicationVersion, bool) {
	b.mu.RLock("ComposeEnvironments")
	defer b.mu.RUnlock()

	ver, ok := b.appVersionGet(getRegion(ctx, b.region), appName, label)
	if !ok {
		return nil, false
	}

	return cloneApplicationVersion(ver), true
}

// composedEnvName resolves a manifest environment name; a trailing "+" is replaced by "-<GroupName>".
func composedEnvName(name, group string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: the environment manifest does not specify an EnvironmentName", ErrInvalidParameter)
	}

	base, grouped := strings.CutSuffix(name, groupSuffix)
	if !grouped {
		return name, nil
	}

	if group == "" {
		return "", fmt.Errorf(
			"%w: environment name '%s' ends with '+' but no GroupName was specified", ErrInvalidParameter, name,
		)
	}

	return base + "-" + group, nil
}

func readEnvManifest(ctx context.Context, reader S3Reader, ver *ApplicationVersion) (envManifest, error) {
	var m envManifest

	if ver.S3Bucket == "" || ver.S3Key == "" {
		return m, fmt.Errorf("%w: application version '%s' has no source bundle", ErrInvalidParameter, ver.VersionLabel)
	}

	obj, err := reader.GetObject(
		ctx,
		&awss3.GetObjectInput{Bucket: aws.String(ver.S3Bucket), Key: aws.String(ver.S3Key)},
	)
	if err != nil || obj.Body == nil {
		return m, fmt.Errorf("%w: unable to read source bundle for version '%s'", ErrInvalidParameter, ver.VersionLabel)
	}

	defer obj.Body.Close()

	data, err := io.ReadAll(io.LimitReader(obj.Body, maxSourceBundleLen))
	if err != nil {
		return m, fmt.Errorf("%w: unable to read source bundle for version '%s'", ErrInvalidParameter, ver.VersionLabel)
	}

	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return m, fmt.Errorf(
			"%w: source bundle for version '%s' is not a zip archive",
			ErrInvalidParameter,
			ver.VersionLabel,
		)
	}

	raw, err := readZipFile(zr, envManifestFile)
	if err != nil {
		return m, fmt.Errorf(
			"%w: source bundle for version '%s' has no %s", ErrInvalidParameter, ver.VersionLabel, envManifestFile,
		)
	}

	if err = yaml.Unmarshal(raw, &m); err != nil {
		return m, fmt.Errorf(
			"%w: invalid %s in version '%s': %w",
			ErrInvalidParameter,
			envManifestFile,
			ver.VersionLabel,
			err,
		)
	}

	return m, nil
}

func readZipFile(zr *zip.Reader, name string) ([]byte, error) {
	for _, f := range zr.File {
		if f.Name != name {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			return nil, err
		}

		defer rc.Close()

		return io.ReadAll(io.LimitReader(rc, maxManifestBytes))
	}

	return nil, errManifestNotFound
}

// manifestOptionSettings flattens the manifest's OptionSettings (namespace map or setting list).
func manifestOptionSettings(raw any) []OptionSetting {
	var out []OptionSetting

	switch v := raw.(type) {
	case map[string]any:
		for ns, opts := range v {
			m, ok := opts.(map[string]any)
			if !ok {
				continue
			}

			for name, val := range m {
				out = append(out, OptionSetting{Namespace: ns, OptionName: name, Value: fmt.Sprint(val)})
			}
		}
	case []any:
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}

			out = append(out, OptionSetting{
				Namespace:    fmt.Sprint(m["Namespace"]),
				OptionName:   fmt.Sprint(m["OptionName"]),
				ResourceName: strings.TrimPrefix(fmt.Sprint(m["ResourceName"]), "<nil>"),
				Value:        strings.TrimPrefix(fmt.Sprint(m["Value"]), "<nil>"),
			})
		}
	}

	sort.Slice(out, func(i, j int) bool { return optionSettingKey(out[i]) < optionSettingKey(out[j]) })

	return out
}

func (b *InMemoryBackend) applyComposed(ctx context.Context, appName string, c composedEnv) (*Environment, error) {
	settings := manifestOptionSettings(c.manifest.OptionSettings)

	existing := b.DescribeEnvironments(ctx, appName, []string{c.name}, nil)
	if len(existing) == 0 {
		return b.CreateEnvironment(ctx, appName, c.name, c.manifest.SolutionStack, "", nil, CreateEnvironmentParams{
			VersionLabel:     c.version,
			TierName:         c.manifest.Tier.Name,
			TierType:         c.manifest.Tier.Type,
			CNAMEPrefix:      c.manifest.CName,
			OptionSettings:   settings,
			EnvironmentLinks: c.links,
		})
	}

	return b.UpdateEnvironmentWithParams(ctx, appName, c.name, UpdateEnvironmentParams{
		VersionLabel:      c.version,
		SolutionStackName: c.manifest.SolutionStack,
		TierName:          c.manifest.Tier.Name,
		TierType:          c.manifest.Tier.Type,
		OptionSettings:    settings,
		EnvironmentLinks:  c.links,
	})
}
