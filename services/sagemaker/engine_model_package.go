package sagemaker

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

var (
	errModelPackageMissing     = errors.New("model package not found")
	errModelPackageNoContainer = errors.New("model package defines no inference container")
)

type packageContainer struct {
	Environment  map[string]string `json:"Environment"`
	Image        string            `json:"Image"`
	ModelDataURL string            `json:"ModelDataUrl"`
}

type packageSpecification struct {
	Name       string             `json:"Name"`
	Containers []packageContainer `json:"Containers"`
}

// resolveModelPackages replaces every container that names a ModelPackageName with the package's
// inference containers (API_ContainerDefinition: ModelPackageName; API_InferenceSpecification).
func (b *InMemoryBackend) resolveModelPackages(region string, m Model) (Model, error) {
	defs := modelContainers(m)
	needs := false

	for _, d := range defs {
		if d.Image == "" && d.ModelPackageName != "" {
			needs = true
		}
	}

	if !needs {
		return m, nil
	}

	b.mu.RLock("resolveModelPackages")
	defer b.mu.RUnlock()

	out := make([]ContainerDefinition, 0, len(defs))

	for _, d := range defs {
		if d.Image != "" || d.ModelPackageName == "" {
			out = append(out, d)

			continue
		}

		mp, ok := b.modelPackageLocked(region, d.ModelPackageName)
		if !ok {
			return m, fmt.Errorf("%w: %s", errModelPackageMissing, d.ModelPackageName)
		}

		resolved, err := packageContainers(mp, d)
		if err != nil {
			return m, err
		}

		out = append(out, resolved...)
	}

	m.Containers = out
	m.PrimaryContainer = nil

	return m, nil
}

func (b *InMemoryBackend) modelPackageLocked(region, nameOrArn string) (*ModelPackage, bool) {
	if mp, ok := b.modelPackagesStoreRO(region).Get(nameOrArn); ok {
		return mp, true
	}

	if arnStr, ok := b.modelPackageARNIndexStoreRO(region)[nameOrArn]; ok {
		return b.modelPackagesStoreRO(region).Get(arnStr)
	}

	return nil, false
}

func packageSpecFor(mp *ModelPackage, name string) (packageSpecification, error) {
	var spec packageSpecification

	if name != "" && len(mp.AdditionalInferenceSpecifications) > 0 {
		var extra []packageSpecification
		if err := json.Unmarshal(mp.AdditionalInferenceSpecifications, &extra, jsonx.V1()); err != nil {
			return spec, fmt.Errorf("model package %s: %w", mp.ModelPackageName, err)
		}

		for _, e := range extra {
			if e.Name == name {
				return e, nil
			}
		}
	}

	if len(mp.InferenceSpecification) > 0 {
		if err := json.Unmarshal(mp.InferenceSpecification, &spec, jsonx.V1()); err != nil {
			return spec, fmt.Errorf("model package %s: %w", mp.ModelPackageName, err)
		}
	}

	return spec, nil
}

func packageContainers(mp *ModelPackage, d ContainerDefinition) ([]ContainerDefinition, error) {
	spec, err := packageSpecFor(mp, d.InferenceSpecificationName)
	if err != nil {
		return nil, err
	}

	out := make([]ContainerDefinition, 0, len(spec.Containers))

	for _, c := range spec.Containers {
		if c.Image == "" {
			continue
		}

		env := map[string]string{}
		maps.Copy(env, c.Environment)
		maps.Copy(env, d.Environment)

		cd := ContainerDefinition{
			Image: c.Image, ModelDataURL: c.ModelDataURL, Environment: env,
			ContainerHostname: d.ContainerHostname, Mode: d.Mode, ImageConfig: d.ImageConfig,
		}

		if d.ModelDataURL != "" {
			cd.ModelDataURL = d.ModelDataURL
		}

		out = append(out, cd)
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("%w: %s", errModelPackageNoContainer, mp.ModelPackageName)
	}

	return out, nil
}
