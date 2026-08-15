package ecr

import (
	"context"
	"fmt"
	"maps"
	"strconv"
)

// BatchGetRepositoryScanningConfiguration returns scanning configuration for repositories.
func (b *InMemoryBackend) BatchGetRepositoryScanningConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryNames []string,
) ([]RepositoryScanningConfiguration, []RepositoryScanningConfigurationFailure, error) {
	b.mu.RLock("BatchGetRepositoryScanningConfiguration")
	defer b.mu.RUnlock()

	configs := make([]RepositoryScanningConfiguration, 0, len(repositoryNames))
	failures := make([]RepositoryScanningConfigurationFailure, 0, len(repositoryNames))

	for _, name := range repositoryNames {
		repo, ok := b.repos.Get(name)
		if !ok {
			failures = append(failures, RepositoryScanningConfigurationFailure{
				RepositoryName: name,
				FailureCode:    "RepositoryNotFoundException",
				FailureReason:  fmt.Sprintf("repository %s not found", name),
			})

			continue
		}

		freq, filters := b.repoEffectiveScanFrequency(name, repo.ScanOnPush)
		configs = append(configs, RepositoryScanningConfiguration{
			RepositoryARN:      repo.RepositoryARN,
			RepositoryName:     name,
			ScanOnPush:         repo.ScanOnPush,
			ScanFrequency:      freq,
			AppliedScanFilters: filters,
		})
	}

	return configs, failures, nil
}

// DescribeImageScanFindings returns scan findings for an image.
func (b *InMemoryBackend) DescribeImageScanFindings(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	imageID ImageIdentifier,
	maxResults int,
	nextToken string,
) (*ImageScanFindingsResult, string, error) {
	b.mu.RLock("DescribeImageScanFindings")
	defer b.mu.RUnlock()

	if !b.repos.Has(repositoryName) {
		return nil, "", fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	img, ok := findImageLocked(b.images, b.imagesByRepo, repositoryName, b.tagIndex[repositoryName], imageID)
	if !ok {
		return nil, "", fmt.Errorf("%w: image not found", ErrImageNotFound)
	}

	findings, ok := b.imageScanFindings.Get(findingsTableKey(repositoryName, img.ImageDigest))
	if !ok {
		return nil, "", fmt.Errorf("%w: image scan not found for %s in %s",
			ErrScanNotFoundException, img.ImageDigest, repositoryName)
	}

	cp := copyImageScanFindingsResult(findings)

	if maxResults <= 0 {
		maxResults = 100 // AWS default
	}

	// Simple pagination using the finding index as the nextToken. Pagination is
	// applied to whichever finding list the scan type populated: BASIC scans page
	// Findings, ENHANCED scans page EnhancedFindings.
	var startIdx int
	if nextToken != "" {
		if parsed, err := strconv.Atoi(nextToken); err == nil {
			startIdx = parsed
		}
	}

	enhanced := len(cp.EnhancedFindings) > 0
	total := len(cp.Findings)
	if enhanced {
		total = len(cp.EnhancedFindings)
	}

	if startIdx >= total {
		cp.Findings = nil
		cp.EnhancedFindings = nil

		return &cp, "", nil
	}

	endIdx := startIdx + maxResults
	var outNextToken string
	if endIdx < total {
		outNextToken = strconv.Itoa(endIdx)
	} else {
		endIdx = total
	}

	if enhanced {
		cp.EnhancedFindings = cp.EnhancedFindings[startIdx:endIdx]
	} else {
		cp.Findings = cp.Findings[startIdx:endIdx]
	}

	return &cp, outNextToken, nil
}

// StartImageScan starts an image scan and returns the scan status.
func (b *InMemoryBackend) StartImageScan(ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	imageID ImageIdentifier,
) (*ImageScanStartResult, error) {
	b.mu.Lock("StartImageScan")
	defer b.mu.Unlock()

	if !b.repos.Has(repositoryName) {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	img, ok := findImageLocked(b.images, b.imagesByRepo, repositoryName, b.tagIndex[repositoryName], imageID)
	if !ok {
		return nil, fmt.Errorf("%w: image not found", ErrImageNotFound)
	}

	result := generateMockScanFindings(
		img.ImageDigest, repositoryName, b.accountID, img.ImageID, b.effectiveScanTypeLocked(),
	)
	b.imageScanFindings.Put(result)

	return &ImageScanStartResult{
		ImageID:        img.ImageID,
		RepositoryName: repositoryName,
		RegistryID:     b.accountID,
		Status:         result.Status,
		Description:    result.Description,
	}, nil
}

// PutImageScanningConfiguration updates per-repository scan-on-push config.
func (b *InMemoryBackend) PutImageScanningConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	scanOnPush bool,
) (*RepositoryScanningConfiguration, error) {
	b.mu.Lock("PutImageScanningConfiguration")
	defer b.mu.Unlock()

	repo, ok := b.repos.Get(repositoryName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	repo.ScanOnPush = scanOnPush

	freq, filters := b.repoEffectiveScanFrequency(repositoryName, scanOnPush)

	return &RepositoryScanningConfiguration{
		RepositoryARN:      repo.RepositoryARN,
		RepositoryName:     repositoryName,
		ScanFrequency:      freq,
		ScanOnPush:         scanOnPush,
		AppliedScanFilters: filters,
	}, nil
}

func scanFrequency(scanOnPush bool) string {
	if scanOnPush {
		return "SCAN_ON_PUSH"
	}

	return "MANUAL"
}

// repoEffectiveScanFrequency returns the effective scan frequency for a
// repository, plus the registry scan rule's repository filters that produced
// it (real AWS's RepositoryScanningConfiguration.appliedScanFilters). When
// the registry has ENHANCED scanning with a CONTINUOUS_SCAN rule matching the
// repository, that takes precedence over the per-repo ScanOnPush setting and
// its filters are the "applied" ones; otherwise no filter rule applied. Must
// be called with at least a read lock held.
func (b *InMemoryBackend) repoEffectiveScanFrequency(
	repositoryName string,
	scanOnPush bool,
) (string, []RepositoryFilter) {
	if b.registryScanningConfig != nil && b.registryScanningConfig.ScanType == "ENHANCED" {
		for _, rule := range b.registryScanningConfig.Rules {
			if rule.ScanFrequency == "CONTINUOUS_SCAN" &&
				repoMatchesFilters(repositoryName, rule.RepositoryFilters) {
				return "CONTINUOUS_SCAN", rule.RepositoryFilters
			}
		}
	}

	return scanFrequency(scanOnPush), nil
}

// effectiveScanTypeLocked returns the registry-wide scan type ("BASIC" or
// "ENHANCED"), defaulting to BASIC when no registry scanning configuration is
// set. Must be called with at least a read lock held.
func (b *InMemoryBackend) effectiveScanTypeLocked() string {
	if b.registryScanningConfig != nil && b.registryScanningConfig.ScanType != "" {
		return b.registryScanningConfig.ScanType
	}

	return scanTypeBasic
}

func copyImageScanFindingsResult(in *ImageScanFindingsResult) ImageScanFindingsResult {
	cp := *in
	cp.FindingSeverityCounts = make(map[string]int32, len(in.FindingSeverityCounts))
	maps.Copy(cp.FindingSeverityCounts, in.FindingSeverityCounts)
	cp.Findings = make([]ImageScanFinding, len(in.Findings))
	for i, finding := range in.Findings {
		cp.Findings[i] = finding
		cp.Findings[i].Attributes = copyStringMap(finding.Attributes)
	}

	cp.EnhancedFindings = copyEnhancedFindings(in.EnhancedFindings)

	return cp
}
