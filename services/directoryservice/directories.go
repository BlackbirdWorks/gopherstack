package directoryservice

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"time"
)

// directoryLifecycleDelay is the time between each stage transition on directory creation.
const directoryLifecycleDelay = 50 * time.Millisecond

// restoreLifecycleDelay is the delay before a restoring directory returns to Active.
const restoreLifecycleDelay = 100 * time.Millisecond

// setStage transitions d to stage and stamps StageLastUpdatedDateTime, matching
// AWS's DirectoryDescription.StageLastUpdatedDateTime contract ("The date and
// time that the stage was last updated"). Caller must hold b.mu.
func setStage(d *storedDirectory, stage DirectoryStage) {
	d.Stage = string(stage)
	d.StageLastUpdatedDateTime = time.Now().UTC()
}

// transitionDirectoryToActive runs the Requested → Creating → Active lifecycle.
// Must be called as a goroutine after the directory has been stored.
func (b *InMemoryBackend) transitionDirectoryToActive(region, dirID string) {
	time.Sleep(directoryLifecycleDelay)

	b.mu.Lock("transitionDirectoryToActive:creating")
	if d, ok := b.directoryGet(region, dirID); ok && d.Stage == string(DirectoryStageRequested) {
		setStage(d, DirectoryStageCreating)
	}
	b.mu.Unlock()

	time.Sleep(directoryLifecycleDelay)

	b.mu.Lock("transitionDirectoryToActive:active")
	if d, ok := b.directoryGet(region, dirID); ok && d.Stage == string(DirectoryStageCreating) {
		setStage(d, DirectoryStageActive)
	}
	b.mu.Unlock()
}

// synthesizeDNSIPAddrs deterministically derives two plausible private DNS
// server IPs for a newly created directory from its ID, mirroring the shape
// of AWS's real DnsIpAddrs (the IP addresses of the directory's domain
// controllers) without depending on unavailable real network state.
func synthesizeDNSIPAddrs(directoryID string) []string {
	sum := sha256.Sum256([]byte(directoryID))

	return []string{
		fmt.Sprintf("10.0.%d.%d", sum[0], sum[1]),
		fmt.Sprintf("10.0.%d.%d", sum[2], sum[3]),
	}
}

func (b *InMemoryBackend) newStoredDirectory(
	region, name, shortName, description string,
	dirType DirectoryType,
	size DirectorySize,
	edition DirectoryEdition,
	vpcSettings *DirectoryVpcSettings,
	tags []Tag,
) *storedDirectory {
	id := b.newDirectoryID()
	alias := b.defaultAlias(id)
	now := time.Now().UTC()

	d := &storedDirectory{
		region:                   region,
		LaunchTime:               now,
		StageLastUpdatedDateTime: now,
		DirectoryID:              id,
		Name:                     name,
		ShortName:                shortName,
		Description:              description,
		Alias:                    alias,
		AccessURL:                b.defaultAccessURL(alias),
		DirType:                  string(dirType),
		Stage:                    string(DirectoryStageRequested),
		Size:                     string(size),
		Edition:                  string(edition),
		Tags:                     tagsToMap(tags),
		DNSIPAddrs:               synthesizeDNSIPAddrs(id),
	}
	if vpcSettings != nil {
		d.VpcSettings = &storedVpcSettings{
			VpcID:             vpcSettings.VpcID,
			SubnetIDs:         vpcSettings.SubnetIDs,
			SecurityGroupIDs:  vpcSettings.SecurityGroupIDs,
			AvailabilityZones: vpcSettings.AvailabilityZones,
		}
	}

	return d
}

// CreateDirectory creates a new Simple AD directory.
func (b *InMemoryBackend) CreateDirectory(
	ctx context.Context,
	name, shortName, description, _ string,
	size DirectorySize, vpcSettings *DirectoryVpcSettings, tags []Tag,
) (*Directory, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateDirectory")
	defer b.mu.Unlock()

	if name == "" {
		return nil, ErrInvalidParameter
	}
	if size != DirectorySizeSmall && size != DirectorySizeLarge && size != "" {
		return nil, ErrInvalidParameter
	}

	var count int32
	for _, d := range b.directoriesInRegion(region) {
		if DirectoryType(d.DirType) == DirectoryTypeSimpleAD {
			count++
		}
	}
	if count >= defaultSimpleADLimit {
		return nil, ErrDirectoryLimitExceeded
	}

	d := b.newStoredDirectory(
		region,
		name,
		shortName,
		description,
		DirectoryTypeSimpleAD,
		size,
		"",
		vpcSettings,
		tags,
	)
	b.directoryPut(d)
	b.aliasesStore(region)[d.Alias] = d.DirectoryID

	cp := d.toDirectory()

	go b.transitionDirectoryToActive(region, d.DirectoryID)

	return &cp, nil
}

// CreateMicrosoftAD creates a new Managed Microsoft AD directory.
func (b *InMemoryBackend) CreateMicrosoftAD(
	ctx context.Context,
	name, shortName, description, _ string,
	edition DirectoryEdition, vpcSettings *DirectoryVpcSettings, tags []Tag,
) (*Directory, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateMicrosoftAD")
	defer b.mu.Unlock()

	if name == "" {
		return nil, ErrInvalidParameter
	}
	if edition != DirectoryEditionEnterprise && edition != DirectoryEditionStandard &&
		edition != "" {
		return nil, ErrInvalidParameter
	}

	var count int32
	for _, d := range b.directoriesInRegion(region) {
		if DirectoryType(d.DirType) == DirectoryTypeMicrosoftAD {
			count++
		}
	}
	if count >= defaultMicrosoftADLimit {
		return nil, ErrDirectoryLimitExceeded
	}

	d := b.newStoredDirectory(
		region,
		name,
		shortName,
		description,
		DirectoryTypeMicrosoftAD,
		"",
		edition,
		vpcSettings,
		tags,
	)
	b.directoryPut(d)
	b.aliasesStore(region)[d.Alias] = d.DirectoryID

	cp := d.toDirectory()

	go b.transitionDirectoryToActive(region, d.DirectoryID)

	return &cp, nil
}

// DeleteDirectory deletes a directory and all associated resources.
func (b *InMemoryBackend) DeleteDirectory(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteDirectory")
	defer b.mu.Unlock()

	d, ok := b.directoryGet(region, directoryID)
	if !ok {
		return ErrDirectoryNotFound
	}

	delete(b.aliasesStore(region), d.Alias)
	b.directoryDelete(region, directoryID)
	b.cascadeDeleteDirectory(region, directoryID)

	return nil
}

// DescribeDirectories returns directories, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeDirectories(
	ctx context.Context,
	directoryIDs []string,
	limit int32,
	nextToken string,
) ([]*Directory, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeDirectories")
	defer b.mu.RUnlock()

	var ids []string

	if len(directoryIDs) > 0 {
		for _, id := range directoryIDs {
			if _, ok := b.directoryGet(region, id); !ok {
				return nil, "", ErrDirectoryNotFound
			}
		}
		ids = append([]string(nil), directoryIDs...)
		sort.Strings(ids)
	} else {
		for _, d := range b.directoriesInRegion(region) {
			ids = append(ids, d.DirectoryID)
		}
		sort.Strings(ids)
	}

	start := 0
	if nextToken != "" {
		if n, err := decodePageToken(nextToken); err == nil && n > 0 {
			start = n
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(ids))

	result := make([]*Directory, 0, end-start)
	for _, id := range ids[start:end] {
		d, _ := b.directoryGet(region, id)
		cp := d.toDirectory()
		result = append(result, &cp)
	}

	var outToken string
	if end < len(ids) {
		outToken = encodePageToken(end)
	}

	return result, outToken, nil
}

// CreateAlias creates an alias for a directory.
func (b *InMemoryBackend) CreateAlias(ctx context.Context, directoryID, alias string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateAlias")
	defer b.mu.Unlock()

	d, ok := b.directoryGet(region, directoryID)
	if !ok {
		return ErrDirectoryNotFound
	}

	aliases := b.aliasesStore(region)
	if _, taken := aliases[alias]; taken {
		return ErrAliasAlreadyExists
	}

	delete(aliases, d.Alias)
	d.Alias = alias
	d.AccessURL = b.defaultAccessURL(alias)
	aliases[alias] = directoryID

	return nil
}

// GetDirectoryLimits returns directory limits for the region.
func (b *InMemoryBackend) GetDirectoryLimits(ctx context.Context) *DirectoryLimits {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetDirectoryLimits")
	defer b.mu.RUnlock()

	var simpleADCount, msADCount, connectedCount int32

	for _, d := range b.directoriesInRegion(region) {
		switch DirectoryType(d.DirType) { //nolint:exhaustive // existing issue.
		case DirectoryTypeSimpleAD:
			simpleADCount++
		case DirectoryTypeMicrosoftAD:
			msADCount++
		case DirectoryTypeADConnector:
			connectedCount++
		}
	}

	return &DirectoryLimits{
		CloudOnlyDirectoriesCurrentCount: simpleADCount,
		CloudOnlyDirectoriesLimit:        defaultSimpleADLimit,
		CloudOnlyDirectoriesLimitReached: simpleADCount >= defaultSimpleADLimit,
		CloudOnlyMicrosoftADCurrentCount: msADCount,
		CloudOnlyMicrosoftADLimit:        defaultMicrosoftADLimit,
		CloudOnlyMicrosoftADLimitReached: msADCount >= defaultMicrosoftADLimit,
		ConnectedDirectoriesCurrentCount: connectedCount,
		ConnectedDirectoriesLimit:        10,                   //nolint:mnd // existing issue.
		ConnectedDirectoriesLimitReached: connectedCount >= 10, //nolint:mnd // existing issue.
	}
}

// CreateComputer creates a computer account in a directory.
func (b *InMemoryBackend) CreateComputer(
	ctx context.Context,
	directoryID, computerName, _ string,
) (*ComputerInfo, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("CreateComputer")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, ErrDirectoryNotFound
	}

	if computerName == "" {
		return nil, ErrInvalidParameter
	}

	return &ComputerInfo{
		ComputerID:   fmt.Sprintf("CN=%s,OU=Computers", computerName),
		ComputerName: computerName,
	}, nil
}

// ResetUserPassword resets a user password.
func (b *InMemoryBackend) ResetUserPassword(ctx context.Context, directoryID, _, _ string) error {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ResetUserPassword")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	return nil
}

// ConnectDirectory creates an ADConnector directory.
func (b *InMemoryBackend) ConnectDirectory(
	ctx context.Context,
	name, shortName, description, _ string,
	size DirectorySize,
	tags []Tag,
) (*Directory, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("ConnectDirectory")
	defer b.mu.Unlock()

	if name == "" {
		return nil, ErrInvalidParameter
	}
	if size != DirectorySizeSmall && size != DirectorySizeLarge && size != "" {
		return nil, ErrInvalidParameter
	}

	var count int32
	for _, d := range b.directoriesInRegion(region) {
		if DirectoryType(d.DirType) == DirectoryTypeADConnector {
			count++
		}
	}
	if count >= 10 { //nolint:mnd // AWS connected directory limit
		return nil, ErrDirectoryLimitExceeded
	}

	d := b.newStoredDirectory(region, name, shortName, description, DirectoryTypeADConnector, size, "", nil, tags)
	b.directoryPut(d)
	b.aliasesStore(region)[d.Alias] = d.DirectoryID

	cp := d.toDirectory()

	return &cp, nil
}
