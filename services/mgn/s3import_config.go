package mgn

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

const (
	csvColStagingTagPrefix = "mgn:replication:staging-area-tag:"
	csvColSecurityGroupPfx = "mgn:replication:security-group-id:"

	csvLaunchBootMode      = "mgn:launch:boot-mode"
	csvLaunchCopyPrivateIP = "mgn:launch:copy-private-ip"
	csvLaunchMapTagKey     = "mgn:launch:map-tag-key"
	csvLaunchMapTagValue   = "mgn:launch:map-tag-value"
	csvLaunchMapTagging    = "mgn:launch:map-tagging"
	csvLaunchLicensing     = "mgn:launch:operating-system-licensing"
	csvLaunchStartInstance = "mgn:launch:start-instance"
	csvLaunchTransferTags  = "mgn:launch:transfer-server-tags"

	csvReplAssociateSG   = "mgn:replication:associate-default-security-group"
	csvReplBandwidth     = "mgn:replication:bandwidth-throttling"
	csvReplPublicIP      = "mgn:replication:create-public-ip"
	csvReplRouting       = "mgn:replication:data-replication-routing"
	csvReplStagingDisk   = "mgn:replication:default-large-staging-disk-type"
	csvReplEbsEncryption = "mgn:replication:ebs-encryption"
	csvReplEbsKey        = "mgn:replication:ebs-encryption-key-arn"
	csvReplFsxARN        = "mgn:replication:fsx-ontap:credentials-secret-arn"
	csvReplFsxSVM        = "mgn:replication:fsx-ontap:storage-virtual-machine-id"
	csvReplIPProtocol    = "mgn:replication:internet-protocol"
	csvReplInstanceType  = "mgn:replication:replication-server-instance-type"
	csvReplSubnet        = "mgn:replication:staging-area-subnet-id"
	csvReplStorageType   = "mgn:replication:storage-type"
	csvReplLocalZone     = "mgn:replication:store-snapshot-on-local-zone"
	csvReplDedicated     = "mgn:replication:use-dedicated-replication-server"
	csvReplFips          = "mgn:replication:use-fips-endpoint"
)

var (
	errImportBadBool  = errors.New("expected true or false")
	errImportBadEnum  = errors.New("unexpected value")
	errImportBadInt   = errors.New("expected a non-negative integer")
	errImportFsxRules = errors.New("FSX_ONTAP storage requires both " +
		csvReplFsxSVM + " and " + csvReplFsxARN + "; EBS storage rejects them")
)

type importConfigParser struct {
	err  error
	cols map[string]int
	row  []string
}

func (p *importConfigParser) fail(col string, err error) {
	if p.err == nil {
		p.err = fmt.Errorf("%s: %w", col, err)
	}
}

func (p *importConfigParser) str(col string) string { return colValue(p.row, p.cols, col) }

func (p *importConfigParser) boolean(col string) *bool {
	v := p.str(col)
	if v == "" {
		return nil
	}

	var b bool

	switch strings.ToLower(v) {
	case "true":
		b = true
	case "false":
	default:
		p.fail(col, errImportBadBool)

		return nil
	}

	return &b
}

func (p *importConfigParser) enum(col string, allowed ...string) *string {
	v := p.str(col)
	if v == "" {
		return nil
	}

	if !slices.Contains(allowed, v) {
		p.fail(col, fmt.Errorf("%w %q (want one of %s)", errImportBadEnum, v, strings.Join(allowed, ", ")))

		return nil
	}

	return &v
}

func (p *importConfigParser) optStr(col string) *string {
	if v := p.str(col); v != "" {
		return &v
	}

	return nil
}

func parseImportRowConfigs(row []string, idx importHeaderIndex, r *importedRow) error {
	p := &importConfigParser{row: row, cols: idx.cols}

	launch := parseImportLaunch(p)
	repl := parseImportReplication(p, idx)

	launchData := parseImportLaunchData(p, idx)

	if p.err != nil {
		return p.err
	}

	r.launch, r.replication, r.launchData = launch, repl, launchData

	return nil
}

func parseImportLaunch(p *importConfigParser) *UpdateLaunchConfigurationInput {
	in := &UpdateLaunchConfigurationInput{
		BootMode:             p.enum(csvLaunchBootMode, BootModeLegacyBios, BootModeUefi, BootModeUseSource),
		CopyPrivateIP:        p.boolean(csvLaunchCopyPrivateIP),
		CopyTags:             p.boolean(csvLaunchTransferTags),
		EnableMapAutoTagging: p.boolean(csvLaunchMapTagging),
		MapAutoTaggingMpeID:  p.optStr(csvLaunchMapTagValue),
	}

	if lic := p.enum(csvLaunchLicensing, "BYOL", "LI"); lic != nil {
		in.Licensing = &Licensing{OsByol: *lic == "BYOL"}
	}

	if start := p.boolean(csvLaunchStartInstance); start != nil {
		disp := LaunchDispositionStopped
		if *start {
			disp = LaunchDispositionStarted
		}

		in.LaunchDisposition = &disp
	}

	if reflect.ValueOf(*in).IsZero() {
		return nil
	}

	return in
}

func parseImportReplication(p *importConfigParser, idx importHeaderIndex) *UpdateReplicationConfigurationInput {
	in := &UpdateReplicationConfigurationInput{
		AssociateDefaultSecurityGroup: p.boolean(csvReplAssociateSG),
		CreatePublicIP:                p.boolean(csvReplPublicIP),
		DataPlaneRouting:              p.enum(csvReplRouting, DataPlaneRoutingPrivateIP, DataPlaneRoutingPublicIP),
		DefaultLargeStagingDiskType: p.enum(
			csvReplStagingDisk, StagingDiskTypeGp2, StagingDiskTypeGp3, StagingDiskTypeSt1,
		),
		EbsEncryption:                 p.enum(csvReplEbsEncryption, EbsEncryptionDefault, EbsEncryptionCustom),
		EbsEncryptionKeyArn:           p.optStr(csvReplEbsKey),
		InternetProtocol:              p.enum(csvReplIPProtocol, InternetProtocolIPv4, InternetProtocolIPv6),
		ReplicationServerInstanceType: p.optStr(csvReplInstanceType),
		StagingAreaSubnetID:           p.optStr(csvReplSubnet),
		StoreSnapshotOnLocalZone:      p.boolean(csvReplLocalZone),
		UseDedicatedReplicationServer: p.boolean(csvReplDedicated),
		UseFipsEndpoint:               p.boolean(csvReplFips),
	}

	if v := p.str(csvReplBandwidth); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			p.fail(csvReplBandwidth, errImportBadInt)
		} else {
			in.BandwidthThrottling = &n
		}
	}

	in.StorageConfiguration = parseImportStorage(p)
	in.StagingAreaTags = rowTagMap(p.row, idx.stagingCols)

	if len(in.StagingAreaTags) == 0 {
		in.StagingAreaTags = nil
	}

	in.ReplicationServersSecurityGroupsIDs = importSecurityGroups(p, idx)

	if reflect.ValueOf(*in).IsZero() {
		return nil
	}

	return in
}

func importSecurityGroups(p *importConfigParser, idx importHeaderIndex) []string {
	type pos struct {
		id string
		n  int
	}

	var found []pos

	for col := range idx.cols {
		suffix, ok := strings.CutPrefix(col, csvColSecurityGroupPfx)
		if !ok {
			continue
		}

		n, err := strconv.Atoi(suffix)
		if err != nil || n < 0 {
			p.fail(col, errImportBadInt)

			continue
		}

		if id := colValue(p.row, idx.cols, col); id != "" {
			found = append(found, pos{n: n, id: id})
		}
	}

	slices.SortFunc(found, func(a, b pos) int { return a.n - b.n })

	ids := make([]string, 0, len(found))
	for _, f := range found {
		ids = append(ids, f.id)
	}

	if len(ids) == 0 {
		return nil
	}

	return ids
}

func parseImportStorage(p *importConfigParser) *StorageConfiguration {
	st := p.enum(csvReplStorageType, StorageTypeEbs, StorageTypeFsxOntap)
	svm, secret := p.str(csvReplFsxSVM), p.str(csvReplFsxARN)

	if st == nil {
		if svm != "" || secret != "" {
			p.fail(csvReplStorageType, errImportFsxRules)
		}

		return nil
	}

	switch *st {
	case StorageTypeFsxOntap:
		if svm == "" || secret == "" {
			p.fail(csvReplStorageType, errImportFsxRules)

			return nil
		}

		return &StorageConfiguration{
			StorageType:           StorageTypeFsxOntap,
			FsxOntapConfiguration: &FsxOntapConfiguration{StorageVirtualMachineID: svm, CredentialsSecretArn: secret},
		}
	default:
		if svm != "" || secret != "" {
			p.fail(csvReplStorageType, errImportFsxRules)

			return nil
		}

		return &StorageConfiguration{StorageType: StorageTypeEbs}
	}
}

func (b *InMemoryBackend) applyImportConfigsLocked(serverID string, row importedRow) {
	if row.launch != nil {
		if lc, ok := b.launchConfigs.Get(serverID); ok {
			applyLaunchConfigUpdate(lc, *row.launch)
		}
	}

	b.applyImportLaunchDataLocked(serverID, row.launchData)

	if row.replication != nil {
		if rc, ok := b.replicationConfigs.Get(serverID); ok {
			applyReplicationConfigUpdate(rc, *row.replication)
		}
	}
}
