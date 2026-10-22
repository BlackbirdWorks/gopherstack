package connect

import (
	"context"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	dirConnectManaged  = "CONNECT_MANAGED"
	dirSAML            = "SAML"
	dirExisting        = "EXISTING_DIRECTORY"
	statusActive       = "ACTIVE"
	statusCreating     = "CREATION_IN_PROGRESS"
	maxApprovedOrigins = 100
	maxSecurityKeys    = 2
	maxAliasLen        = 45
)

var (
	aliasPattern    = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	originPattern   = regexp.MustCompile(`^https://[A-Za-z0-9][A-Za-z0-9.-]*(:[0-9]+)?$`)
	lambdaARNRegexp = regexp.MustCompile(
		`^arn:aws[a-z-]*:lambda:[a-z0-9-]+:\d{12}:function:[A-Za-z0-9_-]+(:[A-Za-z0-9_$-]+)?$`,
	)
	lexAliasRegexp = regexp.MustCompile(`^arn:aws[a-z-]*:lex:[a-z0-9-]+:\d{12}:bot-alias/[A-Z0-9]{10}/[A-Z0-9]{10}$`)
)

const (
	storageS3       = "S3"
	storageStream   = "KINESIS_STREAM"
	storageFirehose = "KINESIS_FIREHOSE"
	boolTrue        = "true"
)

func attributeNames() []string {
	return []string{
		"INBOUND_CALLS", "OUTBOUND_CALLS", "CONTACTFLOW_LOGS", "CONTACT_LENS", "AUTO_RESOLVE_BEST_VOICES",
		"USE_CUSTOM_TTS_VOICES", "EARLY_MEDIA", "MULTI_PARTY_CONFERENCE", "AUTO_MUTE_AGENT_ON_HOLD",
		"HIGH_VOLUME_OUTBOUND", "ENHANCED_CONTACT_MONITORING", "ENHANCED_CHAT_MONITORING",
		"MULTI_PARTY_CHAT_CONFERENCE", "MESSAGE_STREAMING",
	}
}

func storageTypesByResource() map[string][]string {
	return map[string][]string{
		"CHAT_TRANSCRIPTS":                          {storageS3},
		"CALL_RECORDINGS":                           {storageS3},
		"SCHEDULED_REPORTS":                         {storageS3},
		"ATTACHMENTS":                               {storageS3},
		"CONTACT_EVALUATIONS":                       {storageS3},
		"SCREEN_RECORDINGS":                         {storageS3},
		"EMAIL_MESSAGES":                            {storageS3},
		"MEDIA_STREAMS":                             {"KINESIS_VIDEO_STREAM"},
		"CONTACT_TRACE_RECORDS":                     {storageStream, storageFirehose},
		"AGENT_EVENTS":                              {storageStream, storageFirehose},
		"REAL_TIME_CONTACT_ANALYSIS_SEGMENTS":       {storageStream},
		"REAL_TIME_CONTACT_ANALYSIS_CHAT_SEGMENTS":  {storageStream},
		"REAL_TIME_CONTACT_ANALYSIS_VOICE_SEGMENTS": {storageStream},
	}
}

// CreateInstanceInput is the CreateInstance request body.
type CreateInstanceInput struct {
	InboundCallsEnabled    *bool
	OutboundCallsEnabled   *bool
	Tags                   map[string]string
	IdentityManagementType string
	InstanceAlias          string
	DirectoryID            string `json:"DirectoryId"`
	ClientToken            string
}

func instanceID(ref string) string {
	if _, after, ok := strings.CutLast(ref, ":instance/"); ok {
		return strings.TrimSuffix(after, "/")
	}

	return ref
}

func (b *Backend) instanceLocked(ref string) (*instanceRec, error) {
	if r, ok := b.instances.Get(instanceID(ref)); ok {
		return r, nil
	}

	return nil, notFoundErr("Instance", ref)
}

func (b *Backend) viewInstance(r *instanceRec) Instance {
	i := r.Instance
	i.Tags = cloneTags(i.Tags)

	if b.now().Sub(i.CreatedTime.Time) < b.delay {
		i.InstanceStatus = statusCreating
	}

	return i
}

func validateCreateInstance(in *CreateInstanceInput) error {
	if err := oneOf(
		"IdentityManagementType",
		in.IdentityManagementType,
		dirSAML,
		dirConnectManaged,
		dirExisting,
	); err != nil {
		return err
	}

	if in.InboundCallsEnabled == nil || in.OutboundCallsEnabled == nil {
		return invalidParamErr("InboundCallsEnabled and OutboundCallsEnabled are required")
	}

	if in.IdentityManagementType == dirExisting {
		if in.DirectoryID == "" {
			return invalidParamErr("DirectoryId is required for EXISTING_DIRECTORY")
		}
	} else if in.InstanceAlias == "" {
		return invalidParamErr("InstanceAlias is required for %s", in.IdentityManagementType)
	}

	if in.InstanceAlias != "" &&
		(len(in.InstanceAlias) < 1 || len(in.InstanceAlias) > maxAliasLen || !aliasPattern.MatchString(in.InstanceAlias)) {
		return invalidParamErr("InstanceAlias must be 1-%d lowercase letters, digits or hyphens", maxAliasLen)
	}

	return checkTags(in.Tags)
}

// CreateInstance provisions an instance with its default queue, routing profile, security profiles and agent statuses.
func (b *Backend) CreateInstance(ctx context.Context, in *CreateInstanceInput) (*Instance, error) {
	if err := validateCreateInstance(in); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateInstance")
	defer b.mu.Unlock()

	for _, r := range b.instances.All() {
		if in.InstanceAlias != "" && r.Instance.InstanceAlias == in.InstanceAlias {
			return nil, duplicateErr("Instance alias", in.InstanceAlias)
		}
	}

	account, _ := b.scope(ctx)
	id := uuid.NewString()
	svcRole := "arn:aws:iam::" + account + ":role/aws-service-role/connect.amazonaws.com/AWSServiceRoleForAmazonConnect_" +
		strings.ReplaceAll(id, "-", "")[:20]
	inst := Instance{
		CreatedTime:            awstime.NewEpochTime(b.now()),
		Tags:                   cloneTags(in.Tags),
		Arn:                    b.buildARN(ctx, "instance/"+id),
		ID:                     id,
		IdentityManagementType: in.IdentityManagementType,
		InstanceAlias:          in.InstanceAlias,
		InstanceStatus:         statusActive,
		ServiceRole:            svcRole,
		InboundCallsEnabled:    *in.InboundCallsEnabled,
		OutboundCallsEnabled:   *in.OutboundCallsEnabled,
	}

	if in.InstanceAlias != "" {
		inst.InstanceAccessURL = "https://" + in.InstanceAlias + ".my.connect.aws"
	}

	rec := &instanceRec{Instance: inst, Attributes: defaultAttributes(inst)}
	b.instances.Put(rec)
	b.provisionDefaults(ctx, rec)

	out := b.viewInstance(rec)

	return &out, nil
}

func defaultAttributes(i Instance) map[string]string {
	attrs := make(map[string]string, len(attributeNames()))
	for _, a := range attributeNames() {
		attrs[a] = "false"
	}

	attrs["AUTO_RESOLVE_BEST_VOICES"] = boolTrue
	attrs["EARLY_MEDIA"] = boolTrue
	attrs["INBOUND_CALLS"] = boolStr(i.InboundCallsEnabled)
	attrs["OUTBOUND_CALLS"] = boolStr(i.OutboundCallsEnabled)

	return attrs
}

func boolStr(v bool) string {
	if v {
		return boolTrue
	}

	return "false"
}

// DescribeInstance returns an instance by ID or ARN.
func (b *Backend) DescribeInstance(ref string) (*Instance, error) {
	b.mu.RLock("DescribeInstance")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return nil, err
	}

	out := b.viewInstance(r)

	return &out, nil
}

// ListInstances pages through instances ordered by ID.
func (b *Backend) ListInstances(token string, maxResults int32) ([]InstanceSummary, string, error) {
	b.mu.RLock("ListInstances")
	all := make([]InstanceSummary, 0, b.instances.Len())

	for _, r := range b.instances.All() {
		i := b.viewInstance(r)
		all = append(all, InstanceSummary{
			CreatedTime:            i.CreatedTime,
			Arn:                    i.Arn,
			ID:                     i.ID,
			IdentityManagementType: i.IdentityManagementType,
			InstanceAccessURL:      i.InstanceAccessURL,
			InstanceAlias:          i.InstanceAlias,
			InstanceStatus:         i.InstanceStatus,
			ServiceRole:            i.ServiceRole,
			InboundCallsEnabled:    i.InboundCallsEnabled,
			OutboundCallsEnabled:   i.OutboundCallsEnabled,
		})
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(a, c InstanceSummary) int { return strings.Compare(a.ID, c.ID) })

	return paged(all, token, maxResults)
}

// DeleteInstance removes an instance and everything scoped to it.
func (b *Backend) DeleteInstance(ctx context.Context, ref string) error {
	b.mu.Lock("DeleteInstance")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return err
	}

	id := r.Instance.ID

	for _, ir := range b.integrations.in(id) {
		b.dropExternalAssociation(ctx, &ir.Association, ir.ExternalID)
	}

	b.users.dropInstance(id)
	b.routingProfiles.dropInstance(id)
	b.securityProfiles.dropInstance(id)
	b.hierarchyGroups.dropInstance(id)
	b.hierarchyLevels.Delete(id)
	b.queues.dropInstance(id)
	b.hours.dropInstance(id)
	b.quickConnects.dropInstance(id)
	b.flows.dropInstance(id)
	b.modules.dropInstance(id)
	b.agentStatuses.dropInstance(id)
	b.prompts.dropInstance(id)
	b.contacts.dropInstance(id)
	b.integrations.dropInstance(id)
	b.dropExtras(id)

	for _, p := range b.phoneNumbers.All() {
		if p.InstanceID == id {
			b.phoneNumbers.Delete(p.Phone.PhoneNumberID)
		}
	}

	b.instances.Delete(id)

	return nil
}

// DescribeInstanceAttribute returns one attribute.
func (b *Backend) DescribeInstanceAttribute(ref, attrType string) (*Attribute, error) {
	if err := oneOf("AttributeType", attrType, attributeNames()...); err != nil {
		return nil, err
	}

	b.mu.RLock("DescribeInstanceAttribute")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return nil, err
	}

	return &Attribute{AttributeType: attrType, Value: r.Attributes[attrType]}, nil
}

// ListInstanceAttributes pages through all attributes.
func (b *Backend) ListInstanceAttributes(ref, token string, maxResults int32) ([]Attribute, string, error) {
	b.mu.RLock("ListInstanceAttributes")

	r, err := b.instanceLocked(ref)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	all := make([]Attribute, 0, len(r.Attributes))
	for _, a := range attributeNames() {
		all = append(all, Attribute{AttributeType: a, Value: r.Attributes[a]})
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

// UpdateInstanceAttribute sets one attribute; INBOUND_CALLS and OUTBOUND_CALLS also drive the instance flags.
func (b *Backend) UpdateInstanceAttribute(ref, attrType, value string) error {
	if err := oneOf("AttributeType", attrType, attributeNames()...); err != nil {
		return err
	}

	if err := oneOf("Value", value, boolTrue, "false"); err != nil {
		return err
	}

	b.mu.Lock("UpdateInstanceAttribute")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return err
	}

	r.Attributes[attrType] = value

	switch attrType {
	case "INBOUND_CALLS":
		r.Instance.InboundCallsEnabled = value == boolTrue
	case "OUTBOUND_CALLS":
		r.Instance.OutboundCallsEnabled = value == boolTrue
	}

	return nil
}

func validateStorage(resourceType string, cfg *InstanceStorageConfig) error {
	allowed, ok := storageTypesByResource()[resourceType]
	if !ok {
		return invalidParamErr("ResourceType %q is not valid", resourceType)
	}

	if cfg == nil {
		return invalidParamErr("StorageConfig is required")
	}

	if err := oneOf("StorageType", cfg.StorageType, allowed...); err != nil {
		return err
	}

	if err := validateStorageTarget(cfg); err != nil {
		return err
	}

	if enc := encryptionOf(cfg); enc != nil && (enc.EncryptionType != "KMS" || enc.KeyID == "") {
		return invalidParamErr("EncryptionConfig needs EncryptionType KMS and a KeyId")
	}

	return nil
}

func validateStorageTarget(cfg *InstanceStorageConfig) error {
	switch cfg.StorageType {
	case storageS3:
		if cfg.S3Config == nil || cfg.S3Config.BucketName == "" || cfg.S3Config.BucketPrefix == "" {
			return invalidParamErr("S3Config with BucketName and BucketPrefix is required")
		}
	case "KINESIS_VIDEO_STREAM":
		if cfg.KinesisVideoStreamConfig == nil || cfg.KinesisVideoStreamConfig.Prefix == "" {
			return invalidParamErr("KinesisVideoStreamConfig with Prefix is required")
		}
	case storageStream:
		if cfg.KinesisStreamConfig == nil || !strings.Contains(cfg.KinesisStreamConfig.StreamArn, ":stream/") {
			return invalidParamErr("KinesisStreamConfig with a stream ARN is required")
		}
	case storageFirehose:
		if cfg.KinesisFirehoseConfig == nil ||
			!strings.Contains(cfg.KinesisFirehoseConfig.FirehoseArn, ":deliverystream/") {
			return invalidParamErr("KinesisFirehoseConfig with a delivery stream ARN is required")
		}
	}

	return nil
}

func encryptionOf(cfg *InstanceStorageConfig) *EncryptionConfig {
	switch {
	case cfg.S3Config != nil:
		return cfg.S3Config.EncryptionConfig
	case cfg.KinesisVideoStreamConfig != nil:
		return cfg.KinesisVideoStreamConfig.EncryptionConfig
	default:
		return nil
	}
}

// AssociateInstanceStorageConfig attaches a storage target for one kind of instance data.
func (b *Backend) AssociateInstanceStorageConfig(
	ctx context.Context, ref, resourceType string, cfg *InstanceStorageConfig,
) (string, error) {
	if err := validateStorage(resourceType, cfg); err != nil {
		return "", err
	}

	b.mu.Lock("AssociateInstanceStorageConfig")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return "", err
	}

	if cfg.S3Config != nil && b.resolvers != nil && !b.resolvers.BucketExists(ctx, cfg.S3Config.BucketName) {
		return "", notFoundErr("S3 bucket", cfg.S3Config.BucketName)
	}

	for _, s := range r.Storage {
		if s.ResourceType == resourceType {
			return "", duplicateErr("Storage config", resourceType)
		}
	}

	stored := *cfg
	stored.AssociationID = awsid.NewN("", assocIDHexLen)
	r.Storage = append(r.Storage, StorageAssociation{InstanceStorageConfig: stored, ResourceType: resourceType})

	return stored.AssociationID, nil
}

func storageIndex(r *instanceRec, resourceType, assocID string) int {
	return slices.IndexFunc(r.Storage, func(s StorageAssociation) bool {
		return s.ResourceType == resourceType && s.AssociationID == assocID
	})
}

// DescribeInstanceStorageConfig returns one storage association.
func (b *Backend) DescribeInstanceStorageConfig(ref, resourceType, assocID string) (*InstanceStorageConfig, error) {
	b.mu.RLock("DescribeInstanceStorageConfig")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return nil, err
	}

	i := storageIndex(r, resourceType, assocID)
	if i < 0 {
		return nil, notFoundErr("StorageConfig", assocID)
	}

	cfg := r.Storage[i].InstanceStorageConfig

	return &cfg, nil
}

// ListInstanceStorageConfigs pages through storage associations of one resource type.
func (b *Backend) ListInstanceStorageConfigs(
	ref, resourceType, token string, maxResults int32,
) ([]InstanceStorageConfig, string, error) {
	if _, ok := storageTypesByResource()[resourceType]; !ok {
		return nil, "", invalidParamErr("resourceType %q is not valid", resourceType)
	}

	b.mu.RLock("ListInstanceStorageConfigs")

	r, err := b.instanceLocked(ref)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []InstanceStorageConfig

	for _, s := range r.Storage {
		if s.ResourceType == resourceType {
			all = append(all, s.InstanceStorageConfig)
		}
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

// UpdateInstanceStorageConfig replaces the target of an existing storage association.
func (b *Backend) UpdateInstanceStorageConfig(
	ctx context.Context, ref, resourceType, assocID string, cfg *InstanceStorageConfig,
) error {
	if err := validateStorage(resourceType, cfg); err != nil {
		return err
	}

	b.mu.Lock("UpdateInstanceStorageConfig")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return err
	}

	i := storageIndex(r, resourceType, assocID)
	if i < 0 {
		return notFoundErr("StorageConfig", assocID)
	}

	if cfg.S3Config != nil && b.resolvers != nil && !b.resolvers.BucketExists(ctx, cfg.S3Config.BucketName) {
		return notFoundErr("S3 bucket", cfg.S3Config.BucketName)
	}

	stored := *cfg
	stored.AssociationID = assocID
	r.Storage[i].InstanceStorageConfig = stored

	return nil
}

// DisassociateInstanceStorageConfig removes a storage association.
func (b *Backend) DisassociateInstanceStorageConfig(ref, resourceType, assocID string) error {
	b.mu.Lock("DisassociateInstanceStorageConfig")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return err
	}

	i := storageIndex(r, resourceType, assocID)
	if i < 0 {
		return notFoundErr("StorageConfig", assocID)
	}

	r.Storage = slices.Delete(r.Storage, i, i+1)

	return nil
}

// AssociateApprovedOrigin allows an origin to embed the instance's CCP.
func (b *Backend) AssociateApprovedOrigin(ref, origin string) error {
	if !originPattern.MatchString(origin) || len(origin) > 267 {
		return invalidParamErr("Origin must be an https URL without a path")
	}

	b.mu.Lock("AssociateApprovedOrigin")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return err
	}

	if slices.Contains(r.Origins, origin) {
		return duplicateErr("Approved origin", origin)
	}

	if len(r.Origins) >= maxApprovedOrigins {
		return limitErr("an instance can have at most %d approved origins", maxApprovedOrigins)
	}

	r.Origins = append(r.Origins, origin)

	return nil
}

// DisassociateApprovedOrigin removes an approved origin.
func (b *Backend) DisassociateApprovedOrigin(ref, origin string) error {
	b.mu.Lock("DisassociateApprovedOrigin")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return err
	}

	i := slices.Index(r.Origins, origin)
	if i < 0 {
		return notFoundErr("Approved origin", origin)
	}

	r.Origins = slices.Delete(r.Origins, i, i+1)

	return nil
}

// ListApprovedOrigins pages through approved origins.
func (b *Backend) ListApprovedOrigins(ref, token string, maxResults int32) ([]string, string, error) {
	b.mu.RLock("ListApprovedOrigins")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return nil, "", err
	}

	return paged(slices.Clone(r.Origins), token, maxResults)
}

// AssociateLambdaFunction lets contact flows invoke a Lambda function.
func (b *Backend) AssociateLambdaFunction(ctx context.Context, ref, functionARN string) error {
	if !lambdaARNRegexp.MatchString(functionARN) {
		return invalidParamErr("FunctionArn %q is not a Lambda function ARN", functionARN)
	}

	b.mu.Lock("AssociateLambdaFunction")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return err
	}

	if b.resolvers != nil && !b.resolvers.LambdaFunctionExists(ctx, functionARN) {
		return notFoundErr("Lambda function", functionARN)
	}

	if slices.Contains(r.Lambdas, functionARN) {
		return duplicateErr("Lambda function", functionARN)
	}

	r.Lambdas = append(r.Lambdas, functionARN)

	return nil
}

// DisassociateLambdaFunction removes a Lambda function association.
func (b *Backend) DisassociateLambdaFunction(ref, functionARN string) error {
	b.mu.Lock("DisassociateLambdaFunction")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return err
	}

	i := slices.Index(r.Lambdas, functionARN)
	if i < 0 {
		return notFoundErr("Lambda function", functionARN)
	}

	r.Lambdas = slices.Delete(r.Lambdas, i, i+1)

	return nil
}

// ListLambdaFunctions pages through associated Lambda function ARNs.
func (b *Backend) ListLambdaFunctions(ref, token string, maxResults int32) ([]string, string, error) {
	b.mu.RLock("ListLambdaFunctions")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return nil, "", err
	}

	return paged(slices.Clone(r.Lambdas), token, maxResults)
}

func validateLex(c LexBotConfig) error {
	switch {
	case c.LexBot != nil && c.LexV2Bot == nil:
		if c.LexBot.Name == "" || c.LexBot.LexRegion == "" {
			return invalidParamErr("LexBot needs Name and LexRegion")
		}
	case c.LexV2Bot != nil && c.LexBot == nil:
		if !lexAliasRegexp.MatchString(c.LexV2Bot.AliasArn) {
			return invalidParamErr("LexV2Bot.AliasArn %q is not a Lex V2 bot alias ARN", c.LexV2Bot.AliasArn)
		}
	default:
		return invalidParamErr("exactly one of LexBot and LexV2Bot is required")
	}

	return nil
}

func sameLex(a, c LexBotConfig) bool {
	switch {
	case a.LexBot != nil && c.LexBot != nil:
		return *a.LexBot == *c.LexBot
	case a.LexV2Bot != nil && c.LexV2Bot != nil:
		return *a.LexV2Bot == *c.LexV2Bot
	default:
		return false
	}
}

// AssociateBot associates a Lex V1 or V2 bot with the instance.
func (b *Backend) AssociateBot(ref string, cfg LexBotConfig) error {
	if err := validateLex(cfg); err != nil {
		return err
	}

	b.mu.Lock("AssociateBot")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return err
	}

	if slices.ContainsFunc(r.LexBots, func(o LexBotConfig) bool { return sameLex(o, cfg) }) {
		return duplicateErr("Bot", "association")
	}

	r.LexBots = append(r.LexBots, cfg)

	return nil
}

// DisassociateBot removes a Lex V1 or V2 bot association.
func (b *Backend) DisassociateBot(ref string, cfg LexBotConfig) error {
	if err := validateLex(cfg); err != nil {
		return err
	}

	b.mu.Lock("DisassociateBot")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return err
	}

	i := slices.IndexFunc(r.LexBots, func(o LexBotConfig) bool { return sameLex(o, cfg) })
	if i < 0 {
		return notFoundErr("Bot", "association")
	}

	r.LexBots = slices.Delete(r.LexBots, i, i+1)

	return nil
}

// ListBots pages through bot associations of one Lex version (V1 or V2).
func (b *Backend) ListBots(ref, version, token string, maxResults int32) ([]LexBotConfig, string, error) {
	if err := oneOf("lexVersion", version, "V1", "V2"); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListBots")

	r, err := b.instanceLocked(ref)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []LexBotConfig

	for _, c := range r.LexBots {
		if (version == "V1") == (c.LexBot != nil) {
			all = append(all, c)
		}
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

// AssociateSecurityKey trusts a public key for the instance.
func (b *Backend) AssociateSecurityKey(ref, key string) (string, error) {
	const maxKeyLen = 1024
	if !strings.HasPrefix(key, "-----BEGIN PUBLIC KEY-----") || len(key) > maxKeyLen {
		return "", invalidParamErr("Key must be a PEM public key of at most %d characters", maxKeyLen)
	}

	b.mu.Lock("AssociateSecurityKey")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return "", err
	}

	if len(r.SecurityKeys) >= maxSecurityKeys {
		return "", limitErr("an instance can have at most %d security keys", maxSecurityKeys)
	}

	k := SecurityKey{
		CreationTime:  awstime.NewEpochTime(b.now()),
		AssociationID: awsid.NewN("", assocIDHexLen),
		Key:           key,
	}
	r.SecurityKeys = append(r.SecurityKeys, k)

	return k.AssociationID, nil
}

// DisassociateSecurityKey removes a security key.
func (b *Backend) DisassociateSecurityKey(ref, assocID string) error {
	b.mu.Lock("DisassociateSecurityKey")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return err
	}

	i := slices.IndexFunc(r.SecurityKeys, func(k SecurityKey) bool { return k.AssociationID == assocID })
	if i < 0 {
		return notFoundErr("SecurityKey", assocID)
	}

	r.SecurityKeys = slices.Delete(r.SecurityKeys, i, i+1)

	return nil
}

// ListSecurityKeys pages through security keys.
func (b *Backend) ListSecurityKeys(ref, token string, maxResults int32) ([]SecurityKey, string, error) {
	b.mu.RLock("ListSecurityKeys")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return nil, "", err
	}

	return paged(slices.Clone(r.SecurityKeys), token, maxResults)
}
