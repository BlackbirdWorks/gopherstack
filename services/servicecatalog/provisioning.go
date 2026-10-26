package servicecatalog

import (
	"context"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

var ppNameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

func registerProvisioningOps(m map[string]opFunc, b *InMemoryBackend) {
	m["ProvisionProduct"] = op(b.ProvisionProduct)
	m["UpdateProvisionedProduct"] = op(b.UpdateProvisionedProduct)
	m["TerminateProvisionedProduct"] = op(b.TerminateProvisionedProduct)
	m["ImportAsProvisionedProduct"] = op(b.ImportAsProvisionedProduct)
	m["UpdateProvisionedProductProperties"] = op(b.UpdateProvisionedProductProperties)
	m["DescribeProvisionedProduct"] = op(b.DescribeProvisionedProduct)
	m["ListStackInstancesForProvisionedProduct"] = op(b.ListStackInstancesForProvisionedProduct)
	m["SearchProvisionedProducts"] = op(b.SearchProvisionedProducts)
	m["ScanProvisionedProducts"] = op(b.ScanProvisionedProducts)
	m["GetProvisionedProductOutputs"] = op(b.GetProvisionedProductOutputs)
	m["DescribeRecord"] = op(b.DescribeRecord)
	m["ListRecordHistory"] = op(b.ListRecordHistory)
	m["NotifyProvisionProductEngineWorkflowResult"] = op(b.NotifyProvisionProductEngineWorkflowResult)
	m["NotifyTerminateProvisionedProductEngineWorkflowResult"] = op(
		b.NotifyTerminateProvisionedProductEngineWorkflowResult,
	)
	m["NotifyUpdateProvisionedProductEngineWorkflowResult"] = op(b.NotifyUpdateProvisionedProductEngineWorkflowResult)
}

func (p *ProvisionedProduct) keyFor(actionID string) string {
	return p.ProductID + "|" + p.ArtifactID + "|" + actionID
}

func (b *InMemoryBackend) provisionedLocked(account, region, id, name string) (*ProvisionedProduct, error) {
	if id != "" {
		pp, ok := b.provisioned.Get(id)
		if ok && pp.AccountID == account && pp.Region == region {
			return pp, nil
		}

		return nil, notFound("provisioned product", id)
	}

	if name == "" {
		return nil, invalid("either the provisioned product ID or name is required")
	}

	for _, pp := range b.provisioned.All() {
		if pp.AccountID == account && pp.Region == region && pp.Name == name {
			return pp, nil
		}
	}

	return nil, notFound("provisioned product", name)
}

// ppStatus is the status a provisioned product reports: UNDER_CHANGE while its last record is running.
func (b *InMemoryBackend) ppStatus(pp *ProvisionedProduct) string {
	if r, ok := b.records.Get(pp.LastRecordID); ok && b.recStatus(r) == recInProgress {
		return ppUnderChange
	}

	return pp.Status
}

func (b *InMemoryBackend) recStatus(r *Record) string {
	if r.AwaitingEngine || b.now().Before(r.SettleAt) {
		return recInProgress
	}

	return r.FinalStatus
}

func (b *InMemoryBackend) newRecordLocked(account, region, typ string, pp *ProvisionedProduct) *Record {
	r := &Record{
		ID:                     newID("rec-"),
		AccountID:              account,
		Region:                 region,
		Type:                   typ,
		CreatedTime:            b.now(),
		AwaitingEngine:         true,
		ProvisionedProductID:   pp.ID,
		ProvisionedProductName: pp.Name,
		ProductID:              pp.ProductID,
		ArtifactID:             pp.ArtifactID,
		PathID:                 pp.PathID,
		LaunchRoleArn:          pp.LaunchRoleArn,
		PPType:                 pp.Type,
	}
	b.records.Put(r)

	return r
}

// settleLocked marks a record's work finished; it reports IN_PROGRESS for the lifecycle delay, then its final status.
func (b *InMemoryBackend) settleLocked(r *Record) {
	r.AwaitingEngine = false
	r.SettleAt = b.now().Add(b.dwell)
}

func (b *InMemoryBackend) recordDetailLocked(r *Record, justCreated bool) *RecordDetail {
	status := b.recStatus(r)
	if justCreated {
		status = recCreated
	}

	updated := r.CreatedTime
	if !r.AwaitingEngine && !r.SettleAt.IsZero() {
		updated = r.SettleAt
	}

	d := &RecordDetail{
		CreatedTime:            ts(r.CreatedTime),
		UpdatedTime:            ts(updated),
		LaunchRoleArn:          r.LaunchRoleArn,
		PathID:                 r.PathID,
		ProductID:              r.ProductID,
		ProvisionedProductID:   r.ProvisionedProductID,
		ProvisionedProductName: r.ProvisionedProductName,
		ProvisionedProductType: strOr(r.PPType, ppTypeCFNStack),
		ProvisioningArtifactID: r.ArtifactID,
		RecordID:               r.ID,
		RecordType:             r.Type,
		Status:                 status,
	}

	if status == recFailed {
		for _, e := range r.Errors {
			d.RecordErrors = append(d.RecordErrors, RecordErrorWire(e))
		}
	}

	for _, k := range sortedKeys(r.Tags) {
		d.RecordTags = append(d.RecordTags, RecordTag{Key: k, Value: r.Tags[k]})
	}

	return d
}

// launchPlan is everything resolved before a stack is touched.
type launchPlan struct {
	prod         *Product
	art          *Artifact
	path         *Portfolio
	productTags  map[string]string
	stackSet     *stackSetConfig
	roleARN      string
	templateRule []string
	notify       []string
	tagUpdateOff bool
}

func (b *InMemoryBackend) resolveLaunchLocked(
	ctx context.Context, account, region, productID, productName, artID, artName, pathID, pathName string,
) (*launchPlan, error) {
	p, err := b.visibleProductLocked(ctx, account, region, productID, productName)
	if err != nil {
		return nil, err
	}

	path, err := b.resolvePathLocked(ctx, account, region, p.ID, pathID, pathName)
	if err != nil {
		return nil, err
	}

	a, err := b.launchArtifactLocked(p, artID, artName)
	if err != nil {
		return nil, err
	}

	lp := &launchPlan{prod: p, art: a, path: path, productTags: maps.Clone(p.Tags)}

	for _, c := range b.constraintsForLocked(path.ID, p.ID) {
		applyConstraint(lp, c, account)
	}

	return lp, nil
}

func (b *InMemoryBackend) launchArtifactLocked(p *Product, artID, artName string) (*Artifact, error) {
	arts := b.artifactsOfLocked(p.ID)

	var found *Artifact

	switch {
	case artID != "" || artName != "":
		for _, a := range arts {
			if (artID != "" && a.ID == artID) || (artID == "" && a.Name == artName) {
				found = a
			}
		}

		if found == nil {
			return nil, notFound("provisioning artifact", strOr(artID, artName))
		}
	default:
		for _, a := range arts {
			if !a.Inactive {
				found = a
			}
		}

		if found == nil {
			return nil, invalid("product %s has no active provisioning artifact", p.ID)
		}
	}

	if found.Inactive {
		return nil, invalid("provisioning artifact %s is not active", found.ID)
	}

	return found, nil
}

func applyConstraint(lp *launchPlan, c *Constraint, account string) {
	var doc map[string]any
	if err := jsonUnmarshal(c.Parameters, &doc); err != nil {
		return
	}

	switch c.Type {
	case constraintLaunch:
		lp.roleARN = stringField(doc, "RoleArn")
		if local := stringField(doc, "LocalRoleName"); local != "" {
			lp.roleARN = "arn:aws:iam::" + account + ":role/" + local
		}
	case constraintNotify:
		if arns, ok := doc["NotificationArns"].([]any); ok {
			for _, a := range arns {
				if s, isStr := a.(string); isStr {
					lp.notify = append(lp.notify, s)
				}
			}
		}
	case constraintTemplate:
		lp.templateRule = append(lp.templateRule, c.Parameters)
	case constraintStackSet:
		lp.stackSet = parseStackSetConfig(doc)
	case constraintResource:
		lp.tagUpdateOff = stringField(doc, "TagUpdateOnProvisionedProduct") == "NOT_ALLOWED"
	}
}

// prepareParameters validates launch parameters against the template and returns the explicit stack parameters.
func prepareParameters(
	info TemplateInfo,
	given, previous map[string]string,
	usePrevious map[string]bool,
) (map[string]string, error) {
	declared := map[string]TemplateParameter{}
	for _, p := range info.Parameters {
		declared[p.Key] = p
	}

	for k := range given {
		if _, ok := declared[k]; !ok {
			return nil, invalid("parameter %q is not declared by the template", k)
		}
	}

	out := map[string]string{}

	var missing []string

	for _, p := range info.Parameters {
		val, ok := given[p.Key]

		switch {
		case usePrevious[p.Key]:
			prev, had := previous[p.Key]
			if !had {
				return nil, invalid("parameter %q has no previous value", p.Key)
			}

			val = prev
		case !ok && p.Default == "":
			missing = append(missing, p.Key)

			continue
		case !ok:
			continue
		}

		if len(p.AllowedValues) > 0 && !slices.Contains(p.AllowedValues, val) {
			return nil, invalid("parameter %q value %q is not one of the allowed values", p.Key, val)
		}

		out[p.Key] = val
	}

	if len(missing) > 0 {
		return nil, invalid("Parameters: [%s] must have values", strings.Join(missing, ", "))
	}

	return out, nil
}

func ruleValues(info TemplateInfo, explicit map[string]string) map[string]string {
	vals := map[string]string{}

	for _, p := range info.Parameters {
		if p.Default != "" {
			vals[p.Key] = p.Default
		}
	}

	maps.Copy(vals, explicit)

	return vals
}

func (b *InMemoryBackend) stackTags(
	ctx context.Context,
	lp *launchPlan,
	ppARN string,
	user map[string]string,
) map[string]string {
	tags := maps.Clone(lp.productTags)
	if tags == nil {
		tags = map[string]string{}
	}

	maps.Copy(tags, b.tagOptionTags(b.callerAccount(ctx), lp))
	maps.Copy(tags, user)

	tags["aws:servicecatalog:productArn"] = lp.prod.ARN
	tags["aws:servicecatalog:portfolioArn"] = lp.path.ARN

	if ppARN != "" {
		tags["aws:servicecatalog:provisionedProductArn"] = ppARN
	}

	if caller := awsmeta.CallerArn(ctx); caller != "" {
		tags["aws:servicecatalog:provisioningPrincipalArn"] = caller
	}

	return tags
}

func stackSucceeded(status string) bool {
	switch status {
	case "CREATE_COMPLETE", "UPDATE_COMPLETE", "IMPORT_COMPLETE":
		return true
	}

	return false
}

// ProvisionProductInput is the ProvisionProduct request.
type ProvisionProductInput struct {
	ProvisioningPreferences  *ProvisioningPreferences `json:"ProvisioningPreferences,omitempty"`
	ProvisionToken           string                   `json:"ProvisionToken"`
	ProvisionedProductName   string                   `json:"ProvisionedProductName"`
	PathID                   string                   `json:"PathId,omitempty"`
	PathName                 string                   `json:"PathName,omitempty"`
	ProductID                string                   `json:"ProductId,omitempty"`
	ProductName              string                   `json:"ProductName,omitempty"`
	ProvisioningArtifactID   string                   `json:"ProvisioningArtifactId,omitempty"`
	ProvisioningArtifactName string                   `json:"ProvisioningArtifactName,omitempty"`
	NotificationArns         []string                 `json:"NotificationArns,omitempty"`
	ProvisioningParameters   []ProvisioningParameter  `json:"ProvisioningParameters,omitempty"`
	Tags                     []Tag                    `json:"Tags,omitempty"`
}

// ProvisioningParameter is the ProvisioningParameter shape.
type ProvisioningParameter struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

func paramMap(ps []ProvisioningParameter) (map[string]string, error) {
	m := make(map[string]string, len(ps))

	for _, p := range ps {
		if _, dup := m[p.Key]; dup {
			return nil, invalid("parameter %q is given more than once", p.Key)
		}

		m[p.Key] = p.Value
	}

	return m, nil
}

// ProvisionProduct launches a product: it creates the provisioned product and a CloudFormation stack for it.
func (b *InMemoryBackend) ProvisionProduct(
	ctx context.Context,
	in *ProvisionProductInput,
) (*RecordOutputDetail, error) {
	if !ppNameRE.MatchString(in.ProvisionedProductName) {
		return nil, invalid(
			"ProvisionedProductName must be 1-128 letters, digits, '.', '_' or '-' and start with a letter or digit",
		)
	}

	if len(in.NotificationArns) > maxNotificationARNs {
		return nil, invalid("at most %d NotificationArns are allowed", maxNotificationARNs)
	}

	given, err := paramMap(in.ProvisioningParameters)
	if err != nil {
		return nil, err
	}

	userTags := tagsToMap(in.Tags)
	if err = validateTags(userTags); err != nil {
		return nil, err
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	return idemReplay(b, "ProvisionProduct", in.ProvisionToken, in, func() (*RecordOutputDetail, error) {
		b.mu.RLock("ProvisionProduct")
		lp, lerr := b.resolveLaunchLocked(ctx, account, region, in.ProductID, in.ProductName,
			in.ProvisioningArtifactID, in.ProvisioningArtifactName, in.PathID, in.PathName)
		b.mu.RUnlock()

		if lerr != nil {
			return nil, lerr
		}

		return b.launch(ctx, account, region, lp, in, given, userTags)
	})
}

func (b *InMemoryBackend) launch(
	ctx context.Context,
	account, region string,
	lp *launchPlan,
	in *ProvisionProductInput,
	given, userTags map[string]string,
) (*RecordOutputDetail, error) {
	external := isExternalProduct(lp.prod.Type)

	if err := b.checkStackSetLaunch(lp, external, in.ProvisioningPreferences); err != nil {
		return nil, err
	}

	var (
		template string
		params   map[string]string
	)

	if !external {
		body, info, err := b.templateInfoOf(ctx, lp.art)
		if err != nil {
			return nil, err
		}

		if params, err = prepareParameters(info, given, nil, nil); err != nil {
			return nil, err
		}

		if err = b.checkRules(ctx, lp, info, params); err != nil {
			return nil, err
		}

		template = body
	}

	if err := b.checkLaunchRole(ctx, lp.roleARN); err != nil {
		return nil, err
	}

	pp, rec, err := b.startProvisioning(ctx, account, region, lp, in, params, userTags)
	if err != nil {
		return nil, err
	}

	if external {
		return b.detailOf(rec, true), nil
	}

	if lp.stackSet != nil {
		return b.createStackSet(ctx, lp, pp, rec, in, template, params), nil
	}

	req := StackRequest{
		Name: pp.StackName, Template: template, Parameters: params, Tags: rec.Tags, RoleARN: lp.roleARN,
		NotificationARNs: slices.Concat(in.NotificationArns, lp.notify),
	}

	st, serr := b.engineOrNil().CreateStack(ctx, req)

	return b.finish(pp.ID, rec.ID, st, serr, false), nil
}

// checkStackSetLaunch validates the StackSet side of a launch before anything is created.
func (b *InMemoryBackend) checkStackSetLaunch(lp *launchPlan, external bool, prefs *ProvisioningPreferences) error {
	if err := prefs.validate(); err != nil {
		return err
	}

	if lp.stackSet == nil {
		return nil
	}

	if external {
		return invalid("a STACKSET constraint applies to CloudFormation products only")
	}

	if b.stackSetEngine() == nil {
		return fail(ErrOperationNotSupported, "%v", errStackEngineUnavailable)
	}

	_, _, err := lp.stackSet.targets(prefs)

	return err
}

func launchType(lp *launchPlan) string {
	if lp.stackSet != nil {
		return ppTypeCFNStackSet
	}

	return ppTypeCFNStack
}

func (b *InMemoryBackend) detailOf(rec *Record, created bool) *RecordOutputDetail {
	b.mu.RLock("detailOf")
	defer b.mu.RUnlock()

	return &RecordOutputDetail{RecordDetail: b.recordDetailLocked(rec, created)}
}

func (b *InMemoryBackend) startProvisioning(
	ctx context.Context, account, region string, lp *launchPlan, in *ProvisionProductInput,
	params, userTags map[string]string,
) (*ProvisionedProduct, *Record, error) {
	b.mu.Lock("ProvisionProduct")
	defer b.mu.Unlock()

	for _, existing := range b.provisioned.All() {
		if existing.AccountID == account && existing.Region == region && existing.Name == in.ProvisionedProductName {
			return nil, nil, fail(
				ErrDuplicateResource,
				"a provisioned product named %s already exists",
				in.ProvisionedProductName,
			)
		}
	}

	id := newID("pp-")
	pp := &ProvisionedProduct{
		ID:               id,
		ARN:              b.provisionedARN(account, region, in.ProvisionedProductName, id),
		AccountID:        account,
		Region:           region,
		Name:             in.ProvisionedProductName,
		Type:             launchType(lp),
		Status:           ppUnderChange,
		ProductID:        lp.prod.ID,
		ArtifactID:       lp.art.ID,
		PathID:           lp.path.ID,
		StackName:        "SC-" + account + "-" + id,
		LaunchRoleArn:    lp.roleARN,
		IdempotencyToken: in.ProvisionToken,
		CreatedTime:      b.now(),
		Parameters:       params,
		UserArn:          awsmeta.CallerArn(ctx),
		Tags:             userTags,
	}

	rec := b.newRecordLocked(account, region, recProvision, pp)
	rec.Tags = b.stackTags(ctx, lp, pp.ARN, userTags)

	if isExternalProduct(lp.prod.Type) {
		rec.WorkflowToken = workflowTokenFor(rec.ID)
		pp.StackName = ""
	}

	pp.LastRecordID, pp.LastProvisioningRecordID = rec.ID, rec.ID
	b.provisioned.Put(pp)

	return pp, rec, nil
}

// finish records the outcome of a stack create or update on the provisioned product and its record.
func (b *InMemoryBackend) finish(ppID, recID string, st StackInfo, serr error, update bool) *RecordOutputDetail {
	b.mu.Lock("finishProvisioning")
	defer b.mu.Unlock()

	pp, pok := b.provisioned.Get(ppID)
	rec, rok := b.records.Get(recID)

	if !pok || !rok {
		return &RecordOutputDetail{RecordDetail: &RecordDetail{RecordID: recID, Status: recFailed}}
	}

	failed := serr != nil || !stackSucceeded(st.Status)
	b.settleLocked(rec)

	if failed {
		msg := st.Reason
		if serr != nil {
			msg = serr.Error()
		}

		rec.FinalStatus = recFailed
		rec.Errors = []RecordError{
			{Code: "CloudFormationError", Description: strOr(msg, "the stack operation failed ("+st.Status+")")},
		}
		pp.StatusMessage = rec.Errors[0].Description

		pp.Status = ppError
		if update {
			pp.Status = ppTainted
		}
	} else {
		rec.FinalStatus = recSucceeded
		rec.Outputs = st.Outputs
		pp.Outputs, pp.StackID, pp.Status, pp.StatusMessage = st.Outputs, st.ID, statusAvailable, ""
		pp.LastSuccessfulProvisioningRecordID = rec.ID
	}

	return &RecordOutputDetail{RecordDetail: b.recordDetailLocked(rec, true)}
}

// UpdateProvisionedProductInput is the UpdateProvisionedProduct request.
type UpdateProvisionedProductInput struct {
	ProvisioningPreferences  *ProvisioningPreferences      `json:"ProvisioningPreferences,omitempty"`
	UpdateToken              string                        `json:"UpdateToken"`
	ProvisionedProductID     string                        `json:"ProvisionedProductId,omitempty"`
	ProvisionedProductName   string                        `json:"ProvisionedProductName,omitempty"`
	PathID                   string                        `json:"PathId,omitempty"`
	PathName                 string                        `json:"PathName,omitempty"`
	ProductID                string                        `json:"ProductId,omitempty"`
	ProductName              string                        `json:"ProductName,omitempty"`
	ProvisioningArtifactID   string                        `json:"ProvisioningArtifactId,omitempty"`
	ProvisioningArtifactName string                        `json:"ProvisioningArtifactName,omitempty"`
	ProvisioningParameters   []UpdateProvisioningParameter `json:"ProvisioningParameters,omitempty"`
	Tags                     []Tag                         `json:"Tags,omitempty"`
}

// UpdateProvisioningParameter is the UpdateProvisioningParameter shape.
type UpdateProvisioningParameter struct {
	Key              string `json:"Key"`
	Value            string `json:"Value,omitempty"`
	UsePreviousValue bool   `json:"UsePreviousValue,omitempty"`
}

// UpdateProvisionedProduct moves a provisioned product to another version, parameters or tags.
func (b *InMemoryBackend) UpdateProvisionedProduct(
	ctx context.Context,
	in *UpdateProvisionedProductInput,
) (*RecordOutputDetail, error) {
	given, usePrev := map[string]string{}, map[string]bool{}

	for _, p := range in.ProvisioningParameters {
		if p.UsePreviousValue {
			usePrev[p.Key] = true
		} else {
			given[p.Key] = p.Value
		}
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	return idemReplay(b, "UpdateProvisionedProduct", in.UpdateToken, in, func() (*RecordOutputDetail, error) {
		b.mu.RLock("UpdateProvisionedProduct")
		pp, err := b.provisionedLocked(account, region, in.ProvisionedProductID, in.ProvisionedProductName)

		var lp *launchPlan
		if err == nil {
			lp, err = b.resolveUpdateLocked(ctx, account, region, pp, in)
		}

		b.mu.RUnlock()

		if err != nil {
			return nil, err
		}

		newTags := tagsToMap(in.Tags)
		if in.Tags != nil {
			if err = validateTags(newTags); err != nil {
				return nil, err
			}

			if lp.tagUpdateOff && !maps.Equal(newTags, pp.Tags) {
				return nil, invalid(
					"tag updates are not allowed on this provisioned product by a RESOURCE_UPDATE constraint",
				)
			}
		}

		return b.updateStack(ctx, account, region, pp, lp, in, given, usePrev, newTags)
	})
}

func (b *InMemoryBackend) resolveUpdateLocked(
	ctx context.Context, account, region string, pp *ProvisionedProduct, in *UpdateProvisionedProductInput,
) (*launchPlan, error) {
	if st := b.ppStatus(pp); st == ppUnderChange {
		return nil, invalid("provisioned product %s is under change", pp.ID)
	}

	productID, productName := in.ProductID, in.ProductName
	if productID == "" && productName == "" {
		productID = pp.ProductID
	}

	artID, artName := in.ProvisioningArtifactID, in.ProvisioningArtifactName
	if artID == "" && artName == "" {
		artID = pp.ArtifactID
	}

	pathID, pathName := in.PathID, in.PathName
	if pathID == "" && pathName == "" {
		pathID = pp.PathID
	}

	lp, err := b.resolveLaunchLocked(ctx, account, region, productID, productName, artID, artName, pathID, pathName)
	if err != nil {
		return nil, err
	}

	if lp.stackSet != nil || pp.Type == ppTypeCFNStackSet {
		return nil, fail(ErrOperationNotSupported, "updating a STACKSET provisioned product is not supported")
	}

	return lp, nil
}

func (b *InMemoryBackend) updateStack(
	ctx context.Context,
	account, region string,
	pp *ProvisionedProduct,
	lp *launchPlan,
	in *UpdateProvisionedProductInput,
	given map[string]string,
	usePrev map[string]bool,
	newTags map[string]string,
) (*RecordOutputDetail, error) {
	external := isExternalProduct(lp.prod.Type)

	var (
		template string
		params   map[string]string
	)

	if !external {
		body, info, err := b.templateInfoOf(ctx, lp.art)
		if err != nil {
			return nil, err
		}

		b.mu.RLock("updateStack")
		prev := maps.Clone(pp.Parameters)
		b.mu.RUnlock()

		if params, err = prepareParameters(info, given, prev, usePrev); err != nil {
			return nil, err
		}

		if err = b.checkRules(ctx, lp, info, params); err != nil {
			return nil, err
		}

		template = body
	}

	if err := b.checkLaunchRole(ctx, lp.roleARN); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateProvisionedProduct")
	if pp.Type == "" || b.ppStatus(pp) == ppUnderChange {
		b.mu.Unlock()

		return nil, invalid("provisioned product %s is under change", pp.ID)
	}

	if in.Tags != nil {
		pp.Tags = newTags
	}

	pp.Status = ppUnderChange
	rec := b.newRecordLocked(account, region, recUpdate, pp)
	rec.ProductID, rec.ArtifactID, rec.PathID, rec.LaunchRoleArn = lp.prod.ID, lp.art.ID, lp.path.ID, lp.roleARN
	rec.Tags = b.stackTags(ctx, lp, pp.ARN, pp.Tags)
	pp.LastRecordID = rec.ID
	pp.ProductID, pp.ArtifactID, pp.PathID = lp.prod.ID, lp.art.ID, lp.path.ID
	pp.LaunchRoleArn, pp.Parameters = lp.roleARN, params

	if external {
		rec.WorkflowToken = workflowTokenFor(rec.ID)
	}

	stack := pp.StackName
	b.mu.Unlock()

	if external {
		return b.detailOf(rec, true), nil
	}

	st, serr := b.engineOrNil().UpdateStack(ctx, StackRequest{
		Name: stack, Template: template, Parameters: params, Tags: rec.Tags, RoleARN: lp.roleARN,
		NotificationARNs: lp.notify, Update: true,
	})

	return b.finish(pp.ID, rec.ID, st, serr, true), nil
}

// TerminateProvisionedProductInput is the TerminateProvisionedProduct request.
type TerminateProvisionedProductInput struct {
	TerminateToken          string `json:"TerminateToken"`
	ProvisionedProductID    string `json:"ProvisionedProductId,omitempty"`
	ProvisionedProductName  string `json:"ProvisionedProductName,omitempty"`
	IgnoreErrors            bool   `json:"IgnoreErrors,omitempty"`
	RetainPhysicalResources bool   `json:"RetainPhysicalResources,omitempty"`
}

// TerminateProvisionedProduct deletes a provisioned product and its stack.
func (b *InMemoryBackend) TerminateProvisionedProduct(
	ctx context.Context,
	in *TerminateProvisionedProductInput,
) (*RecordOutputDetail, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	return idemReplay(b, "TerminateProvisionedProduct", in.TerminateToken, in, func() (*RecordOutputDetail, error) {
		b.mu.Lock("TerminateProvisionedProduct")
		pp, err := b.provisionedLocked(account, region, in.ProvisionedProductID, in.ProvisionedProductName)

		if err == nil && b.ppStatus(pp) == ppUnderChange {
			err = invalid("provisioned product %s is under change", pp.ID)
		}

		if err != nil {
			b.mu.Unlock()

			return nil, err
		}

		pp.Status = ppUnderChange
		rec := b.newRecordLocked(account, region, recTerminate, pp)
		pp.LastRecordID = rec.ID
		stack, typ := pp.StackName, pp.Type

		if stack == "" {
			rec.WorkflowToken = workflowTokenFor(rec.ID)
		}

		b.mu.Unlock()

		if stack == "" {
			return b.detailOf(rec, true), nil
		}

		var derr error
		if !in.RetainPhysicalResources {
			derr = b.deleteBacking(ctx, typ, stack)
		}

		return b.finishTerminate(pp.ID, rec.ID, derr, in.IgnoreErrors), nil
	})
}

func (b *InMemoryBackend) finishTerminate(ppID, recID string, derr error, ignore bool) *RecordOutputDetail {
	b.mu.Lock("finishTerminate")
	defer b.mu.Unlock()

	pp, pok := b.provisioned.Get(ppID)
	rec, rok := b.records.Get(recID)

	if !pok || !rok {
		return &RecordOutputDetail{RecordDetail: &RecordDetail{RecordID: recID, Status: recFailed}}
	}

	b.settleLocked(rec)

	if derr != nil && !ignore {
		rec.FinalStatus = recFailed
		rec.Errors = []RecordError{{Code: "CloudFormationError", Description: derr.Error()}}
		pp.Status, pp.StatusMessage = ppError, derr.Error()
	} else {
		rec.FinalStatus = recSucceeded
		b.provisioned.Delete(pp.ID)
	}

	return &RecordOutputDetail{RecordDetail: b.recordDetailLocked(rec, true)}
}

// ImportAsProvisionedProductInput is the ImportAsProvisionedProduct request.
type ImportAsProvisionedProductInput struct {
	IdempotencyToken       string `json:"IdempotencyToken"`
	PhysicalID             string `json:"PhysicalId"`
	ProductID              string `json:"ProductId"`
	ProvisionedProductName string `json:"ProvisionedProductName"`
	ProvisioningArtifactID string `json:"ProvisioningArtifactId"`
}

// ImportAsProvisionedProduct adopts an existing CloudFormation stack as a provisioned product.
func (b *InMemoryBackend) ImportAsProvisionedProduct(
	ctx context.Context,
	in *ImportAsProvisionedProductInput,
) (*RecordOutputDetail, error) {
	if !ppNameRE.MatchString(in.ProvisionedProductName) {
		return nil, invalid(
			"ProvisionedProductName must be 1-128 letters, digits, '.', '_' or '-' and start with a letter or digit",
		)
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	return idemReplay(b, "ImportAsProvisionedProduct", in.IdempotencyToken, in, func() (*RecordOutputDetail, error) {
		b.mu.RLock("ImportAsProvisionedProduct")
		lp, err := b.resolveLaunchLocked(ctx, account, region, in.ProductID, "", in.ProvisioningArtifactID, "", "", "")
		eng := b.engine
		b.mu.RUnlock()

		if err != nil {
			return nil, err
		}

		if eng == nil {
			return nil, fail(ErrOperationNotSupported, "%v", errStackEngineUnavailable)
		}

		st, serr := eng.DescribeStack(ctx, in.PhysicalID)
		if serr != nil || !stackSucceeded(st.Status) {
			return nil, invalid("PhysicalId %s is not an importable stack", in.PhysicalID)
		}

		return b.importStack(ctx, account, region, lp, in, st)
	})
}

func (b *InMemoryBackend) importStack(
	ctx context.Context, account, region string, lp *launchPlan, in *ImportAsProvisionedProductInput, st StackInfo,
) (*RecordOutputDetail, error) {
	b.mu.Lock("ImportAsProvisionedProduct")
	defer b.mu.Unlock()

	for _, existing := range b.provisioned.All() {
		if existing.AccountID == account && existing.Region == region &&
			(existing.Name == in.ProvisionedProductName || existing.StackID == st.ID) {
			return nil, fail(
				ErrDuplicateResource,
				"provisioned product %s already exists or the stack is already imported",
				in.ProvisionedProductName,
			)
		}
	}

	id := newID("pp-")
	pp := &ProvisionedProduct{
		ID:               id,
		ARN:              b.provisionedARN(account, region, in.ProvisionedProductName, id),
		AccountID:        account,
		Region:           region,
		Name:             in.ProvisionedProductName,
		Type:             ppTypeCFNStack,
		Status:           statusAvailable,
		ProductID:        lp.prod.ID,
		ArtifactID:       lp.art.ID,
		PathID:           lp.path.ID,
		StackName:        st.Name,
		StackID:          st.ID,
		LaunchRoleArn:    lp.roleARN,
		IdempotencyToken: in.IdempotencyToken,
		CreatedTime:      b.now(),
		UserArn:          awsmeta.CallerArn(ctx),
		Outputs:          st.Outputs,
	}
	b.provisioned.Put(pp)

	rec := b.newRecordLocked(account, region, recImport, pp)
	rec.FinalStatus, rec.Outputs, rec.Tags = recSucceeded, st.Outputs, b.stackTags(ctx, lp, pp.ARN, nil)
	b.settleLocked(rec)
	pp.LastRecordID, pp.LastProvisioningRecordID, pp.LastSuccessfulProvisioningRecordID = rec.ID, rec.ID, rec.ID

	return &RecordOutputDetail{RecordDetail: b.recordDetailLocked(rec, true)}, nil
}

// UpdateProvisionedProductPropertiesInput is the UpdateProvisionedProductProperties request.
type UpdateProvisionedProductPropertiesInput struct {
	ProvisionedProductProperties map[string]string `json:"ProvisionedProductProperties"`
	IdempotencyToken             string            `json:"IdempotencyToken"`
	ProvisionedProductID         string            `json:"ProvisionedProductId"`
}

// UpdateProvisionedProductPropertiesOutput is the UpdateProvisionedProductProperties response.
type UpdateProvisionedProductPropertiesOutput struct {
	ProvisionedProductProperties map[string]string `json:"ProvisionedProductProperties"`
	ProvisionedProductID         string            `json:"ProvisionedProductId"`
	RecordID                     string            `json:"RecordId"`
	Status                       string            `json:"Status"`
}

// UpdateProvisionedProductProperties changes the owner or launch role of a provisioned product.
func (b *InMemoryBackend) UpdateProvisionedProductProperties(
	ctx context.Context,
	in *UpdateProvisionedProductPropertiesInput,
) (*UpdateProvisionedProductPropertiesOutput, error) {
	for k, v := range in.ProvisionedProductProperties {
		if (k != "OWNER" && k != "LAUNCH_ROLE") || !strings.HasPrefix(v, "arn:") {
			return nil, invalid("properties must be OWNER or LAUNCH_ROLE with an ARN value")
		}
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	return idemReplay(
		b,
		"UpdateProvisionedProductProperties",
		in.IdempotencyToken,
		in,
		func() (*UpdateProvisionedProductPropertiesOutput, error) {
			b.mu.Lock("UpdateProvisionedProductProperties")
			defer b.mu.Unlock()

			pp, err := b.provisionedLocked(account, region, in.ProvisionedProductID, "")
			if err != nil {
				return nil, err
			}

			if v, ok := in.ProvisionedProductProperties["OWNER"]; ok {
				pp.Owner = v
			}

			if v, ok := in.ProvisionedProductProperties["LAUNCH_ROLE"]; ok {
				pp.LaunchRoleArn = v
			}

			rec := b.newRecordLocked(account, region, recProperties, pp)
			rec.FinalStatus = recSucceeded
			b.settleLocked(rec)
			pp.LastRecordID = rec.ID

			return &UpdateProvisionedProductPropertiesOutput{
				ProvisionedProductProperties: map[string]string{"OWNER": pp.Owner, "LAUNCH_ROLE": pp.LaunchRoleArn},
				ProvisionedProductID:         pp.ID, RecordID: rec.ID, Status: recCreated,
			}, nil
		},
	)
}

// workflowTokenFor is the token an external engine presents for a record: the emulator has no channel to hand one over.
func workflowTokenFor(recordID string) string { return "wf-" + recordID }
