package iot

import (
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"
)

func (b *InMemoryBackend) GetRegistrationCode() string {
	var code string
	func() {
		b.mu.RLock()
		defer b.mu.RUnlock()
		code = b.registrationCode
	}()

	if code != "" {
		return code
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.registrationCode == "" {
		b.registrationCode = uuid.NewString()
	}

	return b.registrationCode
}

func (b *InMemoryBackend) DeleteRegistrationCode() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.registrationCode = ""

	return nil
}

// registrationThingNameParam is the conventional provisioning-template
// parameter used to supply an explicit thing name to RegisterThing.
const registrationThingNameParam = "ThingName"

// registrationSerialNumberParam is a common fallback parameter used by
// provisioning templates to derive a thing name when ThingName is absent.
const registrationSerialNumberParam = "SerialNumber"

// RegisterThing provisions (creates or updates) a thing from a provisioning
// template body and parameters, generating a device certificate and
// attaching it to the thing.
func (b *InMemoryBackend) RegisterThing(input *RegisterThingInput) (*RegisterThingOutput, error) {
	if input.TemplateBody == "" {
		return nil, fmt.Errorf("%w: TemplateBody is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	thingName := input.Parameters[registrationThingNameParam]
	if thingName == "" {
		thingName = input.Parameters[registrationSerialNumberParam]
	}

	if thingName == "" {
		thingName = "thing-" + uuid.NewString()
	}

	arn := fmt.Sprintf("arn:aws:iot:%s:%s:thing/%s", b.region, b.accountID, thingName)

	if t, exists := b.things.Get(thingName); exists {
		if t.Attributes == nil {
			t.Attributes = make(map[string]string)
		}

		maps.Copy(t.Attributes, input.Parameters)
		t.Version++
	} else {
		attrs := make(map[string]string, len(input.Parameters))
		maps.Copy(attrs, input.Parameters)

		b.things.Put(&Thing{
			ThingName:  thingName,
			ThingID:    uuid.NewString(),
			ARN:        arn,
			Attributes: attrs,
			Version:    1,
			CreatedAt:  time.Now(),
		})
	}

	cert := b.newCertificate(fakePEM, certStatusActive)
	b.certificates.Put(cert)
	b.thingPrincipals[thingName] = append(b.thingPrincipals[thingName], cert.ARN)

	return &RegisterThingOutput{
		CertificatePem: cert.PEM,
		ResourceArns: map[string]string{
			"thing":       arn,
			"certificate": cert.ARN,
		},
	}, nil
}

// Registration task status values (mirrors the AWS IoT "Status" enum).
const (
	registrationTaskInProgress = "InProgress"
	registrationTaskCompleted  = "Completed"
	registrationTaskFailed     = "Failed"
	registrationTaskCancelled  = "Cancelled"
)

func cloneRegistrationTask(t *ThingRegistrationTask) *ThingRegistrationTask {
	cp := *t

	return &cp
}

// StartThingRegistrationTask creates a bulk thing provisioning task.
func (b *InMemoryBackend) StartThingRegistrationTask(
	input *StartThingRegistrationTaskInput,
) (*ThingRegistrationTask, error) {
	if input.TemplateBody == "" || input.InputFileBucket == "" ||
		input.InputFileKey == "" || input.RoleArn == "" {
		return nil, fmt.Errorf(
			"%w: TemplateBody, InputFileBucket, InputFileKey, and RoleArn are required", ErrValidation,
		)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := float64(time.Now().Unix())
	task := &ThingRegistrationTask{
		TaskID:           uuid.NewString(),
		Status:           registrationTaskInProgress,
		TemplateBody:     input.TemplateBody,
		InputFileBucket:  input.InputFileBucket,
		InputFileKey:     input.InputFileKey,
		RoleArn:          input.RoleArn,
		CreationDate:     now,
		LastModifiedDate: now,
	}

	b.registrationTasks.Put(task)

	return cloneRegistrationTask(task), nil
}

// StopThingRegistrationTask cancels a bulk thing provisioning task.
func (b *InMemoryBackend) StopThingRegistrationTask(taskID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	task, ok := b.registrationTasks.Get(taskID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrRegistrationTaskNotFound, taskID)
	}

	task.Status = registrationTaskCancelled
	task.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

// DescribeThingRegistrationTask returns a bulk thing provisioning task by ID.
func (b *InMemoryBackend) DescribeThingRegistrationTask(taskID string) (*ThingRegistrationTask, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	task, ok := b.registrationTasks.Get(taskID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRegistrationTaskNotFound, taskID)
	}

	return cloneRegistrationTask(task), nil
}

// ListThingRegistrationTasks lists bulk thing provisioning tasks, optionally
// filtered by status.
func (b *InMemoryBackend) ListThingRegistrationTasks(status string) []*ThingRegistrationTask {
	b.mu.RLock()
	defer b.mu.RUnlock()

	items := b.registrationTasks.Snapshot()
	out := make([]*ThingRegistrationTask, 0, len(items))

	for _, v := range items {
		task := v
		if status != "" && task.Status != status {
			continue
		}

		out = append(out, cloneRegistrationTask(task))
	}

	return out
}

// ListThingRegistrationTaskReports returns the resource-report links for a
// bulk thing provisioning task.
func (b *InMemoryBackend) ListThingRegistrationTaskReports(taskID, reportType string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	task, ok := b.registrationTasks.Get(taskID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRegistrationTaskNotFound, taskID)
	}

	if reportType == "" {
		reportType = "RESULTS"
	}

	link := fmt.Sprintf(
		"https://%s.s3.amazonaws.com/%s/%s/%s.json",
		task.InputFileBucket, task.TaskID, reportType, task.TaskID,
	)

	return []string{link}, nil
}

// defaultThingPrincipalType is the AWS IoT default relation type applied when
// a principal's attachment type was not recorded (e.g. attached via the v1
// AttachThingPrincipal operation, which does not persist a relation type).
const defaultThingPrincipalType = "NON_EXCLUSIVE_THING"

// managedJobTemplateVersion is the (only) version published for every entry
// in the fixed managed job template catalog.
const managedJobTemplateVersion = "1.0"

// envGreengrassV2 is the AWS IoT Greengrass V2 managed-template environment tag.
const envGreengrassV2 = "AWS_Greengrass_V2"

// managedJobTemplateCatalog returns the fixed set of AWS-managed job
// templates. Managed templates are AWS-owned reference data: they are not
// mutated by the account, so the catalog is derived (not stored per-account).
func (b *InMemoryBackend) managedJobTemplateCatalog() []*ManagedJobTemplate {
	arn := func(name string) string {
		return fmt.Sprintf("arn:aws:iot:%s::managedjobtemplate/%s", b.region, name)
	}
	doc := func(body string) string {
		return fmt.Sprintf(`{"version":%q,"steps":[%s]}`, managedJobTemplateVersion, body)
	}

	return []*ManagedJobTemplate{
		{
			TemplateName:    "AWS-Reboot",
			TemplateArn:     arn("AWS-Reboot"),
			TemplateVersion: managedJobTemplateVersion,
			Description:     "Reboots the device.",
			Environments:    []string{envGreengrassV2},
			Document:        doc(`{"action":{"name":"reboot","type":"RUN_COMMAND","input":{"command":["reboot"]}}}`),
			DocumentParameters: []ManagedJobTemplateParameter{
				{
					Key:         "RebootTimeSeconds",
					Description: "Delay in seconds before the reboot is executed.",
					Regex:       `^[0-9]+$`,
					Example:     "60",
					Optional:    true,
				},
			},
		},
		{
			TemplateName:    "AWS-DownloadFile",
			TemplateArn:     arn("AWS-DownloadFile"),
			TemplateVersion: managedJobTemplateVersion,
			Description:     "Downloads a file from a source location to the device.",
			Environments:    []string{envGreengrassV2, "FreeRTOS"},
			Document: doc(`{"action":{"name":"downloadFile","type":"RUN_COMMAND",` +
				`"input":{"sourceUrl":"{{SourceUrl}}","destinationPath":"{{DestinationPath}}"}}}`),
			DocumentParameters: []ManagedJobTemplateParameter{
				{
					Key:         "SourceUrl",
					Description: "The URL of the file to download.",
					Regex:       `^https?://.+`,
					Example:     "https://example.com/firmware.bin",
				},
				{
					Key:         "DestinationPath",
					Description: "The path on the device to save the downloaded file.",
					Regex:       `^/.+`,
					Example:     "/tmp/firmware.bin",
				},
			},
		},
		{
			TemplateName:    "AWS-UpdateDeviceCertificate",
			TemplateArn:     arn("AWS-UpdateDeviceCertificate"),
			TemplateVersion: managedJobTemplateVersion,
			Description:     "Updates the device certificate used to connect to AWS IoT Core.",
			Environments:    []string{envGreengrassV2},
			Document: doc(`{"action":{"name":"rotateCertificate","type":"RUN_COMMAND",` +
				`"input":{"certificateId":"{{CertificateId}}"}}}`),
			DocumentParameters: []ManagedJobTemplateParameter{
				{
					Key:         "CertificateId",
					Description: "The ID of the new certificate to install.",
					Regex:       `^[a-f0-9]{64}$`,
					Example:     "1234abcd5678ef90",
				},
			},
		},
	}
}

func cloneManagedJobTemplate(t *ManagedJobTemplate) *ManagedJobTemplate {
	cp := *t
	cp.Environments = append([]string(nil), t.Environments...)
	cp.DocumentParameters = append([]ManagedJobTemplateParameter(nil), t.DocumentParameters...)

	return &cp
}

// DescribeManagedJobTemplate returns an AWS-managed job template by name (and
// optional version; the catalog only has one version per template today).
func (b *InMemoryBackend) DescribeManagedJobTemplate(
	templateName, templateVersion string,
) (*ManagedJobTemplate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, t := range b.managedJobTemplateCatalog() {
		if t.TemplateName != templateName {
			continue
		}

		if templateVersion != "" && templateVersion != t.TemplateVersion {
			continue
		}

		return cloneManagedJobTemplate(t), nil
	}

	return nil, fmt.Errorf("%w: %s", ErrManagedJobTemplateNotFound, templateName)
}

// ListManagedJobTemplates lists the AWS-managed job templates, optionally
// filtered by template name.
func (b *InMemoryBackend) ListManagedJobTemplates(templateName string) []*ManagedJobTemplateSummary {
	b.mu.RLock()
	defer b.mu.RUnlock()

	catalog := b.managedJobTemplateCatalog()
	out := make([]*ManagedJobTemplateSummary, 0, len(catalog))

	for _, t := range catalog {
		if templateName != "" && t.TemplateName != templateName {
			continue
		}

		out = append(out, &ManagedJobTemplateSummary{
			TemplateName:    t.TemplateName,
			TemplateArn:     t.TemplateArn,
			TemplateVersion: t.TemplateVersion,
			Description:     t.Description,
			Environments:    append([]string(nil), t.Environments...),
		})
	}

	return out
}
