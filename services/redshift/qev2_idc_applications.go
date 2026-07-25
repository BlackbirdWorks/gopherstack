package redshift

import "fmt"

// CreateQev2IdcApplication creates a new Amazon Redshift Query Editor (QEV2)
// IAM Identity Center application. Real CreateQev2IdcApplicationInput
// requires IdcDisplayName, IdcInstanceArn, and Qev2IdcApplicationName
// (confirmed against aws-sdk-go-v2/service/redshift@v1.65.0
// CreateQev2IdcApplicationInput's doc comments), so all three are validated
// as non-empty here -- matching the non-empty-only validation style the
// neighboring CreateIdcApplication (RedshiftIdcApplication) uses for its own
// ARN/name fields; neither family in this package validates ARN *format*.
func (b *InMemoryBackend) CreateQev2IdcApplication(
	appName, idcInstanceArn, idcDisplayName string, tags map[string]string,
) (*Qev2IdcApplication, error) {
	if appName == "" {
		return nil, fmt.Errorf("%w: Qev2IdcApplicationName is required", ErrInvalidParameter)
	}

	if idcInstanceArn == "" {
		return nil, fmt.Errorf("%w: IdcInstanceArn is required", ErrInvalidParameter)
	}

	if idcDisplayName == "" {
		return nil, fmt.Errorf("%w: IdcDisplayName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateQev2IdcApplication")
	defer b.mu.Unlock()

	// Real AWS returns Qev2IdcApplicationAlreadyExistsFault ("Use a different
	// application name") keyed by name, matching RedshiftIdcApplication's own
	// name-keyed uniqueness -- there is no separate account-wide quota fault
	// for this family (unlike RedshiftIdcApplicationQuotaExceededFault, which
	// has no Qev2 counterpart in errors.go).
	if _, exists := b.qev2IdcApplications.Get(appName); exists {
		return nil, fmt.Errorf("%w: application %s already exists", ErrQev2IdcApplicationAlreadyExists, appName)
	}

	arn := "arn:aws:redshift:" + b.region + ":" + b.accountID + ":qev2idcapplication/" + appName
	managedArn := "arn:aws:sso::" + b.accountID + ":application/" + idcInstanceArn + "/" + appName

	app := &Qev2IdcApplication{
		Qev2IdcApplicationArn:    arn,
		Qev2IdcApplicationName:   appName,
		IdcInstanceArn:           idcInstanceArn,
		IdcDisplayName:           idcDisplayName,
		IdcManagedApplicationArn: managedArn,
		IdcOnboardStatus:         qev2IdcOnboardStatusComplete,
		Tags:                     tags,
	}
	b.qev2IdcApplications.Put(app)

	cp := *app

	return &cp, nil
}

// DeleteQev2IdcApplication deletes the Qev2 IdC application identified by ARN.
func (b *InMemoryBackend) DeleteQev2IdcApplication(appArn string) error {
	if appArn == "" {
		return fmt.Errorf("%w: Qev2IdcApplicationArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteQev2IdcApplication")
	defer b.mu.Unlock()

	for _, app := range b.qev2IdcApplications.All() {
		if app.Qev2IdcApplicationArn == appArn {
			b.qev2IdcApplications.Delete(app.Qev2IdcApplicationName)

			return nil
		}
	}

	return fmt.Errorf("%w: application %s not found", ErrQev2IdcApplicationNotFound, appArn)
}

// DescribeQev2IdcApplications returns Qev2 IdC applications. When appArn is
// non-empty it looks up that single application (no pagination). Otherwise it
// pages through the full set via marker/maxRecords using exactly the same
// convention as DescribeClusters (see store.go): results are sorted ascending
// by key (Qev2IdcApplicationName, the table's key), marker is the last
// application name returned on the previous page (an exclusive cutoff), and
// the returned nextMarker is empty once the final page is reached.
func (b *InMemoryBackend) DescribeQev2IdcApplications(
	appArn, marker string, maxRecords int,
) ([]Qev2IdcApplication, string, error) {
	b.mu.RLock("DescribeQev2IdcApplications")
	defer b.mu.RUnlock()

	if appArn != "" {
		for _, app := range b.qev2IdcApplications.All() {
			if app.Qev2IdcApplicationArn == appArn {
				cp := *app

				return []Qev2IdcApplication{cp}, "", nil
			}
		}

		return nil, "", fmt.Errorf("%w: application %s not found", ErrQev2IdcApplicationNotFound, appArn)
	}

	sorted := b.qev2IdcApplications.Snapshot()

	if marker != "" {
		cut := 0
		for cut < len(sorted) && sorted[cut].Qev2IdcApplicationName <= marker {
			cut++
		}

		sorted = sorted[cut:]
	}

	nextMarker := ""
	if maxRecords > 0 && len(sorted) > maxRecords {
		sorted = sorted[:maxRecords]
		nextMarker = sorted[len(sorted)-1].Qev2IdcApplicationName
	}

	apps := make([]Qev2IdcApplication, 0, len(sorted))
	for _, app := range sorted {
		apps = append(apps, *app)
	}

	return apps, nextMarker, nil
}

// ModifyQev2IdcApplication updates the display name of a Qev2 IdC
// application. Real ModifyQev2IdcApplicationInput exposes only
// Qev2IdcApplicationArn (the lookup key) and IdcDisplayName -- unlike
// ModifyRedshiftIdcApplication, which additionally accepts IamRoleArn, there
// is no IAM role on this resource to modify. IdcInstanceArn and
// Qev2IdcApplicationName are immutable: the real input shape has no fields
// for them, so there is nothing to accept or reject for those beyond ignoring
// any caller-supplied value (the awsquery form simply never carries one).
func (b *InMemoryBackend) ModifyQev2IdcApplication(appArn, idcDisplayName string) (*Qev2IdcApplication, error) {
	if appArn == "" {
		return nil, fmt.Errorf("%w: Qev2IdcApplicationArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyQev2IdcApplication")
	defer b.mu.Unlock()

	for _, app := range b.qev2IdcApplications.All() {
		if app.Qev2IdcApplicationArn == appArn {
			if idcDisplayName != "" {
				app.IdcDisplayName = idcDisplayName
			}

			cp := *app

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: application %s not found", ErrQev2IdcApplicationNotFound, appArn)
}
