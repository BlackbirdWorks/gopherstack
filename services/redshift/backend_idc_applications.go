package redshift

import "fmt"

// CreateIdcApplication creates a new Redshift IDC application.
func (b *InMemoryBackend) CreateIdcApplication(
	appName, idcInstanceArn, idcDisplayName, iamRoleArn string,
) (*IdcApplication, error) {
	if appName == "" {
		return nil, fmt.Errorf("%w: IdcApplicationName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateIdcApplication")
	defer b.mu.Unlock()

	if _, exists := b.idcApplications.Get(appName); exists {
		return nil, fmt.Errorf("%w: application %s already exists", ErrIdcApplicationAlreadyExists, appName)
	}

	arn := "arn:aws:redshift:" + b.region + ":" + b.accountID + ":redshiftidcapplication/" + appName
	app := &IdcApplication{
		IdcApplicationArn:  arn,
		IdcApplicationName: appName,
		IdcInstanceArn:     idcInstanceArn,
		IdcDisplayName:     idcDisplayName,
		IamRoleArn:         iamRoleArn,
	}
	b.idcApplications.Put(app)

	cp := *app

	return &cp, nil
}

// DeleteIdcApplication deletes the named IDC application.
func (b *InMemoryBackend) DeleteIdcApplication(appArn string) error {
	if appArn == "" {
		return fmt.Errorf("%w: IdcApplicationArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIdcApplication")
	defer b.mu.Unlock()

	for _, app := range b.idcApplications.All() {
		if app.IdcApplicationArn == appArn || app.IdcApplicationName == appArn {
			b.idcApplications.Delete(app.IdcApplicationName)

			return nil
		}
	}

	return fmt.Errorf("%w: application %s not found", ErrIdcApplicationNotFound, appArn)
}

// DescribeIdcApplications returns IDC applications, optionally filtered by ARN.
func (b *InMemoryBackend) DescribeIdcApplications(appArn string) ([]IdcApplication, error) {
	b.mu.RLock("DescribeIdcApplications")
	defer b.mu.RUnlock()

	if appArn != "" {
		for _, app := range b.idcApplications.All() {
			if app.IdcApplicationArn == appArn {
				cp := *app

				return []IdcApplication{cp}, nil
			}
		}

		return nil, fmt.Errorf("%w: application %s not found", ErrIdcApplicationNotFound, appArn)
	}

	result := make([]IdcApplication, 0, b.idcApplications.Len())

	for _, app := range b.idcApplications.All() {
		result = append(result, *app)
	}

	return result, nil
}

// ModifyIdcApplication updates the display name and IAM role of an IDC application.
func (b *InMemoryBackend) ModifyIdcApplication(
	appArn, idcDisplayName, iamRoleArn string,
) (*IdcApplication, error) {
	if appArn == "" {
		return nil, fmt.Errorf("%w: IdcApplicationArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIdcApplication")
	defer b.mu.Unlock()

	for _, app := range b.idcApplications.All() {
		if app.IdcApplicationArn == appArn {
			if idcDisplayName != "" {
				app.IdcDisplayName = idcDisplayName
			}

			if iamRoleArn != "" {
				app.IamRoleArn = iamRoleArn
			}

			cp := *app

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: application %s not found", ErrIdcApplicationNotFound, appArn)
}
