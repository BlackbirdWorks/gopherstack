package appintegrations

// CFNUpdateApplication applies a CloudFormation update, which unlike UpdateApplication may also change Namespace.
func (b *InMemoryBackend) CFNUpdateApplication(in *UpdateApplicationInput, namespace string) error {
	if namespace != "" {
		if err := checkName("Namespace", namespace); err != nil {
			return err
		}
	}

	if err := b.UpdateApplication(in); err != nil {
		return err
	}

	if namespace == "" {
		return nil
	}

	b.mu.Lock("CFNUpdateApplication")
	defer b.mu.Unlock()

	a, err := b.appLocked(in.Arn)
	if err != nil {
		return err
	}

	if a.Namespace == namespace {
		return nil
	}

	for _, o := range b.apps.All() {
		if o.Namespace == namespace {
			return duplicateErr("Application namespace", namespace)
		}
	}

	a.Namespace = namespace

	return nil
}

// CFNUpdateDataIntegration applies a CloudFormation update, which unlike UpdateDataIntegration may also change
// the file and object configuration.
func (b *InMemoryBackend) CFNUpdateDataIntegration(
	identifier string,
	name, description *string,
	file *FileConfiguration,
	object map[string]map[string][]string,
) error {
	if err := b.UpdateDataIntegration(identifier, name, description); err != nil {
		return err
	}

	b.mu.Lock("CFNUpdateDataIntegration")
	defer b.mu.Unlock()

	d, err := b.dataLocked(identifier)
	if err != nil {
		return err
	}

	d.FileConfiguration = file
	d.ObjectConfiguration = object

	return nil
}
