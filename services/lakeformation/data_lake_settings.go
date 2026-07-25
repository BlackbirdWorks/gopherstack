package lakeformation

import "maps"

// GetDataLakeSettings returns the current data lake settings.
func (b *InMemoryBackend) GetDataLakeSettings() *DataLakeSettings {
	b.mu.RLock("GetDataLakeSettings")
	defer b.mu.RUnlock()

	if b.dataLakeSettings == nil {
		return &DataLakeSettings{}
	}

	return copyDataLakeSettings(b.dataLakeSettings)
}

// PutDataLakeSettings replaces the data lake settings.
func (b *InMemoryBackend) PutDataLakeSettings(settings *DataLakeSettings) {
	b.mu.Lock("PutDataLakeSettings")
	defer b.mu.Unlock()

	b.dataLakeSettings = copyDataLakeSettings(settings)
}

// copyDataLakeSettings returns a deep copy of the DataLakeSettings.
func copyDataLakeSettings(s *DataLakeSettings) *DataLakeSettings {
	if s == nil {
		return nil
	}

	cp := &DataLakeSettings{}

	if s.DataLakeAdmins != nil {
		cp.DataLakeAdmins = make([]DataLakePrincipal, len(s.DataLakeAdmins))
		copy(cp.DataLakeAdmins, s.DataLakeAdmins)
	}

	if s.CreateDatabaseDefaultPermissions != nil {
		cp.CreateDatabaseDefaultPermissions = copyPrincipalPermissions(s.CreateDatabaseDefaultPermissions)
	}

	if s.CreateTableDefaultPermissions != nil {
		cp.CreateTableDefaultPermissions = copyPrincipalPermissions(s.CreateTableDefaultPermissions)
	}

	if s.TrustedResourceOwners != nil {
		cp.TrustedResourceOwners = make([]string, len(s.TrustedResourceOwners))
		copy(cp.TrustedResourceOwners, s.TrustedResourceOwners)
	}

	if s.ReadOnlyAdmins != nil {
		cp.ReadOnlyAdmins = make([]DataLakePrincipal, len(s.ReadOnlyAdmins))
		copy(cp.ReadOnlyAdmins, s.ReadOnlyAdmins)
	}

	if s.Parameters != nil {
		cp.Parameters = make(map[string]string, len(s.Parameters))
		maps.Copy(cp.Parameters, s.Parameters)
	}

	cp.AllowExternalDataFiltering = s.AllowExternalDataFiltering
	cp.AllowFullTableExternalDataAccess = s.AllowFullTableExternalDataAccess

	if s.AuthorizedSessionTagValueList != nil {
		cp.AuthorizedSessionTagValueList = make([]string, len(s.AuthorizedSessionTagValueList))
		copy(cp.AuthorizedSessionTagValueList, s.AuthorizedSessionTagValueList)
	}

	if s.ExternalDataFilteringAllowList != nil {
		cp.ExternalDataFilteringAllowList = make([]DataLakePrincipal, len(s.ExternalDataFilteringAllowList))
		copy(cp.ExternalDataFilteringAllowList, s.ExternalDataFilteringAllowList)
	}

	return cp
}

// copyPrincipalPermissions returns a deep copy of a []PrincipalPermissions slice,
// copying the Permissions []string slice and cloning the Principal pointer for each element.
func copyPrincipalPermissions(src []PrincipalPermissions) []PrincipalPermissions {
	dst := make([]PrincipalPermissions, len(src))

	for i, pp := range src {
		elem := PrincipalPermissions{}

		if pp.Principal != nil {
			p := *pp.Principal
			elem.Principal = &p
		}

		if pp.Permissions != nil {
			elem.Permissions = make([]string, len(pp.Permissions))
			copy(elem.Permissions, pp.Permissions)
		}

		dst[i] = elem
	}

	return dst
}
