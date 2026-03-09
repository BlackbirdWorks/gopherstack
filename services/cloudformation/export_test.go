package cloudformation

// TopoSortResources exposes topoSortResources for white-box testing.
func TopoSortResources(resources map[string]TemplateResource) []string {
	return topoSortResources(resources)
}

// ParseDependsOn exposes parseDependsOn for white-box testing.
func ParseDependsOn(v any) []string {
	return parseDependsOn(v)
}
