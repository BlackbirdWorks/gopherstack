package cloudformation

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// GetTemplate returns the template body for a stack.
func (b *InMemoryBackend) GetTemplate(nameOrID string) (string, error) {
	b.mu.RLock("GetTemplate")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return "", ErrStackNotFound
	}

	return stack.TemplateBody, nil
}

// GetTemplateSummary returns summary information about a template body or an existing stack's template.
func (b *InMemoryBackend) GetTemplateSummary(templateBody, stackName string) (*TemplateSummary, error) {
	b.mu.RLock("GetTemplateSummary")
	defer b.mu.RUnlock()

	if templateBody == "" && stackName != "" {
		stack, ok := b.resolveStack(stackName)
		if !ok {
			return nil, ErrStackNotFound
		}

		templateBody = stack.TemplateBody
	}

	if templateBody == "" {
		return &TemplateSummary{}, nil
	}

	tmpl, err := ParseTemplate(templateBody)
	if err != nil {
		return nil, err
	}

	params := make([]ParameterDeclaration, 0, len(tmpl.Parameters))
	for key, pd := range tmpl.Parameters {
		defaultVal := ""
		if pd.Default != nil {
			defaultVal = fmt.Sprintf("%v", pd.Default)
		}

		params = append(params, ParameterDeclaration{
			ParameterKey:          key,
			ParameterType:         pd.Type,
			DefaultValue:          defaultVal,
			Description:           pd.Description,
			AllowedValues:         pd.AllowedValues,
			ConstraintDescription: pd.ConstraintDescription,
			AllowedPattern:        pd.AllowedPattern,
			NoEcho:                pd.NoEcho,
		})
	}

	sort.Slice(params, func(i, j int) bool { return params[i].ParameterKey < params[j].ParameterKey })

	typesSet := make(map[string]struct{}, len(tmpl.Resources))
	for _, res := range tmpl.Resources {
		typesSet[res.Type] = struct{}{}
	}

	resourceTypes := collections.SortedKeys(typesSet)

	return &TemplateSummary{
		Description:   tmpl.Description,
		Parameters:    params,
		ResourceTypes: resourceTypes,
	}, nil
}

// EstimateTemplateCost returns a mock cost estimation URL.
func (b *InMemoryBackend) EstimateTemplateCost(_ string, _ []Parameter) (string, error) {
	return cfnEstimateCostURL, nil
}

func (b *InMemoryBackend) ValidateTemplate(templateBody string) (*TemplateSummary, error) {
	return b.GetTemplateSummary(templateBody, "")
}
