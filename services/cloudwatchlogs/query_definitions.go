package cloudwatchlogs

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// PutQueryDefinition creates or updates a CWLI query definition.
func (b *InMemoryBackend) PutQueryDefinition(
	name, queryString, queryDefinitionID string,
	logGroupNames []string,
	parameters []QueryParameter,
) (string, error) {
	return b.PutQueryDefinitionWithLanguage(name, queryString, queryDefinitionID, "", logGroupNames, parameters)
}

// PutQueryDefinitionWithLanguage is PutQueryDefinition with an explicit queryLanguage (CWLI default).
func (b *InMemoryBackend) PutQueryDefinitionWithLanguage(
	name, queryString, queryDefinitionID, queryLanguage string,
	logGroupNames []string,
	parameters []QueryParameter,
) (string, error) {
	if queryLanguage == "" {
		queryLanguage = queryLanguageCWLI
	}

	if _, ok := validScheduledQueryLanguages()[queryLanguage]; !ok {
		return "", fmt.Errorf(
			"%w: invalid queryLanguage %q, must be one of CWLI, PPL, SQL",
			ErrValidation,
			queryLanguage,
		)
	}

	if name == "" {
		return "", fmt.Errorf("%w: name is required", ErrValidation)
	}
	if queryString == "" {
		return "", fmt.Errorf("%w: queryString is required", ErrValidation)
	}

	b.mu.Lock("PutQueryDefinition")
	defer b.mu.Unlock()

	id := queryDefinitionID
	if id == "" {
		// New entry: enforce the cap.
		if b.queryDefinitions.Len() >= maxQueryDefinitions {
			return "", fmt.Errorf("%w: query definition limit exceeded", ErrValidation)
		}
		id = uuid.New().String()
	} else if !b.queryDefinitions.Has(id) {
		// Update path: the supplied ID must reference an existing definition.
		return "", fmt.Errorf(
			"%w: query definition %s not found",
			ErrQueryDefinitionNotFound, id,
		)
	}
	qd := &QueryDefinition{
		QueryDefinitionID: id,
		Name:              name,
		QueryString:       queryString,
		QueryLanguage:     queryLanguage,
		LogGroupNames:     slices.Clone(logGroupNames),
		Parameters:        slices.Clone(parameters),
		LastModified:      time.Now().UnixMilli(),
	}
	b.queryDefinitions.Put(qd)

	return id, nil
}

// DescribeQueryDefinitions lists query definitions optionally filtered by name prefix.
func (b *InMemoryBackend) DescribeQueryDefinitions(
	queryDefinitionNamePrefix string,
	limit int,
	nextToken string,
) ([]QueryDefinition, string, error) {
	return b.DescribeQueryDefinitionsByLanguage(queryDefinitionNamePrefix, "", limit, nextToken)
}

// DescribeQueryDefinitionsByLanguage is DescribeQueryDefinitions plus a queryLanguage filter.
func (b *InMemoryBackend) DescribeQueryDefinitionsByLanguage(
	queryDefinitionNamePrefix, queryLanguage string,
	limit int,
	nextToken string,
) ([]QueryDefinition, string, error) {
	b.mu.RLock("DescribeQueryDefinitions")
	defer b.mu.RUnlock()

	all := make([]QueryDefinition, 0, b.queryDefinitions.Len())
	for _, qd := range b.queryDefinitions.All() {
		if queryDefinitionNamePrefix != "" &&
			!strings.HasPrefix(qd.Name, queryDefinitionNamePrefix) {
			continue
		}

		if queryLanguage != "" && cmp.Or(qd.QueryLanguage, queryLanguageCWLI) != queryLanguage {
			continue
		}

		cp := *qd
		cp.LogGroupNames = slices.Clone(qd.LogGroupNames)
		cp.Parameters = slices.Clone(qd.Parameters)
		all = append(all, cp)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Name != all[j].Name {
			return all[i].Name < all[j].Name
		}

		return all[i].QueryDefinitionID < all[j].QueryDefinitionID
	})

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []QueryDefinition{}, "", nil
	}
	if limit <= 0 {
		limit = defaultDescribeLimit
	}
	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// DeleteQueryDefinition deletes a query definition by ID.
func (b *InMemoryBackend) DeleteQueryDefinition(queryDefinitionID string) error {
	if queryDefinitionID == "" {
		return fmt.Errorf("%w: queryDefinitionId is required", ErrValidation)
	}

	b.mu.Lock("DeleteQueryDefinition")
	defer b.mu.Unlock()

	if !b.queryDefinitions.Delete(queryDefinitionID) {
		return fmt.Errorf(
			"%w: query definition %s not found",
			ErrQueryDefinitionNotFound,
			queryDefinitionID,
		)
	}

	return nil
}
