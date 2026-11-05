package azuresearch

import (
	"sort"
	"strings"
)

const (
	redacted     = "<redacted>"
	maxObjectLen = 128
)

var supportedSources = map[string]bool{"azureblob": true, "cosmosdb": true} //nolint:gochecknoglobals // read-only

func validObjectName(name string) bool {
	if name == "" || len(name) > maxObjectLen {
		return false
	}

	for i, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || (r == '-' && i > 0 && i < len(name)-1)
		if !ok {
			return false
		}
	}

	return true
}

func kindLabel(kind string) string {
	switch kind {
	case kindSynonymMaps:
		return "synonym map"
	case kindDataSources:
		return "data source"
	case kindIndexers:
		return "indexer"
	}

	return "skillset"
}

func (s *Service) object(kind, name string) (Object, error) {
	o, ok := s.Objects[kind][name]
	if !ok {
		return nil, wrapf(
			ErrObjectNotFound,
			"No %s with the name '%s' was found in service '%s'.",
			kindLabel(kind),
			name,
			s.Name,
		)
	}

	return o, nil
}

func redact(kind string, o Object) Object {
	out := cloneMap(o)
	if kind != kindDataSources {
		return out
	}

	if cred := objOf(out["credentials"]); cred != nil {
		if cs, ok := cred["connectionString"].(string); ok && cs != "" {
			cred["connectionString"] = redacted
		}
	}

	return out
}

func (s *Service) validateObject(kind string, o Object) error {
	if !validObjectName(strOf(o, "name")) {
		return badf("The request is invalid. Details: The %s name '%s' is invalid. Names must be lowercase letters, "+
			"digits or dashes, not start or end with a dash, and be at most %d characters.",
			kindLabel(kind), strOf(o, "name"), maxObjectLen)
	}

	switch kind {
	case kindSynonymMaps:
		return validateSynonymMap(o)
	case kindDataSources:
		return validateDataSource(o)
	case kindIndexers:
		return s.validateIndexer(o)
	}

	return validateSkillset(o)
}

func validateSynonymMap(o Object) error {
	if f := strOf(o, "format"); f != "" && f != "solr" {
		return badf("The request is invalid. Details: synonym map format '%s' is not supported; use 'solr'.", f)
	}

	o["format"] = "solr"

	syn, ok := o["synonyms"].(string)
	if !ok || strings.TrimSpace(syn) == "" {
		return badf("The request is invalid. Details: synonyms: The synonyms must not be empty.")
	}

	if _, hasCS := o["encryptionKey"]; !hasCS {
		o["encryptionKey"] = nil
	}

	return nil
}

func validateDataSource(o Object) error {
	typ := strOf(o, "type")
	if !supportedSources[typ] {
		return badf(
			"The request is invalid. Details: data source type '%s' is not supported; "+
				"supported types are azureblob and cosmosdb.",
			typ,
		)
	}

	cred := objOf(o["credentials"])
	if cred == nil || strOf(cred, "connectionString") == "" && o["identity"] == nil {
		return badf("The request is invalid. Details: credentials: connectionString is required.")
	}

	if strOf(objOf(o["container"]), "name") == "" {
		return badf("The request is invalid. Details: container: name is required.")
	}

	if _, ok := o["description"]; !ok {
		o["description"] = nil
	}

	return nil
}

func validateSkillset(o Object) error {
	skills := listOf(o, "skills")
	if len(skills) == 0 {
		return badf("The request is invalid. Details: skills: a skillset must have at least one skill.")
	}

	for i, raw := range skills {
		sk := objOf(raw)
		if !strings.HasPrefix(strOf(sk, "@odata.type"), "#Microsoft.Skills.") {
			return badf("The request is invalid. Details: skills[%d]: missing or invalid @odata.type.", i)
		}

		if len(listOf(sk, "inputs")) == 0 || len(listOf(sk, "outputs")) == 0 {
			if strOf(sk, "@odata.type") != "#Microsoft.Skills.Custom.WebApiSkill" {
				return badf("The request is invalid. Details: skills[%d]: inputs and outputs are required.", i)
			}
		}

		if _, ok := sk["context"]; !ok {
			sk["context"] = "/document"
		}
	}

	return nil
}

func (s *Service) putObject(kind, name string, o Object, ifMatch, ifNone string) (Object, bool, error) {
	if strOf(o, "name") == "" {
		o["name"] = name
	}

	if strOf(o, "name") != name {
		return nil, false, badf("The %s name in the request body does not match the one in the URI.", kindLabel(kind))
	}

	old, exists := s.Objects[kind][name]

	var etag any
	if exists {
		etag = old[etagKey]
	}

	if err := checkPrecondition(etag, ifMatch, ifNone); err != nil {
		return nil, false, err
	}

	o = cloneMap(o)
	delete(o, fieldContext)

	if kind == kindDataSources && exists {
		keepSecret(o, old)
	}

	if err := s.validateObject(kind, o); err != nil {
		return nil, false, err
	}

	o[etagKey] = s.nextETag()
	s.Objects[kind][name] = o

	return redact(kind, o), !exists, nil
}

// keepSecret retains the stored connection string when an update echoes back the redacted value.
func keepSecret(o, old Object) {
	cred := objOf(o["credentials"])
	if cred != nil && strOf(cred, "connectionString") == redacted {
		cred["connectionString"] = strOf(objOf(old["credentials"]), "connectionString")
	}
}

func (s *Service) deleteObject(kind, name, ifMatch string) error {
	o, err := s.object(kind, name)
	if err != nil {
		return err
	}

	if err = checkPrecondition(o[etagKey], ifMatch, ""); err != nil {
		return err
	}

	if kind == kindIndexers {
		delete(s.Runtime, name)
	}

	delete(s.Objects[kind], name)

	return nil
}

func (s *Service) listObjects(kind string) []Object {
	names := make([]string, 0, len(s.Objects[kind]))
	for n := range s.Objects[kind] {
		names = append(names, n)
	}

	sort.Strings(names)

	out := make([]Object, 0, len(names))
	for _, n := range names {
		out = append(out, redact(kind, s.Objects[kind][n]))
	}

	return out
}
