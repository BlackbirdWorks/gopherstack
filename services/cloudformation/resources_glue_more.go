package cloudformation

import (
	"errors"
	"fmt"

	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
)

const (
	resTypeGlueClassifier            = "AWS::Glue::Classifier"
	resTypeGlueRegistry              = "AWS::Glue::Registry"
	resTypeGlueSchema                = "AWS::Glue::Schema"
	resTypeGlueSecurityConfiguration = "AWS::Glue::SecurityConfiguration"
	resTypeGlueDevEndpoint           = "AWS::Glue::DevEndpoint"
)

func (rc *ResourceCreator) createGlueMoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeGlueClassifier:
		id, err := rc.createGlueClassifier(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeGlueRegistry:
		id, err := rc.createGlueRegistry(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeGlueSchema:
		id, err := rc.createGlueSchema(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeGlueSecurityConfiguration:
		id, err := rc.createGlueSecurityConfiguration(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeGlueDevEndpoint:
		id, err := rc.createGlueDevEndpoint(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteGlueMoreResource handles deletion for Classifier/Registry/
// SecurityConfiguration/DevEndpoint. Schema deletes via
// deletePropsBasedResource since it needs the owning Registry name, a
// sibling property not embedded in the Ref value.
func (rc *ResourceCreator) deleteGlueMoreResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeGlueClassifier:
		return true, rc.deleteGlueClassifier(physicalID)
	case resTypeGlueRegistry:
		return true, rc.deleteGlueRegistry(physicalID)
	case resTypeGlueSecurityConfiguration:
		return true, rc.deleteGlueSecurityConfiguration(physicalID)
	case resTypeGlueDevEndpoint:
		return true, rc.deleteGlueDevEndpoint(physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::Glue::Classifier ----
// Ref returns the classifier name; Fn::GetAtt Name is stashed.

func (rc *ResourceCreator) createGlueClassifier(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	c := gluebackend.Classifier{}

	switch {
	case propHasKey(props, "CsvClassifier"):
		m, _ := props["CsvClassifier"].(map[string]any)
		name := classifierName(m, logicalID, params, physicalIDs)
		c.CsvClassifier = &gluebackend.CsvClassifier{
			Name:        name,
			Delimiter:   strProp(m, "Delimiter", params, physicalIDs),
			QuoteSymbol: strProp(m, "QuoteSymbol", params, physicalIDs),
			Header:      strSliceProp(m["Header"], params, physicalIDs),
		}
	case propHasKey(props, "GrokClassifier"):
		m, _ := props["GrokClassifier"].(map[string]any)
		name := classifierName(m, logicalID, params, physicalIDs)
		c.GrokClassifier = &gluebackend.GrokClassifier{
			Name:           name,
			Classification: strProp(m, "Classification", params, physicalIDs),
			GrokPattern:    strProp(m, "GrokPattern", params, physicalIDs),
			CustomPatterns: strProp(m, "CustomPatterns", params, physicalIDs),
		}
	case propHasKey(props, "JsonClassifier"):
		m, _ := props["JsonClassifier"].(map[string]any)
		name := classifierName(m, logicalID, params, physicalIDs)
		c.JSONClassifier = &gluebackend.JSONClassifier{
			Name:     name,
			JSONPath: strProp(m, "JsonPath", params, physicalIDs),
		}
	case propHasKey(props, "XMLClassifier"):
		m, _ := props["XMLClassifier"].(map[string]any)
		name := classifierName(m, logicalID, params, physicalIDs)
		c.XMLClassifier = &gluebackend.XMLClassifier{
			Name:           name,
			Classification: strProp(m, "Classification", params, physicalIDs),
			RowTag:         strProp(m, "RowTag", params, physicalIDs),
		}
	default:
		return "", fmt.Errorf(
			"%w: one of CsvClassifier/GrokClassifier/JsonClassifier/XMLClassifier is required",
			gluebackend.ErrValidation,
		)
	}

	if err := rc.backends.Glue.Backend.CreateClassifier(c); err != nil {
		return "", fmt.Errorf("create Glue classifier: %w", err)
	}

	name := glueClassifierName(&c)
	physicalIDs[logicalID+"/Name"] = name

	return name, nil
}

func propHasKey(props map[string]any, key string) bool {
	_, ok := props[key]

	return ok
}

// classifierName reads the Name property from a classifier sub-object,
// defaulting to the logical ID when absent.
func classifierName(m map[string]any, logicalID string, params, physicalIDs map[string]string) string {
	name := strProp(m, "Name", params, physicalIDs)
	if name == "" {
		return logicalID
	}

	return name
}

// glueClassifierName returns the name of whichever sub-classifier is set.
func glueClassifierName(c *gluebackend.Classifier) string {
	switch {
	case c.CsvClassifier != nil:
		return c.CsvClassifier.Name
	case c.GrokClassifier != nil:
		return c.GrokClassifier.Name
	case c.JSONClassifier != nil:
		return c.JSONClassifier.Name
	case c.XMLClassifier != nil:
		return c.XMLClassifier.Name
	default:
		return ""
	}
}

func (rc *ResourceCreator) deleteGlueClassifier(name string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	err := rc.backends.Glue.Backend.DeleteClassifier(name)
	if errors.Is(err, gluebackend.ErrNotFound) {
		return nil
	}

	return err
}

// ---- AWS::Glue::Registry ----
// Ref is undocumented: the CFN reference's stated Ref description ("a
// combination of VersionId|Key|Value") does not match this resource's own
// properties and appears to be a copy/paste error from another page, so the
// registry name (this backend's own primary key) is used instead.
// Fn::GetAtt Arn is stashed.

func (rc *ResourceCreator) createGlueRegistry(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	reg, err := rc.backends.Glue.Backend.CreateRegistry(
		name, strProp(props, "Description", params, physicalIDs), tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Glue registry %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = reg.ARN

	return reg.Name, nil
}

func (rc *ResourceCreator) deleteGlueRegistry(name string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	_, err := rc.backends.Glue.Backend.DeleteRegistry(name)
	if errors.Is(err, gluebackend.ErrNotFound) {
		return nil
	}

	return err
}

// ---- AWS::Glue::Schema ----
// Ref is undocumented (the CFN reference lists "Return values Ref" with no
// description text); the schema ARN, also the documented Fn::GetAtt Arn
// value, is used as the primary identifier. Delete is wired through
// deletePropsBasedResource since DeleteSchema needs the owning registry
// name, a sibling property not embedded in the ARN.

func (rc *ResourceCreator) createGlueSchema(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	registryName := ""
	if reg, ok := props["Registry"].(map[string]any); ok {
		registryName = strProp(reg, "Name", params, physicalIDs)
	}

	schema, ver, err := rc.backends.Glue.Backend.CreateSchema(
		registryName, name,
		strProp(props, "DataFormat", params, physicalIDs),
		strProp(props, "Compatibility", params, physicalIDs),
		strProp(props, "Description", params, physicalIDs),
		strProp(props, "SchemaDefinition", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Glue schema %s: %w", name, err)
	}

	if ver != nil {
		physicalIDs[logicalID+"/InitialSchemaVersionId"] = ver.SchemaVersionID
	}

	return schema.SchemaARN, nil
}

// deleteGlueSchema is wired via deletePropsBasedResource (resources.go)
// since it needs Registry.Name/Name from the resource's own properties.
func (rc *ResourceCreator) deleteGlueSchema(props map[string]any, stackPhysicalIDs map[string]string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	name := strProp(props, "Name", nil, stackPhysicalIDs)

	registryName := ""
	if reg, ok := props["Registry"].(map[string]any); ok {
		registryName = strProp(reg, "Name", nil, stackPhysicalIDs)
	}

	_, err := rc.backends.Glue.Backend.DeleteSchema(registryName, name)
	if errors.Is(err, gluebackend.ErrNotFound) {
		return nil
	}

	return err
}

// ---- AWS::Glue::SecurityConfiguration ----
// Ref returns the resource name (documented with no further elaboration).

func (rc *ResourceCreator) createGlueSecurityConfiguration(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	enc := glueEncryptionConfigurationProp(props["EncryptionConfiguration"], params, physicalIDs)

	sc, err := rc.backends.Glue.Backend.CreateSecurityConfiguration(name, enc)
	if err != nil {
		return "", fmt.Errorf("create Glue security configuration %s: %w", name, err)
	}

	return sc.Name, nil
}

// glueEncryptionConfigurationProp decodes props["EncryptionConfiguration"]
// (CloudWatchEncryption/JobBookmarksEncryption/S3Encryptions) into the Glue
// backend's EncryptionConfiguration.
func glueEncryptionConfigurationProp(
	v any, params, physicalIDs map[string]string,
) gluebackend.EncryptionConfiguration {
	em, ok := v.(map[string]any)
	if !ok {
		return gluebackend.EncryptionConfiguration{}
	}

	enc := gluebackend.EncryptionConfiguration{}

	if cw, cwOK := em["CloudWatchEncryption"].(map[string]any); cwOK {
		enc.CloudWatchEncryption = &gluebackend.CloudWatchEncryption{
			CloudWatchEncryptionMode: strProp(cw, "CloudWatchEncryptionMode", params, physicalIDs),
			KMSKeyARN:                strProp(cw, "KmsKeyArn", params, physicalIDs),
		}
	}

	if jb, jbOK := em["JobBookmarksEncryption"].(map[string]any); jbOK {
		enc.JobBookmarksEncryption = &gluebackend.JobBookmarksEncryption{
			JobBookmarksEncryptionMode: strProp(jb, "JobBookmarksEncryptionMode", params, physicalIDs),
			KMSKeyARN:                  strProp(jb, "KmsKeyArn", params, physicalIDs),
		}
	}

	s3List, _ := em["S3Encryptions"].([]any)
	for _, item := range s3List {
		sm, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		enc.S3Encryption = append(enc.S3Encryption, gluebackend.S3EncryptionEntry{
			S3EncryptionMode: strProp(sm, "S3EncryptionMode", params, physicalIDs),
			KMSKeyARN:        strProp(sm, "KmsKeyArn", params, physicalIDs),
		})
	}

	return enc
}

func (rc *ResourceCreator) deleteGlueSecurityConfiguration(name string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	err := rc.backends.Glue.Backend.DeleteSecurityConfiguration(name)
	if errors.Is(err, gluebackend.ErrNotFound) {
		return nil
	}

	return err
}

// ---- AWS::Glue::DevEndpoint ----
// Ref returns the endpoint name.

func (rc *ResourceCreator) createGlueDevEndpoint(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "EndpointName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	input := gluebackend.DevEndpointInput{
		SubnetID:              strProp(props, "SubnetId", params, physicalIDs),
		PublicKey:             strProp(props, "PublicKey", params, physicalIDs),
		WorkerType:            strProp(props, "WorkerType", params, physicalIDs),
		GlueVersion:           strProp(props, "GlueVersion", params, physicalIDs),
		ExtraPythonLibsS3Path: strProp(props, "ExtraPythonLibsS3Path", params, physicalIDs),
		ExtraJarsS3Path:       strProp(props, "ExtraJarsS3Path", params, physicalIDs),
		SecurityConfiguration: strProp(props, "SecurityConfiguration", params, physicalIDs),
		SecurityGroupIDs:      strSliceProp(props["SecurityGroupIds"], params, physicalIDs),
		PublicKeys:            strSliceProp(props["PublicKeys"], params, physicalIDs),
		NumberOfNodes:         intProp(props, "NumberOfNodes"),
		NumberOfWorkers:       intProp(props, "NumberOfWorkers"),
	}

	dep, err := rc.backends.Glue.Backend.CreateDevEndpoint(
		name, input, strProp(props, "RoleArn", params, physicalIDs), tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Glue dev endpoint %s: %w", name, err)
	}

	return dep.EndpointName, nil
}

func (rc *ResourceCreator) deleteGlueDevEndpoint(name string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	err := rc.backends.Glue.Backend.DeleteDevEndpoint(name)
	if errors.Is(err, gluebackend.ErrNotFound) {
		return nil
	}

	return err
}
