package serverlessrepo

// Tag represents a CloudFormation tag passed while deploying an application.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func cloneTags(tags []Tag) []Tag {
	if tags == nil {
		return nil
	}

	return append([]Tag(nil), tags...)
}
