package ssm

// Tag represents a key/value tag pair.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// AddTagsToResourceInput is the request payload for AddTagsToResource.
type AddTagsToResourceInput struct {
	ResourceType string `json:"ResourceType"`
	ResourceID   string `json:"ResourceId"`
	Tags         []Tag  `json:"Tags"`
}

// RemoveTagsFromResourceInput is the request payload for RemoveTagsFromResource.
type RemoveTagsFromResourceInput struct {
	ResourceType string   `json:"ResourceType"`
	ResourceID   string   `json:"ResourceId"`
	TagKeys      []string `json:"TagKeys"`
}

// ListTagsForResourceInput is the request payload for ListTagsForResource.
type ListTagsForResourceInput struct {
	ResourceType string `json:"ResourceType"`
	ResourceID   string `json:"ResourceId"`
}

// ListTagsForResourceOutput is the response payload for ListTagsForResource.
type ListTagsForResourceOutput struct {
	TagList []Tag `json:"TagList"`
}
