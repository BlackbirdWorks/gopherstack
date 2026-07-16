package cognitoidp

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn,omitempty"`
}

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"Tags,omitempty"`
}

type tagResourceInput struct {
	Tags        map[string]string `json:"Tags,omitempty"`
	ResourceArn string            `json:"ResourceArn,omitempty"`
}

type tagResourceOutput struct{}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn,omitempty"`
	TagKeys     []string `json:"TagKeys,omitempty"`
}

type untagResourceOutput struct{}
