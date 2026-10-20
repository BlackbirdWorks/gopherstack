package fixture

import "encoding/json"

type putRuleInput struct {
	Tags map[string]string `json:"Tags"`
	Name string            `json:"Name"`
}

func decode(body []byte) (*putRuleInput, error) {
	var in putRuleInput

	return &in, json.Unmarshal(body, &in)
}
