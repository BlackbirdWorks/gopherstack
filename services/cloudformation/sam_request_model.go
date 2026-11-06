package cloudformation

import "strings"

const (
	samKeyRequestValidators = "x-amazon-apigateway-request-validators"
	samKeyRequestValidator  = "x-amazon-apigateway-request-validator"
)

// samRequestModel is an Api event's RequestModel.
type samRequestModel struct {
	required       any
	model          string
	validateBody   bool
	validateParams bool
	hasValidator   bool
}

func (t *samTranslator) parseRequestModel(
	fnID, name string,
	api *samAPI,
	method, path string,
	v any,
) (*samRequestModel, error) {
	if v == nil {
		return &samRequestModel{}, nil
	}
	rm := asMap(v)
	if rm == nil {
		return nil, samErr(fnID, "Event [%s] RequestModel must be a map.", name)
	}
	if err := rejectUnknown(
		fnID+name,
		rm,
		keySet("Model", "Required", "ValidateBody", "ValidateParameters"),
	); err != nil {
		return nil, err
	}
	prefix := "Unable to set RequestModel [" + asString(
		rm["Model"],
	) + "] on API method [" + method + "] for path [" + path + "] "
	out := &samRequestModel{required: rm["Required"]}
	if rm["Model"] == nil {
		return out, nil
	}
	models := api.props[samKModels]
	switch {
	case models == nil || len(asMap(models)) == 0 && !isIntrinsicValue(models):
		return nil, samErr(fnID, "%sbecause the related API does not define any Models.", prefix)
	case !isIntrinsicValue(models) && asMap(models) == nil:
		return nil, samErr(fnID, "%sbecause the related API Models defined is of invalid type.", prefix)
	}
	model, isStr := rm["Model"].(string)
	if !isStr {
		return nil, samErr(fnID, "%sbecause the related API does not contain valid Models.", prefix)
	}
	if asMap(models)[model] == nil {
		return nil, samErr(fnID, "%sbecause it wasn't defined in the API's Models.", prefix)
	}
	out.model = model
	if err := out.parseValidator(fnID, prefix, rm); err != nil {
		return nil, err
	}

	return out, nil
}

func (m *samRequestModel) parseValidator(fnID, prefix string, rm map[string]any) error {
	vb, vp := rm["ValidateBody"], rm["ValidateParameters"]
	if vb == nil && vp == nil {
		return nil
	}
	b, bOK := boolOrDefault(vb)
	p, pOK := boolOrDefault(vp)
	if !bOK || !pOK {
		return samErr(fnID, "%sValidateBody and ValidateParameters must be a boolean type, strings or "+
			"intrinsics are not supported.", prefix)
	}
	m.hasValidator, m.validateBody, m.validateParams = true, b, p

	return nil
}

func boolOrDefault(v any) (bool, bool) {
	if v == nil {
		return false, true
	}
	b, ok := v.(bool)

	return b, ok
}

func (m *samRequestModel) validatorName() string {
	switch {
	case m.validateBody && m.validateParams:
		return "body-and-params"
	case m.validateBody:
		return "body-only"
	case m.validateParams:
		return "params-only"
	}

	return "no-validation"
}

// apply adds the model (and validator) to the operation of a Swagger 2 or OpenAPI 3 document.
func (m *samRequestModel) apply(doc, op map[string]any) {
	if m == nil || m.model == "" {
		return
	}
	name := strings.ToLower(m.model)
	if _, isV3 := doc["openapi"]; isV3 {
		body := map[string]any{"content": map[string]any{samJSONMime: map[string]any{
			samKeySchema: map[string]any{samKeyDollarRef: "#/components/schemas/" + name},
		}}}
		if m.required != nil {
			body["required"] = m.required
		}
		op["requestBody"] = body
	} else {
		param := map[string]any{"in": "body", samKeyNameLower: name,
			samKeySchema: map[string]any{samKeyDollarRef: "#/definitions/" + name}}
		if m.required != nil {
			param["required"] = m.required
		}
		op["parameters"] = append(asList(op["parameters"]), param)
	}
	if !m.hasValidator {
		return
	}
	vname := m.validatorName()
	validators := asMap(doc[samKeyRequestValidators])
	if validators == nil {
		validators = map[string]any{}
		doc[samKeyRequestValidators] = validators
	}
	if _, has := validators[vname]; !has {
		validators[vname] = map[string]any{
			"validateRequestBody":       m.validateBody,
			"validateRequestParameters": m.validateParams,
		}
	}
	op[samKeyRequestValidator] = vname
}
