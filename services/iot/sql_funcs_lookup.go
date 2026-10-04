package iot

import (
	"encoding/base64"
	"sort"
	"strings"
)

const (
	secretString    = "SecretString"
	secretBinary    = "SecretBinary"
	getDynamoPlain  = 4
	getDynamoRanged = 6
	minSecretArgs   = 3
	minShadowArgs   = 2
	sortNamePos     = 3
	sortValuePos    = 4
	secretKeyPos    = 2
	roleArgPos      = 2
)

// lookupFuncs are the SQL functions that call other services in-process.
func lookupFuncs() map[string]funcDef {
	return map[string]funcDef{
		"get_dynamodb":      {since2016: true, impl: getDynamoDBFunc},
		"get_secret":        {impl: getSecretFunc},
		"get_thing_shadow":  {since2016: true, impl: getThingShadowFunc},
		"get_registry_data": {since2016: true, impl: getRegistryDataFunc},
		"aws_lambda":        {impl: awsLambdaFunc},
	}
}

// lookupEnv returns the message and its hook, or false when the function has nothing to call.
func lookupEnv(c *sqlCtx) (*ruleMessage, *ruleHook, bool) {
	return c.msg, c.msg.hook, c.msg.hook != nil
}

func failed(m *ruleMessage, fn string) any {
	m.fail(errSQLFunction, fn)

	return sqlUndefined{}
}

func keyAttr(v any) (map[string]any, bool) {
	switch t := v.(type) {
	case string:
		return map[string]any{"S": t}, true
	case int64, float64:
		return map[string]any{"N": jsonString(t)}, true
	}

	return nil, false
}

func getDynamoDBFunc(c *sqlCtx, args []any) any {
	m, h, ok := lookupEnv(c)
	if !ok {
		return sqlUndefined{}
	}

	if (len(args) != getDynamoPlain && len(args) != getDynamoRanged) || !m.callOnce("get_dynamodb") {
		return failed(m, "get_dynamodb")
	}

	table, _ := strArg(args, 0)
	hashName, _ := strArg(args, 1)
	role, _ := strArg(args, len(args)-1)

	hash, hok := keyAttr(args[2])
	key := map[string]any{hashName: hash}

	if len(args) == getDynamoRanged {
		rangeName, _ := strArg(args, sortNamePos)
		rng, rok := keyAttr(args[sortValuePos])
		hok = hok && rok
		key[rangeName] = rng
	}

	if !hok {
		return failed(m, "get_dynamodb")
	}

	item, err := h.readDynamoItem(m, role, table, key)
	if err != nil {
		return failed(m, "get_dynamodb")
	}

	if item == nil {
		return sqlUndefined{}
	}

	return attributeToSQL(map[string]any{"M": item}, c.v2016)
}

// attributeToSQL converts a DynamoDB wire value into an addressable SQL value.
func attributeToSQL(av map[string]any, v2016 bool) any {
	for typ, val := range av {
		switch typ {
		case "S":
			s, _ := val.(string)

			return s
		case "N":
			s, _ := val.(string)

			return sqlNumber(s, v2016)
		case "BOOL":
			b, _ := val.(bool)

			return b
		case "B":
			s, _ := val.(string)

			return s
		case "M":
			return mapToSQL(val, v2016)
		case "L", "SS", "NS", "BS":
			return listToSQL(typ, val, v2016)
		}
	}

	return nil
}

func mapToSQL(val any, v2016 bool) any {
	m, _ := val.(map[string]any)
	keys := make([]string, 0, len(m))

	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	obj := newSQLObject()

	for _, k := range keys {
		if av, ok := m[k].(map[string]any); ok {
			obj.set(k, attributeToSQL(av, v2016))
		}
	}

	return obj
}

func listToSQL(typ string, val any, v2016 bool) any {
	items, _ := val.([]any)
	out := make([]any, 0, len(items))

	for _, it := range items {
		switch e := it.(type) {
		case map[string]any:
			out = append(out, attributeToSQL(e, v2016))
		case string:
			if typ == "NS" {
				out = append(out, sqlNumber(e, v2016))
			} else {
				out = append(out, e)
			}
		}
	}

	return out
}

// getSecretFunc implements get_secret(secretId, secretType, [key,] roleArn).
func getSecretFunc(c *sqlCtx, args []any) any {
	m, h, ok := lookupEnv(c)
	if !ok {
		return sqlUndefined{}
	}

	if len(args) < minSecretArgs || len(args) > minSecretArgs+1 {
		return failed(m, "get_secret")
	}

	id, _ := strArg(args, 0)
	typ, _ := strArg(args, 1)
	role, _ := strArg(args, len(args)-1)
	key := ""

	if len(args) == minSecretArgs+1 {
		key, _ = strArg(args, secretKeyPos)
	}

	if typ != secretString && typ != secretBinary {
		return failed(m, "get_secret")
	}

	v, err := h.readSecret(m, role, id)
	if err != nil {
		return failed(m, "get_secret")
	}

	res, rok := secretResult(v, typ, key, c.v2016)
	if !rok {
		return failed(m, "get_secret")
	}

	return res
}

// secretResult applies the documented SecretString/SecretBinary and key rules.
func secretResult(v SecretValue, typ, key string, v2016 bool) (any, bool) {
	if typ == secretBinary {
		if key != "" {
			return nil, false
		}

		return base64.StdEncoding.EncodeToString(v.Binary), true
	}

	parsed, err := decodeJSONValue([]byte(v.String), v2016)
	obj, isObj := parsed.(*sqlObject)

	switch {
	case key == "" && err == nil && isObj:
		return obj, true
	case key == "":
		return v.String, true
	case err != nil || !isObj:
		return nil, false
	}

	return memberOf(obj, key), true
}

// getThingShadowFunc implements get_thing_shadow(thingName, [shadowName,] roleArn).
func getThingShadowFunc(c *sqlCtx, args []any) any {
	m, h, ok := lookupEnv(c)
	if !ok {
		return sqlUndefined{}
	}

	if len(args) < minShadowArgs || len(args) > minShadowArgs+1 {
		return failed(m, "get_thing_shadow")
	}

	thing, _ := strArg(args, 0)
	role, _ := strArg(args, len(args)-1)
	shadow := ""

	if len(args) == minShadowArgs+1 {
		shadow, _ = strArg(args, 1)
	}

	doc, err := h.readShadow(m, role, thing, shadow)
	if err != nil {
		return failed(m, "get_thing_shadow")
	}

	return jsonOrFail(m, "get_thing_shadow", doc, c.v2016)
}

func jsonOrFail(m *ruleMessage, fn string, doc []byte, v2016 bool) any {
	v, err := decodeJSONValue(doc, v2016)
	if err != nil {
		return failed(m, fn)
	}

	return v
}

// getRegistryDataFunc implements get_registry_data(registryAPI, thingName, roleArn).
func getRegistryDataFunc(c *sqlCtx, args []any) any {
	m, h, ok := lookupEnv(c)
	if !ok {
		return sqlUndefined{}
	}

	api, _ := strArg(args, 0)
	thing, _ := strArg(args, 1)
	role, _ := strArg(args, roleArgPos)

	if len(args) != minSecretArgs || !m.callOnce("get_registry_data") ||
		(api != "DescribeThing" && api != "ListThingGroupsForThing") {
		return failed(m, "get_registry_data")
	}

	doc, err := h.registryData(m, role, api, thing)
	if err != nil {
		return failed(m, "get_registry_data")
	}

	return jsonOrFail(m, "get_registry_data", doc, c.v2016)
}

// awsLambdaFunc implements aws_lambda(functionArn, inputJson).
func awsLambdaFunc(c *sqlCtx, args []any) any {
	m, h, ok := lookupEnv(c)
	if !ok {
		return sqlUndefined{}
	}

	fnARN, sok := strArg(args, 0)
	input := arg(args, 1)

	if len(args) != minShadowArgs || !sok || !strings.HasPrefix(fnARN, "arn:") || isUndef(input) {
		return failed(m, "aws_lambda")
	}

	out, err := h.callLambda(m, fnARN, []byte(jsonString(input)))
	if err != nil {
		return failed(m, "aws_lambda")
	}

	return jsonOrFail(m, "aws_lambda", out, c.v2016)
}
