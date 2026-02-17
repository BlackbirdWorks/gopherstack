package expr_test

import (
	"Gopherstack/dynamodb/expr"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAWSExpressions_Functions tests all AWS DynamoDB expression functions
// Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
func TestAWSExpressions_Functions(t *testing.T) {
	t.Parallel()

	// Test item with various attribute types for comprehensive testing
	item := map[string]any{
		"name":    map[string]any{"S": "John Doe"},
		"age":     map[string]any{"N": "30"},
		"email":   map[string]any{"S": "john@example.com"},
		"active":  map[string]any{"BOOL": true},
		"tags":    map[string]any{"SS": []string{"developer", "manager"}},
		"numbers": map[string]any{"NS": []string{"1", "2", "3"}},
		"data":    map[string]any{"B": []byte{0x01, 0x02, 0x03}},
		"profile": map[string]any{
			"M": map[string]any{
				"city":    map[string]any{"S": "New York"},
				"zipcode": map[string]any{"S": "10001"},
			},
		},
		"items": map[string]any{
			"L": []any{
				map[string]any{"S": "item1"},
				map[string]any{"S": "item2"},
			},
		},
		"nullField": map[string]any{"NULL": true},
	}

	attrValues := map[string]any{
		":name":   map[string]any{"S": "John"},
		":email":  map[string]any{"S": "@example.com"},
		":tag":    map[string]any{"S": "developer"},
		":minAge": map[string]any{"N": "18"},
		":maxAge": map[string]any{"N": "65"},
		":true":   map[string]any{"BOOL": true},
		":three":  map[string]any{"N": "3"},
		":typeS":  map[string]any{"S": "S"},
		":typeN":  map[string]any{"S": "N"},
		":typeM":  map[string]any{"S": "M"},
		":typeL":  map[string]any{"S": "L"},
	}

	tests := []struct {
		name      string
		exprStr   string
		wantMatch bool
		wantErr   bool
	}{
		// attribute_exists() - Returns true if the item contains the attribute specified by path
		{
			name:      "attribute_exists - existing attribute",
			exprStr:   "attribute_exists(name)",
			wantMatch: true,
		},
		{
			name:      "attribute_exists - non-existing attribute",
			exprStr:   "attribute_exists(missing)",
			wantMatch: false,
		},
		{
			name:      "attribute_exists - nested path",
			exprStr:   "attribute_exists(profile.city)",
			wantMatch: true,
		},

		// attribute_not_exists() - Returns true if the attribute specified by path does not exist
		{
			name:      "attribute_not_exists - missing attribute",
			exprStr:   "attribute_not_exists(missing)",
			wantMatch: true,
		},
		{
			name:      "attribute_not_exists - existing attribute",
			exprStr:   "attribute_not_exists(name)",
			wantMatch: false,
		},

		// attribute_type() - Returns true if the attribute at the specified path is of a particular data type
		{
			name:      "attribute_type - string type true",
			exprStr:   "attribute_type(name, :typeS)",
			wantMatch: true,
		},
		{
			name:      "attribute_type - string type false",
			exprStr:   "attribute_type(age, :typeS)",
			wantMatch: false,
		},
		{
			name:      "attribute_type - number type",
			exprStr:   "attribute_type(age, :typeN)",
			wantMatch: true,
		},
		{
			name:      "attribute_type - map type",
			exprStr:   "attribute_type(profile, :typeM)",
			wantMatch: true,
		},
		{
			name:      "attribute_type - list type",
			exprStr:   "attribute_type(items, :typeL)",
			wantMatch: true,
		},
		{
			name:      "attribute_type - non-existent attribute",
			exprStr:   "attribute_type(missing, :typeS)",
			wantMatch: false,
		},

		// begins_with() - Returns true if the attribute specified by path begins with a particular substring
		{
			name:      "begins_with - true case",
			exprStr:   "begins_with(name, :name)",
			wantMatch: true,
		},
		{
			name:      "begins_with - false case",
			exprStr:   "begins_with(name, :email)",
			wantMatch: false,
		},
		{
			name:      "begins_with - nested path",
			exprStr:   "begins_with(profile.zipcode, :zipPrefix)",
			wantMatch: true,
		},

		// contains() - Returns true if the attribute specified by path contains a particular value
		{
			name:      "contains - string contains substring",
			exprStr:   "contains(email, :email)",
			wantMatch: true,
		},
		{
			name:      "contains - string set contains value",
			exprStr:   "contains(tags, :tag)",
			wantMatch: true,
		},

		// size() - Returns the size of the attribute specified by path
		{
			name:      "size - string length",
			exprStr:   "size(name) > :three",
			wantMatch: true,
		},
		{
			name:      "size - set size",
			exprStr:   "size(tags) = :two",
			wantMatch: true,
		},
		{
			name:      "size - list size",
			exprStr:   "size(items) = :two",
			wantMatch: true,
		},

		// Comparison operators
		{
			name:      "= operator",
			exprStr:   "active = :true",
			wantMatch: true,
		},
		{
			name:      "<> operator - not equal",
			exprStr:   "age <> :minAge",
			wantMatch: true,
		},
		{
			name:      "< operator",
			exprStr:   "age < :maxAge",
			wantMatch: true,
		},
		{
			name:      "<= operator",
			exprStr:   "age <= :minAge",
			wantMatch: false,
		},
		{
			name:      "> operator",
			exprStr:   "age > :minAge",
			wantMatch: true,
		},
		{
			name:      ">= operator",
			exprStr:   "age >= :minAge",
			wantMatch: true,
		},

		// BETWEEN operator
		{
			name:      "BETWEEN - in range",
			exprStr:   "age BETWEEN :minAge AND :maxAge",
			wantMatch: true,
		},
		{
			name:      "BETWEEN - out of range",
			exprStr:   "age BETWEEN :one AND :ten",
			wantMatch: false,
		},

		// IN operator
		{
			name:      "IN - value in list",
			exprStr:   "age IN (:minAge, :maxAge, :thirty)",
			wantMatch: true,
		},
		{
			name:      "IN - value not in list",
			exprStr:   "age IN (:minAge, :maxAge)",
			wantMatch: false,
		},

		// Logical operators
		{
			name:      "AND operator - both true",
			exprStr:   "age > :minAge AND active = :true",
			wantMatch: true,
		},
		{
			name:      "AND operator - one false",
			exprStr:   "age < :minAge AND active = :true",
			wantMatch: false,
		},
		{
			name:      "OR operator - one true",
			exprStr:   "age < :minAge OR active = :true",
			wantMatch: true,
		},
		{
			name:      "OR operator - both false",
			exprStr:   "age < :minAge OR active <> :true",
			wantMatch: false,
		},
		{
			name:      "NOT operator - negate true",
			exprStr:   "NOT (age < :minAge)",
			wantMatch: true,
		},
		{
			name:      "NOT operator - negate false",
			exprStr:   "NOT (age > :minAge)",
			wantMatch: false,
		},

		// Parentheses for precedence
		{
			name:      "Parentheses - change precedence",
			exprStr:   "(age > :minAge OR active <> :true) AND name = :fullName",
			wantMatch: true, // (30 > 18 OR true <> true) AND "John Doe" = "John Doe" → (true OR false) AND true → true
		},

		// Complex nested expressions
		{
			name:      "Complex - nested functions and operators",
			exprStr:   "size(tags) > :one AND (begins_with(name, :name) OR contains(email, :email))",
			wantMatch: true,
		},
	}

	// Add more attribute values for tests
	attrValues[":zipPrefix"] = map[string]any{"S": "100"}
	attrValues[":two"] = map[string]any{"N": "2"}
	attrValues[":one"] = map[string]any{"N": "1"}
	attrValues[":ten"] = map[string]any{"N": "10"}
	attrValues[":thirty"] = map[string]any{"N": "30"}
	attrValues[":fullName"] = map[string]any{"S": "John Doe"}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			l := expr.NewLexer(tc.exprStr)
			p := expr.NewParser(l)
			node, err := p.ParseCondition()
			if tc.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err, "Parse error for: %s", tc.exprStr)

			eval := &expr.Evaluator{
				Item:       item,
				AttrValues: attrValues,
			}
			result, err := eval.Evaluate(node)
			if tc.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err, "Eval error for: %s", tc.exprStr)
			assert.Equal(t, tc.wantMatch, result, "Match mismatch for: %s", tc.exprStr)
		})
	}
}

// TestAWSExpressions_UpdateExpressions tests all AWS DynamoDB UPDATE expression features
// Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
func TestAWSExpressions_UpdateExpressions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attrNames  map[string]string
		attrValues map[string]any
		item       map[string]any
		verify     func(t *testing.T, item map[string]any)
		name       string
		updateExpr string
		wantErr    bool
	}{
		// SET action - set attribute to a value
		{
			name:       "SET - simple value",
			updateExpr: "SET #name = :newName",
			attrNames:  map[string]string{"#name": "name"},
			attrValues: map[string]any{":newName": map[string]any{"S": "Jane Doe"}},
			item: map[string]any{
				"id":   map[string]any{"S": "123"},
				"name": map[string]any{"S": "John Doe"},
			},
			verify: func(t *testing.T, item map[string]any) {
				t.Helper()
				assert.Equal(t, "Jane Doe", item["name"].(map[string]any)["S"])
			},
		},

		// SET with path - nested attribute
		{
			name:       "SET - nested path",
			updateExpr: "SET profile.city = :city",
			attrValues: map[string]any{":city": map[string]any{"S": "Boston"}},
			item: map[string]any{
				"id": map[string]any{"S": "123"},
				"profile": map[string]any{
					"M": map[string]any{
						"city": map[string]any{"S": "New York"},
					},
				},
			},
			verify: func(t *testing.T, item map[string]any) {
				t.Helper()
				profile := item["profile"].(map[string]any)["M"].(map[string]any)
				assert.Equal(t, "Boston", profile["city"].(map[string]any)["S"])
			},
		},

		// SET with arithmetic - increment
		{
			name:       "SET - arithmetic increment",
			updateExpr: "SET #count = #count + :inc",
			attrNames:  map[string]string{"#count": "count"},
			attrValues: map[string]any{":inc": map[string]any{"N": "1"}},
			item: map[string]any{
				"id":    map[string]any{"S": "123"},
				"count": map[string]any{"N": "5"},
			},
			verify: func(t *testing.T, item map[string]any) {
				t.Helper()
				assert.Equal(t, "6", item["count"].(map[string]any)["N"])
			},
		},

		// SET with arithmetic - decrement
		{
			name:       "SET - arithmetic decrement",
			updateExpr: "SET price = price - :discount",
			attrValues: map[string]any{":discount": map[string]any{"N": "10"}},
			item: map[string]any{
				"id":    map[string]any{"S": "123"},
				"price": map[string]any{"N": "100"},
			},
			verify: func(t *testing.T, item map[string]any) {
				t.Helper()
				assert.Equal(t, "90", item["price"].(map[string]any)["N"])
			},
		},

		// SET with if_not_exists - initialize if missing
		{
			name:       "SET - if_not_exists create new",
			updateExpr: "SET version = if_not_exists(version, :zero) + :inc",
			attrValues: map[string]any{
				":zero": map[string]any{"N": "0"},
				":inc":  map[string]any{"N": "1"},
			},
			item: map[string]any{
				"id": map[string]any{"S": "123"},
			},
			verify: func(t *testing.T, item map[string]any) {
				t.Helper()
				assert.Equal(t, "1", item["version"].(map[string]any)["N"])
			},
		},

		// SET with if_not_exists - use existing value
		{
			name:       "SET - if_not_exists keep existing",
			updateExpr: "SET version = if_not_exists(version, :zero) + :inc",
			attrValues: map[string]any{
				":zero": map[string]any{"N": "0"},
				":inc":  map[string]any{"N": "1"},
			},
			item: map[string]any{
				"id":      map[string]any{"S": "123"},
				"version": map[string]any{"N": "5"},
			},
			verify: func(t *testing.T, item map[string]any) {
				t.Helper()
				assert.Equal(t, "6", item["version"].(map[string]any)["N"])
			},
		},

		// list_append() - Appends elements from one list to another
		{
			name:       "SET - list_append",
			updateExpr: "SET tags = list_append(tags, :newTags)",
			attrValues: map[string]any{
				":newTags": map[string]any{
					"L": []any{
						map[string]any{"S": "tag3"},
					},
				},
			},
			item: map[string]any{
				"id": map[string]any{"S": "123"},
				"tags": map[string]any{
					"L": []any{
						map[string]any{"S": "tag1"},
						map[string]any{"S": "tag2"},
					},
				},
			},
			verify: func(t *testing.T, item map[string]any) {
				t.Helper()
				tags := item["tags"].(map[string]any)["L"].([]any)
				assert.Len(t, tags, 3)
			},
		},

		// Multiple SET actions
		{
			name:       "SET - multiple attributes",
			updateExpr: "SET #name = :name, age = :age, active = :true",
			attrNames:  map[string]string{"#name": "name"},
			attrValues: map[string]any{
				":name": map[string]any{"S": "Updated Name"},
				":age":  map[string]any{"N": "35"},
				":true": map[string]any{"BOOL": true},
			},
			item: map[string]any{
				"id":   map[string]any{"S": "123"},
				"name": map[string]any{"S": "Old Name"},
				"age":  map[string]any{"N": "30"},
			},
			verify: func(t *testing.T, item map[string]any) {
				t.Helper()
				assert.Equal(t, "Updated Name", item["name"].(map[string]any)["S"])
				assert.Equal(t, "35", item["age"].(map[string]any)["N"])
				assert.Equal(t, true, item["active"].(map[string]any)["BOOL"])
			},
		},

		// REMOVE action
		{
			name:       "REMOVE - single attribute",
			updateExpr: "REMOVE #deprecated",
			attrNames:  map[string]string{"#deprecated": "oldField"},
			item: map[string]any{
				"id":       map[string]any{"S": "123"},
				"oldField": map[string]any{"S": "remove me"},
				"keepMe":   map[string]any{"S": "keep me"},
			},
			verify: func(t *testing.T, item map[string]any) {
				t.Helper()
				assert.NotContains(t, item, "oldField")
				assert.Contains(t, item, "keepMe")
			},
		},

		// REMOVE multiple attributes
		{
			name:       "REMOVE - multiple attributes",
			updateExpr: "REMOVE field1, field2",
			item: map[string]any{
				"id":     map[string]any{"S": "123"},
				"field1": map[string]any{"S": "value1"},
				"field2": map[string]any{"S": "value2"},
				"field3": map[string]any{"S": "value3"},
			},
			verify: func(t *testing.T, item map[string]any) {
				t.Helper()
				assert.NotContains(t, item, "field1")
				assert.NotContains(t, item, "field2")
				assert.Contains(t, item, "field3")
			},
		},

		// ADD action - number
		{
			name:       "ADD - increment number",
			updateExpr: "ADD #count :inc",
			attrNames:  map[string]string{"#count": "count"},
			attrValues: map[string]any{":inc": map[string]any{"N": "5"}},
			item: map[string]any{
				"id":    map[string]any{"S": "123"},
				"count": map[string]any{"N": "10"},
			},
			verify: func(t *testing.T, item map[string]any) {
				t.Helper()
				assert.Equal(t, "15", item["count"].(map[string]any)["N"])
			},
		},

		// ADD action - create if missing (number defaults to 0)
		{
			name:       "ADD - create number attribute",
			updateExpr: "ADD newCount :inc",
			attrValues: map[string]any{":inc": map[string]any{"N": "5"}},
			item: map[string]any{
				"id": map[string]any{"S": "123"},
			},
			verify: func(t *testing.T, item map[string]any) {
				t.Helper()
				assert.Equal(t, "5", item["newCount"].(map[string]any)["N"])
			},
		},

		// DELETE action - remove from set
		{
			name:       "DELETE - remove from string set",
			updateExpr: "DELETE tags :tag",
			attrValues: map[string]any{":tag": map[string]any{"SS": []string{"tag2"}}},
			item: map[string]any{
				"id":   map[string]any{"S": "123"},
				"tags": map[string]any{"SS": []string{"tag1", "tag2", "tag3"}},
			},
			verify: func(t *testing.T, item map[string]any) {
				t.Helper()
				tags := item["tags"].(map[string]any)["SS"].([]string)
				assert.Len(t, tags, 2)
				assert.NotContains(t, tags, "tag2")
			},
		},

		// Combined actions
		{
			name:       "SET, REMOVE, ADD combined",
			updateExpr: "SET #name = :name REMOVE oldField ADD #count :inc",
			attrNames: map[string]string{
				"#name":  "name",
				"#count": "count",
			},
			attrValues: map[string]any{
				":name": map[string]any{"S": "New Name"},
				":inc":  map[string]any{"N": "1"},
			},
			item: map[string]any{
				"id":       map[string]any{"S": "123"},
				"name":     map[string]any{"S": "Old Name"},
				"oldField": map[string]any{"S": "remove"},
				"count":    map[string]any{"N": "5"},
			},
			verify: func(t *testing.T, item map[string]any) {
				t.Helper()
				assert.Equal(t, "New Name", item["name"].(map[string]any)["S"])
				assert.NotContains(t, item, "oldField")
				assert.Equal(t, "6", item["count"].(map[string]any)["N"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			l := expr.NewLexer(tc.updateExpr)
			p := expr.NewParser(l)
			updateNode, err := p.ParseUpdate()
			if tc.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err, "Parse error for: %s", tc.updateExpr)

			eval := &expr.Evaluator{
				Item:       tc.item,
				AttrNames:  tc.attrNames,
				AttrValues: tc.attrValues,
			}

			err = eval.ApplyUpdate(updateNode)
			if tc.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err, "Apply error for: %s", tc.updateExpr)

			if tc.verify != nil {
				tc.verify(t, tc.item)
			}
		})
	}
}

// TestAWSExpressions_ExpressionAttributeNames tests Expression Attribute Names
// Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html
func TestAWSExpressions_ExpressionAttributeNames(t *testing.T) {
	t.Parallel()

	item := map[string]any{
		"id":       map[string]any{"S": "123"},
		"name":     map[string]any{"S": "John"},
		"status":   map[string]any{"S": "active"}, // 'status' is a reserved word
		"data":     map[string]any{"S": "value"},  // 'data' is a reserved word
		"time":     map[string]any{"N": "100"},    // 'time' is a reserved word
		"year":     map[string]any{"N": "2024"},   // 'year' is a reserved word
		"comment":  map[string]any{"S": "test"},   // 'comment' is a reserved word
		"date":     map[string]any{"S": "2024-01-01"},
		"values":   map[string]any{"NS": []string{"1", "2", "3"}}, // 'values' is a reserved word
		"my-field": map[string]any{"S": "hyphenated"},             // contains special character
	}

	tests := []struct {
		attrNames  map[string]string
		attrValues map[string]any
		name       string
		expression string
		wantMatch  bool
		isUpdate   bool
	}{
		// Reserved words in condition expressions
		{
			name:       "Reserved word - status",
			expression: "#s = :val",
			attrNames:  map[string]string{"#s": "status"},
			attrValues: map[string]any{":val": map[string]any{"S": "active"}},
			wantMatch:  true,
		},
		{
			name:       "Reserved word - data",
			expression: "#d = :val",
			attrNames:  map[string]string{"#d": "data"},
			attrValues: map[string]any{":val": map[string]any{"S": "value"}},
			wantMatch:  true,
		},
		{
			name:       "Reserved word - time",
			expression: "#t > :min",
			attrNames:  map[string]string{"#t": "time"},
			attrValues: map[string]any{":min": map[string]any{"N": "50"}},
			wantMatch:  true,
		},
		{
			name:       "Reserved word - year",
			expression: "#y = :year",
			attrNames:  map[string]string{"#y": "year"},
			attrValues: map[string]any{":year": map[string]any{"N": "2024"}},
			wantMatch:  true,
		},
		{
			name:       "Special character attribute",
			expression: "#field = :val",
			attrNames:  map[string]string{"#field": "my-field"},
			attrValues: map[string]any{":val": map[string]any{"S": "hyphenated"}},
			wantMatch:  true,
		},

		// Multiple attribute names in one expression
		{
			name:       "Multiple reserved words",
			expression: "#s = :status AND #d = :data",
			attrNames: map[string]string{
				"#s": "status",
				"#d": "data",
			},
			attrValues: map[string]any{
				":status": map[string]any{"S": "active"},
				":data":   map[string]any{"S": "value"},
			},
			wantMatch: true,
		},

		// Attribute names with functions
		{
			name:       "Reserved word with function",
			expression: "attribute_exists(#c)",
			attrNames:  map[string]string{"#c": "comment"},
			wantMatch:  true,
		},
		{
			name:       "Reserved word with begins_with",
			expression: "begins_with(#d, :prefix)",
			attrNames:  map[string]string{"#d": "date"},
			attrValues: map[string]any{":prefix": map[string]any{"S": "2024"}},
			wantMatch:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			l := expr.NewLexer(tc.expression)
			p := expr.NewParser(l)
			node, err := p.ParseCondition()
			require.NoError(t, err, "Parse error for: %s", tc.expression)

			eval := &expr.Evaluator{
				Item:       item,
				AttrNames:  tc.attrNames,
				AttrValues: tc.attrValues,
			}

			result, err := eval.Evaluate(node)
			require.NoError(t, err, "Eval error for: %s", tc.expression)
			assert.Equal(t, tc.wantMatch, result, "Match mismatch for: %s", tc.expression)
		})
	}
}

// TestAWSExpressions_ProjectionExpressions tests Projection Expressions
// Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ProjectionExpressions.html
func TestAWSExpressions_ProjectionExpressions(t *testing.T) {
	t.Parallel()

	item := map[string]any{
		"id":    map[string]any{"S": "123"},
		"name":  map[string]any{"S": "John Doe"},
		"age":   map[string]any{"N": "30"},
		"email": map[string]any{"S": "john@example.com"},
		"profile": map[string]any{
			"M": map[string]any{
				"city":    map[string]any{"S": "New York"},
				"zipcode": map[string]any{"S": "10001"},
				"country": map[string]any{"S": "USA"},
			},
		},
		"tags": map[string]any{
			"SS": []string{"tag1", "tag2", "tag3"},
		},
		"history": map[string]any{
			"L": []any{
				map[string]any{"S": "event1"},
				map[string]any{"S": "event2"},
				map[string]any{"S": "event3"},
			},
		},
	}

	tests := []struct {
		name             string
		projectionExpr   string
		attrNames        map[string]string
		expectedAttrs    []string
		notExpectedAttrs []string
	}{
		{
			name:             "Single top-level attribute",
			projectionExpr:   "name",
			expectedAttrs:    []string{"name"},
			notExpectedAttrs: []string{"age", "email"},
		},
		{
			name:             "Multiple top-level attributes",
			projectionExpr:   "id, name, age",
			expectedAttrs:    []string{"id", "name", "age"},
			notExpectedAttrs: []string{"email", "profile"},
		},
		{
			name:             "Nested attribute - single level",
			projectionExpr:   "profile.city",
			expectedAttrs:    []string{"profile"},
			notExpectedAttrs: []string{"name", "age"},
		},
		{
			name:             "Multiple nested attributes",
			projectionExpr:   "profile.city, profile.zipcode",
			expectedAttrs:    []string{"profile"},
			notExpectedAttrs: []string{"name", "age"},
		},
		{
			name:             "Mix of top-level and nested",
			projectionExpr:   "id, name, profile.city",
			expectedAttrs:    []string{"id", "name", "profile"},
			notExpectedAttrs: []string{"age", "email"},
		},
		{
			name:             "List element by index",
			projectionExpr:   "history[0]",
			expectedAttrs:    []string{"history"},
			notExpectedAttrs: []string{"name", "age"},
		},
		{
			name:             "Reserved word with attribute name",
			projectionExpr:   "#n, #e",
			attrNames:        map[string]string{"#n": "name", "#e": "email"},
			expectedAttrs:    []string{"name", "email"},
			notExpectedAttrs: []string{"age", "profile"},
		},
		{
			name:             "All attributes mixed",
			projectionExpr:   "id, profile.city, tags, history[1]",
			expectedAttrs:    []string{"id", "profile", "tags", "history"},
			notExpectedAttrs: []string{"name", "age", "email"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			l := expr.NewLexer(tc.projectionExpr)
			p := expr.NewParser(l)
			projNode, err := p.ParseProjection()
			require.NoError(t, err, "Parse error for: %s", tc.projectionExpr)

			eval := &expr.Evaluator{
				Item:      item,
				AttrNames: tc.attrNames,
			}

			result := eval.ApplyProjection(projNode)
			require.NotNil(t, result)

			// Verify expected attributes are present
			for _, attr := range tc.expectedAttrs {
				assert.Contains(t, result, attr, "Expected attribute %s to be in projection result", attr)
			}

			// Verify unexpected attributes are not present
			for _, attr := range tc.notExpectedAttrs {
				assert.NotContains(t, result, attr, "Expected attribute %s NOT to be in projection result", attr)
			}
		})
	}
}
