package cloudformation_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

// ---- Macro ----

func TestResourceCreator_Macro_Registration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		logicalID   string
		props       map[string]any
		wantMacName string
	}{
		{
			name:      "explicit Name property",
			logicalID: "MyMacro",
			props: map[string]any{
				"Name":         "MyCustomMacro",
				"FunctionName": "arn:aws:lambda:us-east-1:000000000000:function:macro-fn",
			},
			wantMacName: "MyCustomMacro",
		},
		{
			name:      "Name defaults to logicalID",
			logicalID: "FallbackMacro",
			props: map[string]any{
				"FunctionName": "arn:aws:lambda:us-east-1:000000000000:function:macro-fn",
			},
			wantMacName: "FallbackMacro",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtensibilityBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::CloudFormation::Macro",
				tt.props, nil, make(map[string]string))
			require.NoError(t, err)
			assert.Equal(t, tt.wantMacName, physID)

			rec := backends.MacroRegistry.Get(tt.wantMacName)
			require.NotNil(t, rec)
			assert.Equal(t, tt.wantMacName, rec.Name)
		})
	}
}

func TestResourceCreator_Macro_Delete(t *testing.T) {
	t.Parallel()

	backends := newExtensibilityBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Register a macro.
	_, err := rc.Create(
		t.Context(),
		"MyMacro",
		"AWS::CloudFormation::Macro",
		map[string]any{
			"Name":         "MyMacro",
			"FunctionName": "arn:aws:lambda:us-east-1:000000000000:function:fn",
		},
		nil,
		make(map[string]string),
	)
	require.NoError(t, err)

	rec := backends.MacroRegistry.Get("MyMacro")
	require.NotNil(t, rec, "macro should be registered")

	// Delete it.
	err = rc.Delete(t.Context(), "AWS::CloudFormation::Macro", "MyMacro", nil)
	require.NoError(t, err)

	rec = backends.MacroRegistry.Get("MyMacro")
	assert.Nil(t, rec, "macro should be removed after delete")
}

// ---- Full stack: Macro registration in CFN template ----

func TestStack_Macro_RegisteredViaTemplate(t *testing.T) {
	t.Parallel()

	backends := newExtensibilityBackends()
	rc := cloudformation.NewResourceCreator(backends)
	b := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", rc)

	template := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyMacro": {
      "Type": "AWS::CloudFormation::Macro",
      "Properties": {
        "Name": "StringTransform",
        "FunctionName": "arn:aws:lambda:us-east-1:000000000000:function:string-transform"
      }
    }
  }
}`

	stack, err := b.CreateStack(
		t.Context(),
		"macro-test",
		template,
		nil,
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

	rec := backends.MacroRegistry.Get("StringTransform")
	require.NotNil(t, rec)
	assert.Equal(t, "StringTransform", rec.Name)
	assert.Contains(t, rec.FunctionARN, "string-transform")
}

// ---- MacroRegistry: InvokeMacro with mock Lambda ----

func TestMacroRegistry_InvokeMacro_WithMockLambda(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fragment     map[string]any
		lambdaResp   map[string]any
		wantFragment map[string]any
		name         string
		macroName    string
	}{
		{
			name:      "macro transforms fragment successfully",
			macroName: "UpperCase",
			fragment:  map[string]any{"key": "value"},
			lambdaResp: map[string]any{
				"status":   "success",
				"fragment": map[string]any{"KEY": "VALUE"},
			},
			wantFragment: map[string]any{"KEY": "VALUE"},
		},
		{
			name:      "macro returns failure passes through original",
			macroName: "BrokenMacro",
			fragment:  map[string]any{"original": true},
			lambdaResp: map[string]any{
				"status":       "failure",
				"errorMessage": "macro processing error",
			},
			wantFragment: map[string]any{"original": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockLambda := &mockLambdaStorageBackend{
				invokeFunc: func(_ context.Context, _ string, _ lambdabackend.InvocationType, _ []byte) ([]byte, int, error) {
					respBody, _ := json.Marshal(tt.lambdaResp)

					return respBody, 200, nil
				},
			}

			registry := cloudformation.NewMacroRegistry()
			registry.RegisterForTest(
				tt.macroName,
				"arn:aws:lambda:us-east-1:000000000000:function:macro-fn",
				"",
			)

			fragmentJSON, _ := json.Marshal(tt.fragment)
			result, err := registry.InvokeMacro(
				t.Context(), lambdabackend.NewHandler(mockLambda), tt.macroName, fragmentJSON, nil,
			)
			require.NoError(t, err)

			var got map[string]any
			require.NoError(t, json.Unmarshal(result, &got))
			assert.Equal(t, tt.wantFragment, got)
		})
	}
}

// ---- MacroRegistry: InvokeMacro no Lambda ----

func TestMacroRegistry_InvokeMacro_NoLambdaBackend(t *testing.T) {
	t.Parallel()

	registry := cloudformation.NewMacroRegistry()
	registry.RegisterForTest("MyMacro", "arn:aws:lambda:us-east-1:000000000000:function:fn", "")

	fragment := map[string]any{"unchanged": true}
	fragmentJSON, _ := json.Marshal(fragment)

	// nil lambda backend → returns fragment unchanged.
	result, err := registry.InvokeMacro(t.Context(), nil, "MyMacro", fragmentJSON, nil)
	require.NoError(t, err)
	assert.JSONEq(t, string(fragmentJSON), string(result))
}

// ---- MacroRegistry: InvokeMacro unknown macro ----

func TestMacroRegistry_InvokeMacro_UnknownMacro(t *testing.T) {
	t.Parallel()

	registry := cloudformation.NewMacroRegistry()

	fragment := map[string]any{"pass": "through"}
	fragmentJSON, _ := json.Marshal(fragment)

	mockLambda := &mockLambdaStorageBackend{}
	result, err := registry.InvokeMacro(
		t.Context(), lambdabackend.NewHandler(mockLambda), "DoesNotExist", fragmentJSON, nil,
	)
	require.NoError(t, err)
	assert.JSONEq(t, string(fragmentJSON), string(result))
}
