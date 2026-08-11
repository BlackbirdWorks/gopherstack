package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetRiskConfiguration_PersistsRiskExceptionConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "blocked_ip_ranges"},
		{name: "skipped_ip_ranges"},
		{name: "both_ranges"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "risk-exc-"+tt.name+"-pool")

			body := map[string]any{
				"UserPoolId": poolID,
			}

			switch tt.name {
			case "blocked_ip_ranges":
				body["RiskExceptionConfiguration"] = map[string]any{
					"BlockedIPRangeList": []string{"10.0.0.0/8", "192.168.0.0/16"},
				}
			case "skipped_ip_ranges":
				body["RiskExceptionConfiguration"] = map[string]any{
					"SkippedIPRangeList": []string{"172.16.0.0/12"},
				}
			case "both_ranges":
				body["RiskExceptionConfiguration"] = map[string]any{
					"BlockedIPRangeList": []string{"10.0.0.0/8"},
					"SkippedIPRangeList": []string{"172.16.0.0/12"},
				}
			}

			rec := doCognitoRequest(t, h, "SetRiskConfiguration", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var setOut struct {
				RiskConfiguration *struct {
					RiskExceptionConfiguration *struct {
						BlockedIPRangeList []string `json:"BlockedIPRangeList,omitempty"`
						SkippedIPRangeList []string `json:"SkippedIPRangeList,omitempty"`
					} `json:"RiskExceptionConfiguration"`
				} `json:"RiskConfiguration"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &setOut))
			require.NotNil(t, setOut.RiskConfiguration)
			require.NotNil(
				t,
				setOut.RiskConfiguration.RiskExceptionConfiguration,
				"RiskExceptionConfiguration must be present in Set response",
			)

			switch tt.name {
			case "blocked_ip_ranges":
				assert.Contains(t, setOut.RiskConfiguration.RiskExceptionConfiguration.BlockedIPRangeList, "10.0.0.0/8")
			case "skipped_ip_ranges":
				assert.Contains(
					t,
					setOut.RiskConfiguration.RiskExceptionConfiguration.SkippedIPRangeList,
					"172.16.0.0/12",
				)
			case "both_ranges":
				assert.Contains(t, setOut.RiskConfiguration.RiskExceptionConfiguration.BlockedIPRangeList, "10.0.0.0/8")
				assert.Contains(
					t,
					setOut.RiskConfiguration.RiskExceptionConfiguration.SkippedIPRangeList,
					"172.16.0.0/12",
				)
			}

			// Verify Describe also returns the persisted config.
			rec = doCognitoRequest(t, h, "DescribeRiskConfiguration", map[string]any{
				"UserPoolId": poolID,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var descOut struct {
				RiskConfiguration *struct {
					RiskExceptionConfiguration *struct {
						BlockedIPRangeList []string `json:"BlockedIPRangeList,omitempty"`
						SkippedIPRangeList []string `json:"SkippedIPRangeList,omitempty"`
					} `json:"RiskExceptionConfiguration"`
				} `json:"RiskConfiguration"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descOut))
			require.NotNil(t, descOut.RiskConfiguration)
			require.NotNil(
				t,
				descOut.RiskConfiguration.RiskExceptionConfiguration,
				"RiskExceptionConfiguration must survive round-trip",
			)
		})
	}
}

func TestRiskConfiguration_CompromisedCredentials(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "risk-cc-pool")

	rec := doCognitoRequest(t, h, "SetRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
		"CompromisedCredentialsRiskConfiguration": map[string]any{
			"EventFilter": []string{"SIGN_IN", "PASSWORD_CHANGE"},
			"Actions": map[string]any{
				"EventAction": "BLOCK",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var setOut struct {
		RiskConfiguration *struct {
			CompromisedCredentialsRiskConfiguration *struct {
				Actions *struct {
					EventAction string `json:"EventAction,omitempty"`
				} `json:"Actions"`
				EventFilter []string `json:"EventFilter,omitempty"`
			} `json:"CompromisedCredentialsRiskConfiguration"`
		} `json:"RiskConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &setOut))
	require.NotNil(t, setOut.RiskConfiguration)
	require.NotNil(t, setOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration)
	assert.Contains(t, setOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration.EventFilter, "SIGN_IN")
	require.NotNil(t, setOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration.Actions)
	assert.Equal(t, "BLOCK", setOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration.Actions.EventAction)

	// Describe and verify persisted.
	rec = doCognitoRequest(t, h, "DescribeRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descOut struct {
		RiskConfiguration *struct {
			CompromisedCredentialsRiskConfiguration *struct {
				Actions *struct {
					EventAction string `json:"EventAction,omitempty"`
				} `json:"Actions"`
			} `json:"CompromisedCredentialsRiskConfiguration"`
		} `json:"RiskConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descOut))
	require.NotNil(t, descOut.RiskConfiguration)
	require.NotNil(t, descOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration)
	assert.Equal(t, "BLOCK", descOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration.Actions.EventAction)
}

func TestRiskConfiguration_AccountTakeover(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "risk-at-pool")

	rec := doCognitoRequest(t, h, "SetRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
		"AccountTakeoverRiskConfiguration": map[string]any{
			"Actions": map[string]any{
				"HighAction": map[string]any{
					"Notify":      true,
					"EventAction": "BLOCK",
				},
				"MediumAction": map[string]any{
					"Notify":      true,
					"EventAction": "MFA_IF_CONFIGURED",
				},
				"LowAction": map[string]any{
					"Notify":      false,
					"EventAction": "NO_ACTION",
				},
			},
			"NotifyConfiguration": map[string]any{
				"From":      "noreply@example.com",
				"SourceArn": "arn:aws:ses:us-east-1:123:identity/example.com",
				"BlockEmail": map[string]any{
					"Subject":  "Your account has been blocked",
					"HtmlBody": "<html>Your account was blocked.</html>",
					"TextBody": "Your account was blocked.",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		RiskConfiguration *struct {
			AccountTakeoverRiskConfiguration *struct {
				Actions *struct {
					HighAction *struct {
						EventAction string `json:"EventAction,omitempty"`
						Notify      bool   `json:"Notify,omitempty"`
					} `json:"HighAction"`
					MediumAction *struct {
						EventAction string `json:"EventAction,omitempty"`
					} `json:"MediumAction"`
					LowAction *struct {
						Notify bool `json:"Notify,omitempty"`
					} `json:"LowAction"`
				} `json:"Actions"`
				NotifyConfiguration *struct {
					BlockEmail *struct {
						Subject string `json:"Subject,omitempty"`
					} `json:"BlockEmail"`
					From string `json:"From,omitempty"`
				} `json:"NotifyConfiguration"`
			} `json:"AccountTakeoverRiskConfiguration"`
		} `json:"RiskConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.RiskConfiguration)
	require.NotNil(t, out.RiskConfiguration.AccountTakeoverRiskConfiguration)
	require.NotNil(t, out.RiskConfiguration.AccountTakeoverRiskConfiguration.Actions)

	a := out.RiskConfiguration.AccountTakeoverRiskConfiguration.Actions
	require.NotNil(t, a.HighAction)
	assert.True(t, a.HighAction.Notify)
	assert.Equal(t, "BLOCK", a.HighAction.EventAction)
	require.NotNil(t, a.MediumAction)
	assert.Equal(t, "MFA_IF_CONFIGURED", a.MediumAction.EventAction)
	require.NotNil(t, a.LowAction)
	assert.False(t, a.LowAction.Notify)

	nc := out.RiskConfiguration.AccountTakeoverRiskConfiguration.NotifyConfiguration
	require.NotNil(t, nc)
	assert.Equal(t, "noreply@example.com", nc.From)
	require.NotNil(t, nc.BlockEmail)
	assert.Equal(t, "Your account has been blocked", nc.BlockEmail.Subject)
}

func TestRiskConfiguration_PerClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "risk-perclient-pool")

	rec := doCognitoRequest(t, h, "SetRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"CompromisedCredentialsRiskConfiguration": map[string]any{
			"EventFilter": []string{"SIGN_IN"},
			"Actions":     map[string]any{"EventAction": "NO_ACTION"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Pool-level should still be empty.
	rec = doCognitoRequest(t, h, "DescribeRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var poolOut struct {
		RiskConfiguration *struct {
			CompromisedCredentialsRiskConfiguration *struct{} `json:"CompromisedCredentialsRiskConfiguration,omitempty"`
		} `json:"RiskConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &poolOut))
	assert.Nil(t, poolOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration)

	// Client-level should be set.
	rec = doCognitoRequest(t, h, "DescribeRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var clientOut struct {
		RiskConfiguration *struct {
			CompromisedCredentialsRiskConfiguration *struct {
				Actions *struct {
					EventAction string `json:"EventAction,omitempty"`
				} `json:"Actions"`
			} `json:"CompromisedCredentialsRiskConfiguration"`
		} `json:"RiskConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &clientOut))
	require.NotNil(t, clientOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration)
	assert.Equal(
		t,
		"NO_ACTION",
		clientOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration.Actions.EventAction,
	)
}

func TestRiskConfiguration_Backend_Direct(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("risk-backend-pool")
	require.NoError(t, err)

	cfg := &cognitoidp.TypedRiskConfiguration{
		UserPoolID: pool.ID,
		ClientID:   "",
		CompromisedCredentialsRiskConfig: &cognitoidp.CompromisedCredentialsRiskConfig{
			EventFilter: []string{"SIGN_IN", "SIGN_UP"},
			Actions:     &cognitoidp.CompromisedCredentialsActions{EventAction: "BLOCK"},
		},
		AccountTakeoverRiskConfig: &cognitoidp.AccountTakeoverRiskConfig{
			Actions: &cognitoidp.AccountTakeoverActions{
				HighAction: &cognitoidp.AccountTakeoverActionType{Notify: true, EventAction: "BLOCK"},
			},
		},
	}

	require.NoError(t, b.SetTypedRiskConfiguration(cfg))

	got, err := b.GetTypedRiskConfiguration(pool.ID, "")
	require.NoError(t, err)
	require.NotNil(t, got.CompromisedCredentialsRiskConfig)
	assert.Equal(t, "BLOCK", got.CompromisedCredentialsRiskConfig.Actions.EventAction)
	require.NotNil(t, got.AccountTakeoverRiskConfig)
	require.NotNil(t, got.AccountTakeoverRiskConfig.Actions.HighAction)
	assert.True(t, got.AccountTakeoverRiskConfig.Actions.HighAction.Notify)

	// Invalid pool should fail.
	err = b.SetTypedRiskConfiguration(&cognitoidp.TypedRiskConfiguration{UserPoolID: "bad-pool"})
	require.Error(t, err)
}

func TestRiskConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "risk-cfg-pool")

	// Describe before set — pool exists, returns empty
	rec := doCognitoRequest(t, h, "DescribeRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Set
	rec = doCognitoRequest(t, h, "SetRiskConfiguration", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Set with invalid pool
	rec = doCognitoRequest(t, h, "SetRiskConfiguration", map[string]any{
		"UserPoolId": "bad-pool",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestLogDelivery(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "log-delivery-pool")

	// Get before set — empty config
	rec := doCognitoRequest(t, h, "GetLogDeliveryConfiguration", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Set
	rec = doCognitoRequest(t, h, "SetLogDeliveryConfiguration", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get invalid pool
	rec = doCognitoRequest(t, h, "GetLogDeliveryConfiguration", map[string]any{
		"UserPoolId": "bad-pool",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSetLogDeliveryConfiguration_LogConfigurationsPersistAndEcho(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "log-delivery-configs-pool")

	logConfigs := []any{map[string]any{
		"LogLevel":    "ERROR",
		"EventSource": "userNotification",
		"CloudWatchLogsConfiguration": map[string]any{
			"LogGroupArn": "arn:aws:logs:us-east-1:000000000000:log-group:/cognito/test",
		},
	}}

	setRec := doCognitoRequest(t, h, "SetLogDeliveryConfiguration", map[string]any{
		"UserPoolId":        poolID,
		"LogConfigurations": logConfigs,
	})
	require.Equal(t, http.StatusOK, setRec.Code, setRec.Body.String())

	var setOut struct {
		LogDeliveryConfiguration struct {
			UserPoolID        string           `json:"UserPoolId,omitempty"`
			LogConfigurations []map[string]any `json:"LogConfigurations,omitempty"`
		} `json:"LogDeliveryConfiguration"`
	}
	require.NoError(t, json.Unmarshal(setRec.Body.Bytes(), &setOut))
	require.Len(t, setOut.LogDeliveryConfiguration.LogConfigurations, 1,
		"LogConfigurations is a required accepted field and must not be silently dropped",
	)
	assert.Equal(t, "ERROR", setOut.LogDeliveryConfiguration.LogConfigurations[0]["LogLevel"])

	getRec := doCognitoRequest(t, h, "GetLogDeliveryConfiguration", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		LogDeliveryConfiguration struct {
			LogConfigurations []map[string]any `json:"LogConfigurations,omitempty"`
		} `json:"LogDeliveryConfiguration"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	require.Len(t,
		getOut.LogDeliveryConfiguration.LogConfigurations, 1, "Set must actually persist for Get to read back",
	)
	assert.Equal(t, "ERROR", getOut.LogDeliveryConfiguration.LogConfigurations[0]["LogLevel"])
}
