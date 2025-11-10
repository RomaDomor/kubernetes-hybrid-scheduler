package util

import "testing"

func TestGetEnvInt_Fallback(t *testing.T) {
	const defaultVal = 123

	// Test case where env var is not set
	if val := GetEnvInt("UNSET_VAR", defaultVal); val != defaultVal {
		t.Errorf("Expected default value for unset var, got %d", val)
	}

	// Test case where env var is set to an invalid value
	t.Setenv("INVALID_INT_VAR", "not-a-number")
	if val := GetEnvInt("INVALID_INT_VAR", defaultVal); val != defaultVal {
		t.Errorf("Expected default value for invalid var, got %d", val)
	}
}

func TestGetEnvFloat_Fallback(t *testing.T) {
	const defaultVal = 123.45

	// Test case where env var is not set
	if val := GetEnvFloat("UNSET_VAR", defaultVal); val != defaultVal {
		t.Errorf("Expected default value for unset var, got %f", val)
	}

	// Test case where env var is set to an invalid value
	t.Setenv("INVALID_FLOAT_VAR", "not-a-float")
	if val := GetEnvFloat("INVALID_FLOAT_VAR", defaultVal); val != defaultVal {
		t.Errorf("Expected default value for invalid var, got %f", val)
	}
}
