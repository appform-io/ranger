package rangerdns

import (
	"testing"

	"github.com/coredns/caddy"
	"github.com/stretchr/testify/assert"
)

// TestSetup tests the various things that should be parsed by setup.
// Make sure you also test for parse errors.
func TestSetup(t *testing.T) {
	tests := []struct {
		config  string
		error   bool
		message string
	}{
		{
			`ranger {
				endpoint http://url.random
			}`,
			false,
			"Valid config",
		},

		{
			`ranger {
				endpoint http://url.random
				skip_data_from_replication_sources false
			}`,
			false,
			"Valid config",
		},
		{
			`ranger {
				endpoint http://url.random
				skip_data_from_replication_sources abc
			}`,
			true,
			"Invalid replication source value",
		},
		{
			`ranger {
				endpoint http://url.random 8080
			}`,
			true,
			"Invalid endpoint",
		},
		{
			`ranger {
				skip_data_from_replication_sources false
			}`,
			true,
			"Missing endpoint",
		},
		{
			`ranger {
				endpoint http://url.random 8080
				random
			}`,
			true,
			"Random arg cannot be added",
		},
	}

	for _, tt := range tests {
		c := caddy.NewTestController("rangerdns", tt.config)
		_, err := parseAndCreate(c)
		if tt.error {
			assert.Error(t, err, tt.message)
		} else {
			assert.NoError(t, err, tt.message)
		}

	}

}
