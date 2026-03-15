package runconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateAllowed(t *testing.T) {
	require.NoError(t, ValidateAllowed("", "type", "direct", false, "direct", "kubernetes"))
	require.NoError(t, ValidateAllowed("src-a", "format", "", true, "text", "json"))
	require.EqualError(t, ValidateAllowed("", "type", "invalid", false, "direct"), `unsupported type "invalid"`)
	require.EqualError(t, ValidateAllowed("src-a", "format", "xml", false, "text"), `source "src-a" has unsupported format "xml"`)
}

func TestValidateDirectURLs(t *testing.T) {
	require.NoError(t, ValidateDirectURLs("", "kubernetes", "direct", nil))
	require.NoError(t, ValidateDirectURLs("src-a", "direct", "direct", []string{"http://127.0.0.1"}))
	require.EqualError(t, ValidateDirectURLs("", "direct", "direct", nil), "direct-url is required for type=direct")
	require.EqualError(t, ValidateDirectURLs("src-a", "direct", "direct", nil), `source "src-a" requires endpoint url for type=direct`)
}

func TestValidateRangeAndPositive(t *testing.T) {
	require.NoError(t, ValidateRange("", "vector-port", 8686, 1, 65535))
	require.NoError(t, ValidatePositive("src-a", "interval", 1))
	require.EqualError(t, ValidateRange("", "vector-port", 0, 1, 65535), "vector-port must be between 1 and 65535")
	require.EqualError(t, ValidatePositive("src-a", "limit", 0), `source "src-a" limit must be greater than 0`)
}
