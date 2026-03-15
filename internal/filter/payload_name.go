package filter

import "encoding/json"

func ExtractPayloadName(message string) string {
	if message == "" {
		return ""
	}

	var payload struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(message), &payload); err != nil {
		return ""
	}

	return payload.Name
}
