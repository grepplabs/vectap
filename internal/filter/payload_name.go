package filter

import "encoding/json"

func ExtractPayloadName(message string) string {
	if message == "" {
		return ""
	}

	var payload struct {
		Name    string `json:"name"`
		Message struct {
			Fields struct {
				Name string `json:"name"`
			} `json:"fields"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(message), &payload); err != nil {
		return ""
	}

	if payload.Name != "" {
		return payload.Name
	}
	if payload.Message.Fields.Name != "" {
		return payload.Message.Fields.Name
	}

	return payload.Name
}
