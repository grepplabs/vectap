package runconfig

import "fmt"

func ValidateAllowed(name, field, value string, allowEmpty bool, allowed ...string) error {
	if value == "" && allowEmpty {
		return nil
	}
	for _, candidate := range allowed {
		if value == candidate {
			return nil
		}
	}
	if name == "" {
		return fmt.Errorf("unsupported %s %q", field, value)
	}
	return fmt.Errorf("source %q has unsupported %s %q", name, field, value)
}

func ValidateDirectURLs(name, sourceType, directType string, directURLs []string) error {
	if sourceType != directType || len(directURLs) > 0 {
		return nil
	}
	if name == "" {
		return fmt.Errorf("direct-url is required for type=%s", directType)
	}
	return fmt.Errorf("source %q requires endpoint url for type=%s", name, directType)
}

func ValidateRange(name, field string, value, lowerBound, upperBound int) error {
	if value >= lowerBound && value <= upperBound {
		return nil
	}
	if name == "" {
		return fmt.Errorf("%s must be between %d and %d", field, lowerBound, upperBound)
	}
	return fmt.Errorf("source %q %s must be between %d and %d", name, field, lowerBound, upperBound)
}

func ValidatePositive(name, field string, value int) error {
	if value > 0 {
		return nil
	}
	if name == "" {
		return fmt.Errorf("%s must be greater than 0", field)
	}
	return fmt.Errorf("source %q %s must be greater than 0", name, field)
}
