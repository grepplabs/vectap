package runconfig

import (
	"errors"
	"fmt"
)

func Select[T any](
	all bool,
	selectedNames []string,
	sources []T,
	nameOf func(T) string,
	enabledOf func(T) bool,
) ([]T, error) {
	if len(sources) == 0 {
		return nil, errors.New("no sources configured")
	}

	byName := make(map[string]T, len(sources))
	for _, s := range sources {
		byName[nameOf(s)] = s
	}

	selected := make([]T, 0, len(sources))
	if all {
		for _, s := range sources {
			if enabledOf(s) {
				selected = append(selected, s)
			}
		}
	} else {
		for _, name := range selectedNames {
			s, ok := byName[name]
			if !ok {
				return nil, fmt.Errorf("unknown source %q", name)
			}
			selected = append(selected, s)
		}
	}

	if len(selected) == 0 {
		return nil, errors.New("no sources selected")
	}

	return selected, nil
}
