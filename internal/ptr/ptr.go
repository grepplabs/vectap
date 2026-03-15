package ptr

// To returns a pointer to the given value.
func To[T any](v T) *T {
	return &v
}

// Default returns fallback when value is the zero value for T.
func Default[T comparable](value, fallback T) T {
	var zero T
	if value == zero {
		return fallback
	}
	return value
}

// Deref dereferences ptr and returns the value it points to if not nil, or else
// returns def.
func Deref[T any](ptr *T, def T) T {
	if ptr != nil {
		return *ptr
	}
	return def
}
