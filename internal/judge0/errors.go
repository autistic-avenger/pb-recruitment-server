package judge0

import "errors"

var (
	ErrUnavailable         = errors.New("judge0 unavailable")
	ErrInvalidResponse     = errors.New("judge0 returned an invalid response")
	ErrUnsupportedLanguage = errors.New("unsupported language")
)
