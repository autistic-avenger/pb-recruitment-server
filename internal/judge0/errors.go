package judge0

import "errors"

var (
	ErrUnavailable         = errors.New("judge0 unavailable")
	ErrInvalidResponse     = errors.New("judge0 returned an invalid response")
	ErrNoWorkers           = errors.New("judge0 queue has no available workers")
	ErrUnsupportedLanguage = errors.New("unsupported language")
)
