package model

import "errors"

// ErrValidation is returned when input fails business validation.
type ErrValidation string

func (e ErrValidation) Error() string {
	return string(e)
}

// ErrNotFound is returned when a requested entity does not exist.
var ErrNotFound = errors.New("not found")
