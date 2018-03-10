package common

import "fmt"

// Struct for representing reference to an object that does not exist
type NoObjectError struct {
	key string
}

func NewNoObjectError( key string) *NoObjectError{
	return &NoObjectError{key}
}

func (o *NoObjectError)Error() string {
	return fmt.Sprintf( "Key %s does not exist", o.key)
}

