// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
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

