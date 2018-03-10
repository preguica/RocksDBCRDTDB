// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package opcrdts

type CRDT interface {
	GetType() byte
	Serialize() ([]byte, bool)
	ToString() string
	Apply(CRDTOperation) bool
}

