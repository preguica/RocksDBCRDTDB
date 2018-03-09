package opcrdts

type CRDT interface {
	GetType() byte
	Serialize() ([]byte, bool)
	ToString() string
	Apply(CRDTOperation) bool
}

