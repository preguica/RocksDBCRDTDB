package opcrdts

type CRDTOperation interface {
	GetCRDTType() byte
	GetType() byte
	Merge(CRDTOperation) bool
	Serialize() ([]byte, bool)
}
