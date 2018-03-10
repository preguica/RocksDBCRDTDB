package opcrdts

import (
	"encoding/binary"
	"fmt"
	"rockscrdtdb/utils"
)

// Operation-based Counter CRDT
type Counter struct {
	val int64
}

func NewCounter() *Counter {
	return &Counter{0}
}

func (m *Counter) Serialize() ([]byte, bool) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64((*m).val))
	return buf,true
}

func (m *Counter) GetType() byte {
	return CRDT_OPCOUNTER
}

func (m *Counter) ToString() string {
	return fmt.Sprint( m.val)
}

func UnserializeCounter(b []byte) (CRDT, bool) {
	if len(b) != 8 {
		return nil,false
	}
	return &Counter{int64(binary.BigEndian.Uint64(b))},true
}

func (m *Counter) Val() int64 {
	return m.val
}

func (obj *Counter) Apply( op CRDTOperation) bool {
	return op.Apply( obj)
}

func (m *Counter) Value() int64 {
	return m.val
}

func OpCRDT_CounterAdd( ts *utils.Timestamp, vv *utils.VersionVector, v int64) CRDTOperation {
	return &CounterOpAdd{v}
}

func (m *Counter) Add( ts *utils.Timestamp, vv *utils.VersionVector, v int64) CRDTOperation {
	return OpCRDT_CounterAdd(ts, vv, v)
}

