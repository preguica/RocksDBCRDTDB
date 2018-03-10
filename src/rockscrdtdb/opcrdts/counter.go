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

func (m *Counter) Apply(op CRDTOperation) bool {
	cntOp, ok := (op).(*CounterOpAdd)
	if ok == false {
		return false
	}
	m.val = m.val + cntOp.delta
	return true
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

/*
func OpCRDT_Counter_FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	cnt := &Counter{}
	obj, ok := UnserializeCounter( existingValue)
	if ok {
		cnt, ok = obj.(*Counter)
		if ok == false {
			return nil, false
		}
	}

	for _, opB := range operands {
		opObj, ok := UnserializeCounterOpAdd( opB)
		if ! ok {
			return nil,false
		}
		op, ok2 := opObj.(*CounterOpAdd)
		if ok2 == false {
			return nil, false
		}
		cnt.Apply(op)
	}
	objFinal, okFinal := cnt.Serialize()
	if ! okFinal {
		return nil, false
	} else {
		return objFinal, true
	}
}

func OpCRDT_Counter_PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	leftObj, ok := UnserializeCounterOpAdd( leftOperand)
	if ok == false {
		return nil,false
	}
	leftOp, ok := leftObj.(*CounterOpAdd)
	if ok == false {
		return nil, false
	}
	rightObj, ok := UnserializeCounterOpAdd( rightOperand)
	if ok == false {
		return nil,false
	}
	rightOp, ok := rightObj.(*CounterOpAdd)
	if ok == false {
		return nil, false
	}
	ok = leftOp.merge(rightOp)
	if ok == false {
		return nil, false
	} else {
		return leftOp.Serialize()
	}
}
*/

