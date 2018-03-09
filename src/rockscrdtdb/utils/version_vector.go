package utils

//Represents a version vector
type VersionVector struct {
	Val map[DCId]int64
}

// Create a new version vector
func NewVersionVector() *VersionVector {
	return NewVersionVectorVV( nil)
}

// Create a new version vector that is a copy of the given version vector
func NewVersionVectorVV( otherVv *VersionVector) *VersionVector {
	vv := VersionVector{}
	vv.Val = make(map[DCId]int64)
	if otherVv != nil {
		for k, v := range otherVv.Val {
			vv.Val[k] = v
		}
	}
	return &vv
}

// Add Timestamp to version vector
func (vv *VersionVector)AddTS( ts *Timestamp) {
	v,ok := vv.Val[ts.Dc]
	if ok == false || v < ts.Ts {
		vv.Val[ts.Dc] = ts.Ts
	}
}

// Merge with other version vector, keeping the largest value for each entry
func (vv *VersionVector)PointwiseMax( otherVv *VersionVector) {
	if otherVv == nil {
		return
	}
	for k, v := range otherVv.Val {
		oldV, ok := vv.Val[k]
		if ok {
			if v > oldV {
				vv.Val[k] = v
			}
		} else {
			vv.Val[k] = v
		}
	}
}

// Remove an entry in the current version vector if the otherVv has a larger value for the same entry
func (vv *VersionVector)RemoveIfLargerOrEqual( otherVv *VersionVector) {
	if otherVv == nil {
		return
	}
	for k, v := range otherVv.Val {
		oldV, ok := vv.Val[k]
		if ok && v >= oldV{
			delete(vv.Val, k)
		}
	}
}

// Returns true is this VersionVector has no entry
func (vv *VersionVector)IsEmpty() bool {
	return vv.Val == nil || len(vv.Val) == 0
}
