package utils

// Timestamp: Lamport timestamp
type Timestamp struct {
	Dc DCId
	Ts int64
}

func NewTimestamp( dc DCId, ts int64) *Timestamp {
	return &Timestamp{dc,ts}
}

// Returns negative is this timestamp is smaller than otherTs,
// 0 if it is equal and 1 if it is larger
func (ts Timestamp)CompareTo( otherTs *Timestamp) int {
	if( ts.Ts < otherTs.Ts) {
		return -1
	} else if( ts.Ts > otherTs.Ts) {
		return 1
	} else {
		return ts.Dc.CompareTo(&otherTs.Dc)
	}
}



