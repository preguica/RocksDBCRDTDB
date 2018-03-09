package utils

import (
	"sync"
	"time"
)

// Simple implementation of replica environment.
// Timestamps are created based on clock time.
type SimpleEnvironment struct {
	Dc     DCId
	LastTs int64
	State  *VersionVector
	Mux    sync.Mutex
}

// return a new SimpleEnvironment
func NewSimpleEnvironment( dc DCId ) *SimpleEnvironment{
	env := SimpleEnvironment{Dc: dc}
	env.LastTs = 0
	env.State = NewVersionVector()
	return &env
}

// Interface for representing a replica environment - Timestamps are created based on clock time.
func (env *SimpleEnvironment) GetNewTimestamp() *Timestamp {
	t := time.Now().UnixNano()
	env.Mux.Lock()
	if env.LastTs < t {
		env.LastTs = t
	} else {
		env.LastTs++
	}
	t = env.LastTs;
	env.Mux.Unlock()
	return &Timestamp{env.Dc, t}
}

// Version vector with current state.
// NOTE: use with care -- for read-only
func (env *SimpleEnvironment) ExportCurrentState() *VersionVector {
	return env.State
}

// Version vector with current state.
func (env *SimpleEnvironment) GetCurrentState() *VersionVector {
	return NewVersionVectorVV( env.State)
}

// Updates the current state with the given timestamp
func (env *SimpleEnvironment) UpdateStateTS( ts *Timestamp) {
	env.State.AddTS( ts)
}

// Updates the current state with the given version vector
func (env *SimpleEnvironment) UpdateStateVV( vv *VersionVector) {
	env.State.PointwiseMax( vv)
}
