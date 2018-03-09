package utils

// Interface for representing a replica environment
type Environment interface {
	// Method for generating a monotonically increasing timestamp
	GetNewTimestamp() *Timestamp
	// Method for return the current state
	GetCurrentState() *VersionVector
	// Updates the current state with the given timestamp
	UpdateStateTS(*Timestamp)
	// Updates the current state with the given version vector
	UpdateStateVV(*VersionVector)
}
