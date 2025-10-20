package constants

// Location represents where a workload should be scheduled
type Location string

const (
	Edge  Location = "edge"
	Cloud Location = "cloud"
)
