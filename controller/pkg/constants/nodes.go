package constants

// This file centralizes node-related constants, such as labels and taints.

const (
	// NodeRoleLabelEdge is the label key for identifying edge nodes.
	NodeRoleLabelEdge = "node.role/edge"
	// NodeRoleLabelCloud is the label key for identifying cloud nodes.
	NodeRoleLabelCloud = "node.role/cloud"

	// LabelValueTrue is a common value for boolean-like labels.
	LabelValueTrue = "true"
)
