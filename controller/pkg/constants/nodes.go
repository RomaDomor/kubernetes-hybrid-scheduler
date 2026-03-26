package constants

const (
	// NodeRoleLabelEdge is the label key for identifying local (edge) nodes.
	NodeRoleLabelEdge = "node.role/edge"

	// NodeClusterLabel identifies which remote cluster a virtual node represents.
	// Value is the ClusterID (e.g., "cloud-1", "cloud-2").
	NodeClusterLabel = "node.cluster/id"

	// LabelValueTrue is a common value for boolean-like labels.
	LabelValueTrue = "true"
)
