package constants

// ClusterID identifies a cluster in the federation.
// k=0 is the local (edge) cluster; k>=1 are remote clusters (Liqo virtual nodes).
type ClusterID string

// Location is an alias for backward compatibility in annotations/logs.
type Location = ClusterID

const (
	// LocalCluster is the cluster where the controller runs (k=0).
	LocalCluster ClusterID = "local"
)

// IsLocal returns true if the cluster is the local (controller) cluster.
func IsLocal(id ClusterID) bool {
	return id == LocalCluster
}
