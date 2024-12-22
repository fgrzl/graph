package graph

// GraphDB represents the interface for graph database operations.
type GraphDB interface {
	// Node-related methods
	PutNode(id string, node Node) error
	PutNodes(nodes []Node) error
	RemoveNode(id string) error
	RemoveNodes(ids ...string) error
	GetNode(id string) (Node, error) // Added method

	// Edge-related methods
	PutEdge(from, to, edgeType string, params map[string]string) error
	PutEdges(edges []Edge) error
	RemoveEdge(from, to, edgeType string) error
	RemoveEdges(edges []Edge) error
	GetEdge(from, to, edgeType string) (Edge, error) // Added method

	// Graph traversal
	Traverse(nodeID string, dependencies map[string]bool, depth int) ([]Node, []Edge, error)

	// General methods
	Close() error
}

// Node structure
type Node struct {
	ID   string `json:"id"`
	Data []byte `json:"data"`
}

// Edge structure
type Edge struct {
	From   string            `json:"from"`
	To     string            `json:"to"`
	Type   string            `json:"type"`
	Params map[string]string `json:"params"`
}
