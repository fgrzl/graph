package graph

// Graph represents the interface for graph database operations.
type Graph interface {
	// Node-related methods
	PutNode(node Node) error
	PutNodes(nodes []Node) error
	RemoveNode(id string) error
	RemoveNodes(ids ...string) error
	GetNode(id string) (Node, error)

	// Edge-related methods
	PutEdge(edge Edge) error
	PutEdges(edges []Edge) error
	RemoveEdge(edge Edge) error
	RemoveEdges(edges []Edge) error

	// Graph traversal
	Traverse(nodeID string, edgeTypes EdgeTypes, depth int) ([]Node, []Edge, error)

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
	From string `json:"from"`
	To   string `json:"to"`
	Type string `json:"type"`
}

type EdgeTypes map[string]struct{}
