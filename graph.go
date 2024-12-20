package graph

type GraphDB interface {
	PutNode(id string, node Node) error
	PutNodes(nodes []Node) error
	PutEdge(fromID, toID, edgeType string, params map[string]string) error
	PutEdges(edges []Edge) error
	RemoveNode(nodeID string) error
	RemoveEdge(fromID, toID, edgeType string) error
	RemoveEdges(edges []Edge) error
	Traverse(nodeID string, dependencies map[string]bool, depth int) ([]Node, []Edge, error)
	Close() error
}

// Node structure
type Node struct {
	ID   string            `json:"id"`
	Data map[string]string `json:"data"`
}

// Edge structure
type Edge struct {
	From   string            `json:"from"`
	To     string            `json:"to"`
	Type   string            `json:"type"`
	Params map[string]string `json:"params"`
}
