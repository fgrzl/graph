package pebble

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/fgrzl/graph"
)

var emptyBytes []byte

type pebbleGraph struct {
	db *pebble.DB
}

func NewPebbleGraph(dbPath string) (graph.GraphDB, error) {
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("could not open Pebble database: %v", err)
	}
	return &pebbleGraph{db: db}, nil
}

// PutNode inserts or updates a node in the graph
func (db *pebbleGraph) PutNode(id string, node graph.Node) error {
	nodeKey := getNodeKey(id)
	return db.db.Set(nodeKey, node.Data, pebble.Sync)
}

// PutNodes inserts or updates multiple nodes in the graph
func (db *pebbleGraph) PutNodes(nodes []graph.Node) error {
	batch := db.db.NewBatch()
	for _, node := range nodes {
		nodeKey := getNodeKey(node.ID)
		batch.Set(nodeKey, node.Data, pebble.Sync)
	}
	return batch.Commit(pebble.Sync)
}

// PutEdge inserts or updates an edge in the graph, supporting bidirectional lookups
func (db *pebbleGraph) PutEdge(fromID, toID, edgeType string, params map[string]string) error {
	// Forward edge key
	edgeKey := getEdgeKey(fromID, toID, edgeType)

	// Set the forward edge
	err := db.db.Set(edgeKey, emptyBytes, pebble.Sync)
	if err != nil {
		return fmt.Errorf("failed to put forward edge from %s to %s: %v", fromID, toID, err)
	}

	return nil
}

// PutEdges inserts or updates multiple edges in the graph, supporting bidirectional lookups
func (db *pebbleGraph) PutEdges(edges []graph.Edge) error {
	batch := db.db.NewBatch()
	for _, edge := range edges {
		// Forward edge
		edgeKey := getEdgeKey(edge.From, edge.To, edge.Type)
		batch.Set(edgeKey, emptyBytes, pebble.Sync)

		// Reverse edge
		reverseEdgeKey := getEdgeKey(edge.To, edge.From, edge.Type)
		batch.Set(reverseEdgeKey, emptyBytes, pebble.Sync)
	}
	return batch.Commit(pebble.Sync)
}

// RemoveNode removes a node and its associated edges, supporting bidirectional edge cleanup
func (db *pebbleGraph) RemoveNode(id string) error {
	batch := db.db.NewBatch()

	// Remove the node
	nodeKey := getNodeKey(id)
	batch.Delete(nodeKey, pebble.NoSync)

	// Remove edges related to the node (both forward and reverse)
	iter, err := db.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), []byte("edge:")) {
			break
		}
		key := iter.Key()
		parts := bytes.Split(key, []byte(":"))
		if string(parts[1]) == id || string(parts[2]) == id {
			batch.Delete(key, pebble.NoSync)
		}
	}

	return batch.Commit(pebble.NoSync)
}

// RemoveNodes removes multiple nodes and their associated edges
func (db *pebbleGraph) RemoveNodes(ids ...string) error {
	batch := db.db.NewBatch()

	for _, nodeID := range ids {
		// Remove the node
		nodeKey := getNodeKey(nodeID)
		batch.Delete(nodeKey, pebble.NoSync)

		// Remove edges related to the node (both forward and reverse)
		iter, err := db.db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return err
		}
		defer iter.Close()

		for iter.First(); iter.Valid(); iter.Next() {
			if !bytes.HasPrefix(iter.Key(), []byte("edge:")) {
				break
			}
			key := iter.Key()
			parts := bytes.Split(key, []byte(":"))
			if string(parts[1]) == nodeID || string(parts[2]) == nodeID {
				batch.Delete(key, pebble.NoSync)
			}
		}
	}

	return batch.Commit(pebble.NoSync)
}

// RemoveEdge removes a specific edge, including its reverse counterpart
func (db *pebbleGraph) RemoveEdge(fromID, toID, edgeType string) error {
	// Forward edge key
	edgeKey := getEdgeKey(fromID, toID, edgeType)
	err := db.db.Delete(edgeKey, pebble.NoSync)
	if err != nil {
		return fmt.Errorf("failed to delete forward edge from %s to %s: %v", fromID, toID, err)
	}
	return nil
}

// RemoveEdges removes multiple edges, including their reverse counterparts
func (db *pebbleGraph) RemoveEdges(edges []graph.Edge) error {
	batch := db.db.NewBatch()
	for _, edge := range edges {
		// Forward edge
		edgeKey := getEdgeKey(edge.From, edge.To, edge.Type)
		batch.Delete(edgeKey, pebble.NoSync)
	}
	return batch.Commit(pebble.NoSync)
}

// GetNode retrieves a node by ID
func (db *pebbleGraph) GetNode(id string) (graph.Node, error) {
	nodeKey := getNodeKey(id)
	data, closer, err := db.db.Get(nodeKey)
	if err != nil {
		return graph.Node{}, fmt.Errorf("failed to retrieve node data for %s: %v", id, err)
	}
	defer closer.Close()

	return graph.Node{
		ID:   id,
		Data: data,
	}, nil
}

// GetEdge retrieves an edge by the from and to node IDs and edge type
func (db *pebbleGraph) GetEdge(fromID, toID, edgeType string) (graph.Edge, error) {
	edgeKey := getEdgeKey(fromID, toID, edgeType)
	_, closer, err := db.db.Get(edgeKey)
	if err != nil {
		return graph.Edge{}, fmt.Errorf("failed to retrieve edge data for %s-%s: %v", fromID, toID, err)
	}
	defer closer.Close()

	return graph.Edge{From: fromID, To: toID, Type: edgeType}, nil

}

func (db *pebbleGraph) Traverse(nodeID string, dependencies map[string]bool, maxDepth int) ([]graph.Node, []graph.Edge, error) {
	visitedNodes := make(map[string]bool)
	visitedEdges := make(map[string]bool)
	var resultNodes []graph.Node
	var resultEdges []graph.Edge

	// Use a stack of tuples: (nodeID, currentDepth)
	stack := []struct {
		nodeID string
		depth  int
	}{{nodeID, maxDepth}}

	for len(stack) > 0 {
		// Pop from the stack
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if visitedNodes[current.nodeID] || current.depth <= 0 {
			continue
		}
		visitedNodes[current.nodeID] = true

		// Fetch the current node data
		nodeKey := getNodeKey(current.nodeID)
		data, closer, err := db.db.Get(nodeKey)
		if err != nil {
			if err == pebble.ErrNotFound {
				continue
			}
			return nil, nil, fmt.Errorf("failed to retrieve node data for %s: %v", current.nodeID, err)
		}
		defer closer.Close()

		node := graph.Node{
			ID:   current.nodeID,
			Data: data,
		}
		resultNodes = append(resultNodes, node)

		// Collect related edges
		iter, err := db.db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, nil, err
		}
		defer iter.Close()

		for iter.First(); iter.Valid(); iter.Next() {
			if !bytes.HasPrefix(iter.Key(), []byte("edge:")) {
				continue
			}
			key := iter.Key()
			parts := bytes.Split(key, []byte(":"))
			if string(parts[1]) == current.nodeID {
				edge := graph.Edge{
					From: string(parts[1]),
					To:   string(parts[2]),
					Type: string(parts[3]),
				}

				edgeKey := getEdgeKey(edge.From, edge.To, edge.Type)
				if visitedEdges[string(edgeKey)] {
					continue
				}
				visitedEdges[string(edgeKey)] = true

				resultEdges = append(resultEdges, edge)

				// Add the neighboring node to the stack or fetch its data if at max depth
				toNodeID := string(parts[2])
				if !visitedNodes[toNodeID] {
					if current.depth > 1 {
						// Add to stack for further traversal
						stack = append(stack, struct {
							nodeID string
							depth  int
						}{nodeID: toNodeID, depth: current.depth - 1})
					} else {
						// Fetch and collect "to" node data if not traversing deeper
						toNodeKey := getNodeKey(toNodeID)
						toNodeData, closer, err := db.db.Get(toNodeKey)
						if err != nil && err != pebble.ErrNotFound {
							return nil, nil, fmt.Errorf("failed to retrieve node data for %s: %v", toNodeID, err)
						}
						if err == nil {
							defer closer.Close()
							resultNodes = append(resultNodes, graph.Node{
								ID:   toNodeID,
								Data: toNodeData,
							})
							visitedNodes[toNodeID] = true
						}
					}
				}
			}
		}
	}

	return resultNodes, resultEdges, nil
}

func (db *pebbleGraph) Close() error {
	if db.db == nil {
		return nil // Already closed, no action needed
	}

	err := db.db.Close()
	if err != nil {
		return fmt.Errorf("failed to close the database: %v", err)
	}

	// Set db.db to nil to mark it as closed
	db.db = nil
	return nil
}

func getNodeKey(nodeID string) []byte {
	return []byte("node:" + nodeID)
}

func getEdgeKey(fromID, toID, edgeType string) []byte {
	return []byte("edge:" + fromID + ":" + toID + ":" + edgeType)
}
