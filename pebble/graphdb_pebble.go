package pebble

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/cockroachdb/pebble"
	"github.com/fgrzl/graph"
)

type GraphDBPebble struct {
	db *pebble.DB
}

func NewGraphDBPebble(dbPath string) (graph.GraphDB, error) {
	// Open the Pebble database
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("could not open Pebble database: %v", err)
	}
	return &GraphDBPebble{db: db}, nil
}

// Helper function to serialize data (JSON encoding)
func serialize(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

// Helper function to deserialize data (JSON decoding)
func deserialize(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// PutNode inserts or updates a node in the graph
func (db *GraphDBPebble) PutNode(id string, node graph.Node) error {
	nodeKey := []byte("node:" + id)
	serializedNode, err := serialize(node)
	if err != nil {
		return fmt.Errorf("failed to serialize node with ID %s: %v", id, err)
	}

	return db.db.Set(nodeKey, serializedNode, pebble.Sync)
}

// PutNodes inserts or updates multiple nodes in the graph
func (db *GraphDBPebble) PutNodes(nodes []graph.Node) error {
	batch := db.db.NewBatch()
	for _, node := range nodes {
		nodeKey := []byte("node:" + node.ID)
		serializedNode, err := serialize(node)
		if err != nil {
			return fmt.Errorf("failed to serialize node with ID %s: %v", node.ID, err)
		}
		batch.Set(nodeKey, serializedNode, pebble.Sync)
	}

	return batch.Commit(pebble.Sync)
}

// PutEdge inserts or updates an edge in the graph
func (db *GraphDBPebble) PutEdge(fromID, toID, edgeType string, params map[string]string) error {
	edgeKey := []byte("edge:" + fromID + ":" + toID + ":" + edgeType)
	serializedEdge, err := serialize(params)
	if err != nil {
		return fmt.Errorf("failed to serialize edge from %s to %s: %v", fromID, toID, err)
	}

	return db.db.Set(edgeKey, serializedEdge, pebble.Sync)
}

// PutEdges inserts or updates multiple edges in the graph
func (db *GraphDBPebble) PutEdges(edges []graph.Edge) error {
	batch := db.db.NewBatch()
	for _, edge := range edges {
		edgeKey := []byte("edge:" + edge.From + ":" + edge.To + ":" + edge.Type)
		serializedEdge, err := serialize(edge.Params)
		if err != nil {
			return fmt.Errorf("failed to serialize edge from %s to %s: %v", edge.From, edge.To, err)
		}
		batch.Set(edgeKey, serializedEdge, pebble.Sync)
	}

	return batch.Commit(pebble.Sync)
}

// RemoveNode removes a node and its associated edges
func (db *GraphDBPebble) RemoveNode(nodeID string) error {
	// Remove the node
	nodeKey := []byte("node:" + nodeID)
	err := db.db.Delete(nodeKey, pebble.NoSync)
	if err != nil {
		return fmt.Errorf("failed to remove node with ID %s: %v", nodeID, err)
	}

	// Remove edges related to the node
	iter, err := db.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return fmt.Errorf("failed to create iterator: %v", err)
	}
	defer iter.Close()

	// Collect and remove edges
	for iter.First(); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), []byte("edge:")) {
			break
		}
		key := iter.Key()
		parts := bytes.Split(key, []byte(":"))
		if string(parts[1]) == nodeID || string(parts[2]) == nodeID {
			if err := db.db.Delete(key, pebble.NoSync); err != nil {
				log.Printf("Failed to remove edge %s: %v", key, err)
			}
		}
	}

	return nil
}

// RemoveEdge removes a specific edge
func (db *GraphDBPebble) RemoveEdge(fromID, toID, edgeType string) error {
	edgeKey := []byte("edge:" + fromID + ":" + toID + ":" + edgeType)
	return db.db.Delete(edgeKey, pebble.NoSync)
}

// RemoveEdges removes multiple edges
func (db *GraphDBPebble) RemoveEdges(edges []graph.Edge) error {
	batch := db.db.NewBatch()
	for _, edge := range edges {
		edgeKey := []byte("edge:" + edge.From + ":" + edge.To + ":" + edge.Type)
		batch.Delete(edgeKey, pebble.NoSync)
	}
	return batch.Commit(pebble.NoSync)
}

// Traverse performs a depth-first search (DFS) on the graph starting from a node
func (db *GraphDBPebble) Traverse(nodeID string, dependencies map[string]bool, depth int) ([]graph.Node, []graph.Edge, error) {
	visited := make(map[string]bool)
	var resultNodes []graph.Node
	var resultEdges []graph.Edge
	stack := []string{nodeID}

	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// If we've exceeded the depth or already visited this node, skip it
		if visited[current] || depth <= 0 {
			continue
		}
		visited[current] = true

		// Fetch the node data
		nodeKey := []byte("node:" + current)
		data, closer, err := db.db.Get(nodeKey)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to retrieve node data for %s: %v", current, err)
		}
		defer closer.Close()

		var node graph.Node
		err = deserialize(data, &node)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to deserialize node data for %s: %v", current, err)
		}

		// Add the node to the result
		resultNodes = append(resultNodes, node)

		// Find the edges associated with this node and add the connected nodes to the stack
		iter, err := db.db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create iterator: %v", err)
		}
		defer iter.Close()

		for iter.First(); iter.Valid(); iter.Next() {
			if !bytes.HasPrefix(iter.Key(), []byte("edge:"+current+":")) {
				break
			}
			key := iter.Key()
			parts := bytes.Split(key, []byte(":"))
			if string(parts[1]) == current {
				toID := string(parts[2])
				stack = append(stack, toID)

				// Deserialize edge data
				edgeKey := []byte("edge:" + current + ":" + toID)
				edgeData, closer, err := db.db.Get(edgeKey)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to retrieve edge data for edge %s-%s: %v", current, toID, err)
				}
				defer closer.Close()

				var edge graph.Edge
				err = deserialize(edgeData, &edge)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to deserialize edge data: %v", err)
				}

				// Add the edge to the result
				resultEdges = append(resultEdges, edge)
			}
		}
	}

	// Return the list of nodes and edges
	return resultNodes, resultEdges, nil
}

// Close closes the Pebble database
func (db *GraphDBPebble) Close() error {
	return db.db.Close()
}
