package pebble

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/fgrzl/graph"
)

type GraphDBPebble struct {
	db *pebble.DB
}

func NewGraphDBPebble(dbPath string) (graph.GraphDB, error) {
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("could not open Pebble database: %v", err)
	}
	return &GraphDBPebble{db: db}, nil
}

func serialize(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

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

// PutEdge inserts or updates an edge in the graph, supporting bidirectional lookups
func (db *GraphDBPebble) PutEdge(fromID, toID, edgeType string, params map[string]string) error {
	// Forward edge key
	edgeKey := []byte("edge:" + fromID + ":" + toID + ":" + edgeType)
	serializedEdge, err := serialize(params)
	if err != nil {
		return fmt.Errorf("failed to serialize edge from %s to %s: %v", fromID, toID, err)
	}

	// Set the forward edge
	err = db.db.Set(edgeKey, serializedEdge, pebble.Sync)
	if err != nil {
		return fmt.Errorf("failed to put forward edge from %s to %s: %v", fromID, toID, err)
	}

	// Reverse edge key
	reverseEdgeKey := []byte("edge:" + toID + ":" + fromID + ":" + edgeType)
	err = db.db.Set(reverseEdgeKey, serializedEdge, pebble.Sync)
	if err != nil {
		return fmt.Errorf("failed to put reverse edge from %s to %s: %v", toID, fromID, err)
	}

	return nil
}

// PutEdges inserts or updates multiple edges in the graph, supporting bidirectional lookups
func (db *GraphDBPebble) PutEdges(edges []graph.Edge) error {
	batch := db.db.NewBatch()
	for _, edge := range edges {
		// Forward edge
		edgeKey := []byte("edge:" + edge.From + ":" + edge.To + ":" + edge.Type)
		serializedEdge, err := serialize(edge.Params)
		if err != nil {
			return fmt.Errorf("failed to serialize edge from %s to %s: %v", edge.From, edge.To, err)
		}
		batch.Set(edgeKey, serializedEdge, pebble.Sync)

		// Reverse edge
		reverseEdgeKey := []byte("edge:" + edge.To + ":" + edge.From + ":" + edge.Type)
		batch.Set(reverseEdgeKey, serializedEdge, pebble.Sync)
	}
	return batch.Commit(pebble.Sync)
}

// RemoveNode removes a node and its associated edges, supporting bidirectional edge cleanup
func (db *GraphDBPebble) RemoveNode(id string) error {
	batch := db.db.NewBatch()

	// Remove the node
	nodeKey := []byte("node:" + id)
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
func (db *GraphDBPebble) RemoveNodes(ids ...string) error {
	batch := db.db.NewBatch()

	for _, nodeID := range ids {
		// Remove the node
		nodeKey := []byte("node:" + nodeID)
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
func (db *GraphDBPebble) RemoveEdge(fromID, toID, edgeType string) error {
	// Forward edge key
	edgeKey := []byte("edge:" + fromID + ":" + toID + ":" + edgeType)
	err := db.db.Delete(edgeKey, pebble.NoSync)
	if err != nil {
		return fmt.Errorf("failed to delete forward edge from %s to %s: %v", fromID, toID, err)
	}

	// Reverse edge key
	reverseEdgeKey := []byte("edge:" + toID + ":" + fromID + ":" + edgeType)
	err = db.db.Delete(reverseEdgeKey, pebble.NoSync)
	if err != nil {
		return fmt.Errorf("failed to delete reverse edge from %s to %s: %v", toID, fromID, err)
	}

	return nil
}

// RemoveEdges removes multiple edges, including their reverse counterparts
func (db *GraphDBPebble) RemoveEdges(edges []graph.Edge) error {
	batch := db.db.NewBatch()
	for _, edge := range edges {
		// Forward edge
		edgeKey := []byte("edge:" + edge.From + ":" + edge.To + ":" + edge.Type)
		batch.Delete(edgeKey, pebble.NoSync)

		// Reverse edge
		reverseEdgeKey := []byte("edge:" + edge.To + ":" + edge.From + ":" + edge.Type)
		batch.Delete(reverseEdgeKey, pebble.NoSync)
	}
	return batch.Commit(pebble.NoSync)
}

// GetNode retrieves a node by ID
func (db *GraphDBPebble) GetNode(id string) (graph.Node, error) {
	nodeKey := []byte("node:" + id)
	data, closer, err := db.db.Get(nodeKey)
	if err != nil {
		return graph.Node{}, fmt.Errorf("failed to retrieve node data for %s: %v", id, err)
	}
	defer closer.Close()

	var node graph.Node
	err = deserialize(data, &node)
	if err != nil {
		return graph.Node{}, fmt.Errorf("failed to deserialize node data for %s: %v", id, err)
	}

	return node, nil
}

// GetEdge retrieves an edge by the from and to node IDs and edge type
func (db *GraphDBPebble) GetEdge(fromID, toID, edgeType string) (graph.Edge, error) {
	edgeKey := []byte("edge:" + fromID + ":" + toID + ":" + edgeType)
	data, closer, err := db.db.Get(edgeKey)
	if err != nil {
		return graph.Edge{}, fmt.Errorf("failed to retrieve edge data for %s-%s: %v", fromID, toID, err)
	}
	defer closer.Close()

	var edge graph.Edge
	err = deserialize(data, &edge)
	if err != nil {
		return graph.Edge{}, fmt.Errorf("failed to deserialize edge data for %s-%s: %v", fromID, toID, err)
	}

	return edge, nil
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

		resultNodes = append(resultNodes, node)

		// Collect related edges
		iter, err := db.db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, nil, err
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

				// Retrieve edge data
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

				resultEdges = append(resultEdges, edge)
			}
		}
	}

	return resultNodes, resultEdges, nil
}

func (db *GraphDBPebble) Close() error {
	return db.db.Close()
}
