package graph_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fgrzl/graph"
	"github.com/fgrzl/graph/pebble"
	"github.com/fgrzl/graph/sqlite"
	"github.com/stretchr/testify/assert"
)

var implementations = []string{"sqlite", "pebble"}

func getGraphDB(t *testing.T, dbType string) graph.GraphDB {
	var db graph.GraphDB
	var err error
	var dbPath string

	// Create a temporary directory for the test database
	tempDir := os.TempDir()

	// Determine the database type and initialize the appropriate DB
	switch dbType {
	case "sqlite":
		// Use a temporary file for SQLite DB
		dbPath = filepath.Join(tempDir, "test.db")
		// Initialize SQLite implementation
		db, err = sqlite.NewGraphDBSQLite(dbPath)
		if err != nil {
			t.Fatalf("Failed to initialize SQLite DB: %v", err)
		}
	case "pebble":
		// Use a temporary directory for Pebble DB
		dbPath = filepath.Join(tempDir, "test.pebble")
		// Initialize Pebble implementation
		db, err = pebble.NewGraphDBPebble(dbPath)
		if err != nil {
			t.Fatalf("Failed to initialize Pebble DB: %v", err)
		}
	default:
		t.Fatalf("Unknown database type: %s", dbType)
	}

	// Register the cleanup function to remove the database files after tests
	t.Cleanup(func() {
		// Cleanup logic based on the database type
		if dbType == "sqlite" {
			// Remove the SQLite test database file
			err := os.Remove(dbPath)
			if err != nil {
				t.Errorf("Failed to remove SQLite test database: %v", err)
			}
		} else if dbType == "pebble" {
			// Remove the Pebble test database directory
			err := os.RemoveAll(dbPath)
			if err != nil {
				t.Errorf("Failed to remove Pebble test database: %v", err)
			}
		}
	})

	return db
}

func TestPutNode(t *testing.T) {
	for _, dbType := range implementations {
		t.Run("TestPutNode - "+dbType, func(t *testing.T) {
			db := getGraphDB(t, dbType)
			defer db.Close()

			t.Run("should add a valid node when node ID is unique", func(t *testing.T) {
				node := graph.Node{ID: "1", Data: map[string]string{"name": "Node 1"}}
				err := db.PutNode(node.ID, node)
				assert.NoError(t, err, "PutNode should not return error when adding a valid node")
			})

			t.Run("should update an existing node when node ID already exists", func(t *testing.T) {
				node := graph.Node{ID: "1", Data: map[string]string{"name": "Node 1"}}
				_ = db.PutNode(node.ID, node) // Insert node for the first time
				updatedNode := graph.Node{ID: "1", Data: map[string]string{"name": "Updated Node 1"}}
				err := db.PutNode(updatedNode.ID, updatedNode) // Update the existing node
				assert.NoError(t, err, "PutNode should not return error when updating an existing node")
			})
		})
	}
}

func TestPutNodes(t *testing.T) {
	for _, dbType := range implementations {
		t.Run("TestPutNodes - "+dbType, func(t *testing.T) {
			db := getGraphDB(t, dbType)
			defer db.Close()

			t.Run("should add multiple valid nodes", func(t *testing.T) {
				nodes := []graph.Node{
					{ID: "1", Data: map[string]string{"name": "Node 1"}},
					{ID: "2", Data: map[string]string{"name": "Node 2"}},
				}
				err := db.PutNodes(nodes)
				assert.NoError(t, err, "PutNodes should not return error when adding multiple nodes")
			})

			t.Run("should update existing nodes when IDs are duplicated", func(t *testing.T) {
				nodes := []graph.Node{
					{ID: "1", Data: map[string]string{"name": "Node 1"}},
					{ID: "1", Data: map[string]string{"name": "Node 1 Updated"}}, // Duplicate ID
				}
				err := db.PutNodes(nodes)
				assert.NoError(t, err, "PutNodes should not return error when updating nodes with duplicate IDs")
			})
		})
	}
}

func TestPutEdge(t *testing.T) {
	for _, dbType := range implementations {
		t.Run("TestPutEdge - "+dbType, func(t *testing.T) {
			db := getGraphDB(t, dbType)
			defer db.Close()

			t.Run("should add a valid edge between two nodes", func(t *testing.T) {
				fromNode := "1"
				toNode := "2"
				edge := graph.Edge{From: fromNode, To: toNode, Type: "dependency", Params: map[string]string{"weight": "10"}}
				err := db.PutEdge(fromNode, toNode, edge.Type, edge.Params)
				assert.NoError(t, err, "PutEdge should not return error when adding a valid edge")
			})

			t.Run("should update an existing edge when edge already exists", func(t *testing.T) {
				fromNode := "1"
				toNode := "2"
				edge := graph.Edge{From: fromNode, To: toNode, Type: "dependency", Params: map[string]string{"weight": "10"}}
				_ = db.PutEdge(fromNode, toNode, edge.Type, edge.Params)                                                             // Insert edge for the first time
				updatedEdge := graph.Edge{From: fromNode, To: toNode, Type: "dependency", Params: map[string]string{"weight": "20"}} // Updated edge
				err := db.PutEdge(fromNode, toNode, updatedEdge.Type, updatedEdge.Params)                                            // Update the existing edge
				assert.NoError(t, err, "PutEdge should not return error when updating an existing edge")
			})
		})
	}
}

func TestPutEdges(t *testing.T) {
	for _, dbType := range implementations {
		t.Run("TestPutEdges - "+dbType, func(t *testing.T) {
			db := getGraphDB(t, dbType)
			defer db.Close()

			t.Run("should add multiple valid edges", func(t *testing.T) {
				edges := []graph.Edge{
					{From: "1", To: "2", Type: "dependency", Params: map[string]string{"weight": "10"}},
					{From: "2", To: "3", Type: "dependency", Params: map[string]string{"weight": "5"}},
				}
				err := db.PutEdges(edges)
				assert.NoError(t, err, "PutEdges should not return error when adding multiple edges")
			})

			t.Run("should update existing edges with the same connection", func(t *testing.T) {
				edges := []graph.Edge{
					{From: "1", To: "2", Type: "dependency", Params: map[string]string{"weight": "10"}},
					{From: "1", To: "2", Type: "dependency", Params: map[string]string{"weight": "15"}}, // Duplicate connection
				}
				err := db.PutEdges(edges)
				assert.NoError(t, err, "PutEdges should update existing edges with the same connection")
			})
		})
	}
}

func TestRemoveNode(t *testing.T) {
	for _, dbType := range implementations {
		t.Run("TestRemoveNode - "+dbType, func(t *testing.T) {
			db := getGraphDB(t, dbType)
			defer db.Close()

			t.Run("should remove an existing node", func(t *testing.T) {
				nodeID := "1"
				_ = db.PutNode(nodeID, graph.Node{ID: nodeID, Data: map[string]string{"name": "Node 1"}})
				err := db.RemoveNode(nodeID)
				assert.NoError(t, err, "RemoveNode should not return error when removing an existing node")
			})

			t.Run("should return an error when attempting to remove a nonexistent node", func(t *testing.T) {
				nodeID := "999" // Nonexistent node
				err := db.RemoveNode(nodeID)
				assert.Error(t, err, "RemoveNode should return an error when removing a nonexistent node")
			})
		})
	}
}

func TestRemoveEdge(t *testing.T) {
	for _, dbType := range implementations {
		t.Run("TestRemoveEdge - "+dbType, func(t *testing.T) {
			db := getGraphDB(t, dbType)
			defer db.Close()

			t.Run("should remove an existing edge", func(t *testing.T) {
				fromNode := "1"
				toNode := "2"
				_ = db.PutEdge(fromNode, toNode, "dependency", map[string]string{"weight": "10"})
				err := db.RemoveEdge(fromNode, toNode, "dependency")
				assert.NoError(t, err, "RemoveEdge should not return error when removing an existing edge")
			})

			t.Run("should return an error when attempting to remove a nonexistent edge", func(t *testing.T) {
				fromNode := "1"
				toNode := "2"
				err := db.RemoveEdge(fromNode, toNode, "dependency") // Nonexistent edge
				assert.Error(t, err, "RemoveEdge should return an error when removing a nonexistent edge")
			})
		})
	}
}

func TestRemoveEdges(t *testing.T) {
	for _, dbType := range implementations {
		t.Run("TestRemoveEdges - "+dbType, func(t *testing.T) {
			db := getGraphDB(t, dbType)
			defer db.Close()

			t.Run("should remove multiple edges", func(t *testing.T) {
				edges := []graph.Edge{
					{From: "1", To: "2", Type: "dependency", Params: map[string]string{"weight": "10"}},
					{From: "2", To: "3", Type: "dependency", Params: map[string]string{"weight": "5"}},
				}
				_ = db.PutEdges(edges)
				err := db.RemoveEdges(edges)
				assert.NoError(t, err, "RemoveEdges should not return error when removing multiple edges")
			})

			t.Run("should return an error when attempting to remove nonexistent edges", func(t *testing.T) {
				edges := []graph.Edge{
					{From: "999", To: "1000", Type: "dependency", Params: map[string]string{"weight": "10"}}, // Nonexistent edge
				}
				err := db.RemoveEdges(edges)
				assert.Error(t, err, "RemoveEdges should return an error when removing nonexistent edges")
			})
		})
	}
}

func TestTraverse(t *testing.T) {
	for _, dbType := range implementations {
		t.Run("TestTraverse - "+dbType, func(t *testing.T) {
			db := getGraphDB(t, dbType)
			defer db.Close()

			// Setup sample graph data
			db.PutNode("1", graph.Node{ID: "1", Data: map[string]string{"name": "Node 1"}})
			db.PutNode("2", graph.Node{ID: "2", Data: map[string]string{"name": "Node 2"}})
			db.PutEdge("1", "2", "dependency", map[string]string{"weight": "10"})

			t.Run("should traverse through nodes with given depth and dependencies", func(t *testing.T) {
				nodeID := "1"
				dependencies := map[string]bool{"2": true}
				depth := 3
				nodes, edges, err := db.Traverse(nodeID, dependencies, depth)
				assert.NoError(t, err, "Traverse should not return error")
				assert.Len(t, nodes, 2, "Should return 2 nodes in traversal")
				assert.Len(t, edges, 1, "Should return 1 edge in traversal")
			})

			t.Run("should handle traversing starting from a nonexistent node", func(t *testing.T) {
				nodeID := "999" // Nonexistent node
				dependencies := map[string]bool{"2": true}
				depth := 3
				nodes, edges, err := db.Traverse(nodeID, dependencies, depth)
				assert.Error(t, err, "Traverse should return error for nonexistent node")
				assert.Len(t, nodes, 0, "Should return 0 nodes for nonexistent node")
				assert.Len(t, edges, 0, "Should return 0 edges for nonexistent node")
			})
		})
	}
}

func TestClose(t *testing.T) {
	for _, dbType := range implementations {
		t.Run("TestClose - "+dbType, func(t *testing.T) {
			db := getGraphDB(t, dbType)
			defer db.Close()

			t.Run("should close the graph without errors", func(t *testing.T) {
				err := db.Close()
				assert.NoError(t, err, "Close should not return error when closing the graph DB")
			})

			t.Run("should close the graph after operations are performed", func(t *testing.T) {
				_ = db.PutNode("1", graph.Node{ID: "1", Data: map[string]string{"name": "Node 1"}})
				err := db.Close()
				assert.NoError(t, err, "Close should not return error after operations")
			})
		})
	}
}
