package graph_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/fgrzl/graph"
	"github.com/fgrzl/graph/pebble"
	"github.com/fgrzl/graph/sqlite"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

var implementations = []string{"pebble", "sqlite"}

func getGraphDB(t *testing.T, dbType string) graph.Graph {
	var db graph.Graph
	var err error
	var dbPath string

	// Create a temporary directory for the test database
	tempDir := os.TempDir()

	// Determine the database type and initialize the appropriate DB
	switch dbType {
	case "sqlite":
		// Use a temporary file for SQLite DB
		dbPath = filepath.Join(tempDir, fmt.Sprintf("db_%v.sqlite", uuid.NewString()))
		// Initialize SQLite implementation
		db, err = sqlite.NewSqliteGraph(dbPath)
		if err != nil {
			t.Fatalf("Failed to initialize SQLite DB: %v", err)
		}
	case "pebble":
		// Use a temporary directory for Pebble DB
		dbPath = filepath.Join(tempDir, fmt.Sprintf("db_%v.pebble", uuid.NewString()))
		// Initialize Pebble implementation
		db, err = pebble.NewPebbleGraph(dbPath)
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
				node := graph.Node{ID: "1", Data: []byte("Node 1")}
				err := db.PutNode(node)
				assert.NoError(t, err, "PutNode should not return error when adding a valid node")
			})

			t.Run("should update an existing node when node ID already exists", func(t *testing.T) {
				node := graph.Node{ID: "1", Data: []byte("Node 1")}
				_ = db.PutNode(node) // Insert node for the first time
				updatedNode := graph.Node{ID: "1", Data: []byte("Updated Node 1")}
				err := db.PutNode(updatedNode) // Update the existing node
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
					{ID: "1", Data: []byte("Node 1")},
					{ID: "2", Data: []byte("Node 2")},
				}
				err := db.PutNodes(nodes)
				assert.NoError(t, err, "PutNodes should not return error when adding multiple nodes")
			})

			t.Run("should update existing nodes when IDs are duplicated", func(t *testing.T) {
				nodes := []graph.Node{
					{ID: "1", Data: []byte("Node 1")},
					{ID: "1", Data: []byte("Node 1")}, // Duplicate ID
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
				edge := graph.Edge{From: fromNode, To: toNode, Type: "dependency"}
				err := db.PutEdge(edge)
				assert.NoError(t, err, "PutEdge should not return error when adding a valid edge")
			})

			t.Run("should update an existing edge when edge already exists", func(t *testing.T) {
				fromNode := "1"
				toNode := "2"
				edge := graph.Edge{From: fromNode, To: toNode, Type: "dependency"}
				_ = db.PutEdge(edge)
				updatedEdge := graph.Edge{From: fromNode, To: toNode, Type: "dependency"}
				err := db.PutEdge(updatedEdge)
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
					{From: "1", To: "2", Type: "dependency"},
					{From: "2", To: "3", Type: "dependency"},
				}
				err := db.PutEdges(edges)
				assert.NoError(t, err, "PutEdges should not return error when adding multiple edges")
			})

			t.Run("should update existing edges with the same connection", func(t *testing.T) {
				edges := []graph.Edge{
					{From: "1", To: "2", Type: "dependency"},
					{From: "1", To: "2", Type: "dependency"},
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
				_ = db.PutNode(graph.Node{ID: nodeID, Data: []byte("Node 1")})
				err := db.RemoveNode(nodeID)
				assert.NoError(t, err, "RemoveNode should not return error when removing an existing node")
			})
		})
	}
}

func TestRemoveNodes(t *testing.T) {
	for _, dbType := range implementations {
		t.Run("TestRemoveNodes - "+dbType, func(t *testing.T) {
			db := getGraphDB(t, dbType)
			defer db.Close()

			t.Run("should remove multiple existing nodes", func(t *testing.T) {
				nodes := []graph.Node{
					{ID: "1", Data: []byte("Node 1")},
					{ID: "2", Data: []byte("Node 2")},
				}
				// Add nodes first
				err := db.PutNodes(nodes)
				assert.NoError(t, err, "PutNodes should not return error")

				// Remove nodes
				nodeIDs := make([]string, len(nodes))
				for i, node := range nodes {
					nodeIDs[i] = node.ID
				}
				err = db.RemoveNodes(nodeIDs...)
				assert.NoError(t, err, "RemoveNodes should not return error when removing existing nodes")

				// Verify nodes are removed
				for _, node := range nodes {
					_, err := db.GetNode(node.ID)
					assert.Error(t, err, "GetNode should return error for removed node")
				}
			})

			t.Run("should not return an error when trying to remove nonexistent nodes", func(t *testing.T) {
				nodes := []graph.Node{
					{ID: "3", Data: []byte("Nonexistent Node")},
				}
				err := db.RemoveNodes(nodes[0].ID)
				assert.NoError(t, err, "RemoveNodes should not return error for nonexistent nodes")
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
				edge := graph.Edge{From: fromNode, To: toNode, Type: "dependency"}
				_ = db.PutEdge(edge)
				err := db.RemoveEdge(edge)
				assert.NoError(t, err, "RemoveEdge should not return error when removing an existing edge")
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
					{From: "1", To: "2", Type: "dependency"},
					{From: "2", To: "3", Type: "dependency"},
				}
				_ = db.PutEdges(edges)
				err := db.RemoveEdges(edges)
				assert.NoError(t, err, "RemoveEdges should not return error when removing multiple edges")
			})
		})
	}
}

func TestTraverse(t *testing.T) {
	for _, dbType := range implementations {
		t.Run("Traverse - "+dbType, func(t *testing.T) {
			db := getGraphDB(t, dbType)
			defer db.Close()

			// Arrange
			// Set up some nodes and edges
			node1 := graph.Node{ID: "1", Data: []byte("Node 1")}
			node2 := graph.Node{ID: "2", Data: []byte("Node 2")}
			node3 := graph.Node{ID: "3", Data: []byte("Node 3")}
			node4 := graph.Node{ID: "4", Data: []byte("Node 4")}
			node5 := graph.Node{ID: "5", Data: []byte("Node 5")}
			_ = db.PutNode(node1)
			_ = db.PutNode(node2)
			_ = db.PutNode(node3)
			_ = db.PutNode(node4)
			_ = db.PutNode(node5)
			_ = db.PutEdge(graph.Edge{From: node1.ID, To: node2.ID, Type: "dependency"})
			_ = db.PutEdge(graph.Edge{From: node2.ID, To: node3.ID, Type: "dependency"})
			_ = db.PutEdge(graph.Edge{From: node3.ID, To: node4.ID, Type: "dependency"})
			_ = db.PutEdge(graph.Edge{From: node4.ID, To: node5.ID, Type: "dependency"})

			t.Run("should traverse nodes within the given depth and dependencies", func(t *testing.T) {
				// Arrange
				dependencies := graph.EdgeTypes{}
				depth := 9999

				// Act
				nodes, edges, err := db.Traverse("1", dependencies, depth)

				// Assert
				assert.NoError(t, err, "Traverse should not return error")
				assert.Len(t, nodes, 5, "Should return 5 nodes in traversal")
				assert.Len(t, edges, 4, "Should return 1 edge in traversal")

				assert.Equal(t, "1", nodes[0].ID, "The second node should be Node 1")
				assert.Equal(t, "2", nodes[1].ID, "The first node should be Node 2")
				assert.Equal(t, "3", nodes[2].ID, "The first node should be Node 3")
				assert.Equal(t, "4", nodes[3].ID, "The first node should be Node 4")
				assert.Equal(t, "5", nodes[4].ID, "The first node should be Node 5")

				assert.Equal(t, "1", edges[0].From, "The second node should be Node 1")
				assert.Equal(t, "2", edges[0].To, "The second node should be Node 1")

				assert.Equal(t, "2", edges[1].From, "The first node should be Node 2")
				assert.Equal(t, "3", edges[1].To, "The first node should be Node 2")

				assert.Equal(t, "3", edges[2].From, "The first node should be Node 3")
				assert.Equal(t, "4", edges[2].To, "The first node should be Node 4")

				assert.Equal(t, "4", edges[3].From, "The first node should be Node 4")
				assert.Equal(t, "5", edges[3].To, "The first node should be Node 5")
			})

			t.Run("should return no error if the start node does not exist", func(t *testing.T) {
				// Arrange
				dependencies := graph.EdgeTypes{}
				depth := 3

				// Act
				nodes, edges, err := db.Traverse("999", dependencies, depth)

				// Assert
				assert.NoError(t, err, "Traverse should no error for nonexistent node")
				assert.Len(t, nodes, 0, "Should return 0 nodes for nonexistent node")
				assert.Len(t, edges, 0, "Should return 0 edges for nonexistent node")
			})

			t.Run("should handle no dependencies", func(t *testing.T) {
				// Arrange
				dependencies := graph.EdgeTypes{}
				depth := 3

				// Act
				nodes, edges, err := db.Traverse("5", dependencies, depth)

				// Assert
				assert.NoError(t, err, "Traverse should not return error even with no dependencies")
				assert.Len(t, nodes, 1, "Should return only the start node when no dependencies are set")
				assert.Len(t, edges, 0, "Should return 0 edges when no dependencies are set")
			})

			t.Run("should respect depth in traversal", func(t *testing.T) {
				// Arrange
				dependencies := graph.EdgeTypes{}
				depth := 1 // Limit the depth to 1

				// Act
				nodes, edges, err := db.Traverse("1", dependencies, depth)

				// Assert
				assert.NoError(t, err, "Traverse should not return error")
				assert.Len(t, nodes, 2, "Should return only 1 node at depth 1")
				assert.Len(t, edges, 1, "Should return 0 edges at depth 1, as no edges are allowed with depth 1")
			})

			t.Run("should handle cyclic dependencies correctly", func(t *testing.T) {
				// Arrange
				node4 := graph.Node{ID: "4", Data: []byte("Node 4")}
				_ = db.PutNode(node4)
				_ = db.PutEdge(graph.Edge{From: node3.ID, To: node4.ID, Type: "dependency"})
				_ = db.PutEdge(graph.Edge{From: node4.ID, To: node1.ID, Type: "dependency"})

				dependencies := graph.EdgeTypes{}
				depth := 3

				// Act
				nodes, edges, err := db.Traverse("1", dependencies, depth)

				// Assert
				assert.NoError(t, err, "Traverse should not return error")
				assert.Len(t, nodes, 4, "Should return 4 nodes when there is a cycle")
				assert.Len(t, edges, 3, "Should return 3 edges due to cyclic connections")
			})
		})
	}
}

func TestClose(t *testing.T) {
	for _, dbType := range implementations {
		t.Run("TestClose - "+dbType, func(t *testing.T) {
			db := getGraphDB(t, dbType)
			//defer db.Close()

			t.Run("should close the graph without errors", func(t *testing.T) {
				err := db.Close()
				assert.NoError(t, err, "Close should not return error when closing the graph DB")
			})
		})
	}
}
