package sqlite

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/fgrzl/graph"
	_ "github.com/mattn/go-sqlite3"
)

type sqliteGraph struct {
	db *sql.DB
}

func NewSqliteGraph(dbPath string) (graph.Graph, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("could not open SQLite database: %v", err)
	}

	// Create the schema if it doesn't exist
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS nodes (
		id TEXT PRIMARY KEY,
		data BYTES
	);

	CREATE TABLE IF NOT EXISTS edges (
		from_id TEXT,
		to_id TEXT,
		type TEXT,
		PRIMARY KEY (from_id, to_id, type)
	);

	CREATE INDEX IF NOT EXISTS idx_edges_to_id_type_from_id ON edges (to_id, type, from_id);
	`)
	if err != nil {
		return nil, fmt.Errorf("could not create tables: %v", err)
	}

	return &sqliteGraph{db: db}, nil
}

// PutNode inserts or updates a node
func (g *sqliteGraph) PutNode(node graph.Node) error {
	_, err := g.db.Exec(`INSERT OR REPLACE INTO nodes (id, data) VALUES (?, ?);`, node.ID, node.Data)
	return err
}

// PutNodes inserts or updates multiple nodes
func (g *sqliteGraph) PutNodes(nodes []graph.Node) error {
	tx, err := g.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, node := range nodes {
		err = g.PutNode(node)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// PutEdge inserts or updates an edge
func (g *sqliteGraph) PutEdge(edge graph.Edge) error {

	_, err := g.db.Exec(`
		INSERT OR REPLACE INTO edges (from_id, to_id, type)
		VALUES (?, ?, ?);
	`, edge.From, edge.To, edge.Type)
	return err
}

// PutEdges inserts or updates multiple edges
func (g *sqliteGraph) PutEdges(edges []graph.Edge) error {
	tx, err := g.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, edge := range edges {
		err = g.PutEdge(edge)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// RemoveNode removes a node and its associated edges
func (g *sqliteGraph) RemoveNode(nodeID string) error {
	tx, err := g.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Remove all edges involving the node
	_, err = tx.Exec("DELETE FROM edges WHERE from_id = ? OR to_id = ?", nodeID, nodeID)
	if err != nil {
		return err
	}

	// Remove the node itself
	_, err = tx.Exec("DELETE FROM nodes WHERE id = ?", nodeID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// RemoveNodes removes multiple nodes and their associated edges
func (g *sqliteGraph) RemoveNodes(ids ...string) error {
	tx, err := g.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Prepare a list of node IDs to remove
	nodeIDs := make([]interface{}, len(ids))
	placeholders := make([]string, len(ids))
	for i, id := range ids {
		nodeIDs[i] = id
		placeholders[i] = "?"
	}

	// Remove edges involving the nodes
	edgesQuery := fmt.Sprintf(
		"DELETE FROM edges WHERE from_id IN (%s) OR to_id IN (%s)",
		strings.Join(placeholders, ","), strings.Join(placeholders, ","))
	_, err = tx.Exec(edgesQuery, append(nodeIDs, nodeIDs...)...)
	if err != nil {
		return fmt.Errorf("failed to remove edges: %w", err)
	}

	// Remove the nodes
	nodesQuery := fmt.Sprintf(
		"DELETE FROM nodes WHERE id IN (%s)", strings.Join(placeholders, ","))
	_, err = tx.Exec(nodesQuery, nodeIDs...)
	if err != nil {
		return fmt.Errorf("failed to remove nodes: %w", err)
	}

	// Commit the transaction
	return tx.Commit()
}

// RemoveEdge removes a specific edge
func (g *sqliteGraph) RemoveEdge(edge graph.Edge) error {
	_, err := g.db.Exec("DELETE FROM edges WHERE from_id = ? AND to_id = ? AND type = ?", edge.From, edge.To, edge.Type)
	return err
}

// RemoveEdges removes multiple edges
func (g *sqliteGraph) RemoveEdges(edges []graph.Edge) error {
	tx, err := g.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, edge := range edges {
		err = g.RemoveEdge(edge)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetNode retrieves a node by ID
func (db *sqliteGraph) GetNode(id string) (graph.Node, error) {
	var data []byte
	err := db.db.QueryRow("SELECT id, data FROM nodes WHERE id = ?", id).Scan(&id, &data)
	if err != nil {
		return graph.Node{}, err
	}

	return graph.Node{ID: id, Data: data}, nil
}

func (g *sqliteGraph) Traverse(nodeID string, edgeTypes graph.EdgeTypes, depth int) ([]graph.Node, []graph.Edge, error) {
	// Start by creating the recursive CTE
	// This will allow traversing nodes up to the specified depth

	// todo : the query needs to filter by edge type

	queryEdges := `
-- Insert the results of the recursive query into the temporary table
WITH RECURSIVE _e AS (
    -- Base case: start with the initial node
    SELECT 
        e.from_id, 
        e.to_id, 
        e.type, 
        '/' || e.from_id || '/' AS path, -- Initialize the path
        1 AS depth
    FROM edges e
    WHERE e.from_id = ?

    UNION ALL

    -- Recursive case: expand paths deeper, avoiding cycles
    SELECT 
        e.from_id, 
        e.to_id, 
        e.type, 
        _e.path || e.to_id || '/', -- Extend the path
        _e.depth + 1 AS depth
    FROM edges e
    INNER JOIN _e ON e.from_id = _e.to_id
    WHERE _e.depth < ? -- Depth limit
      AND instr(_e.path, '/' || e.to_id || '/') = 0 -- Avoid revisiting nodes in the path
)
SELECT 
    _e.from_id, 
    _e.to_id, 
    _e.type
FROM _e;
`

	// Prepare the query arguments
	args := []interface{}{nodeID, depth}

	// Execute the query
	nodeIDs := make(map[string]struct{})
	nodeIDs[nodeID] = struct{}{}

	var resultEdges []graph.Edge
	rows, err := g.db.Query(queryEdges, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query edges: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var from, to, edgeType string
		if err := rows.Scan(&from, &to, &edgeType); err != nil {
			return nil, nil, fmt.Errorf("failed to scan edge %v", err)
		}
		resultEdges = append(resultEdges, graph.Edge{
			From: from,
			To:   to,
			Type: edgeType,
		})
		nodeIDs[from] = struct{}{}
		nodeIDs[to] = struct{}{}
	}

	// Check for errors in the rows iteration
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("failed to iterate edges: %v", err)
	}

	resultNodes, err := g.queryNodesInBatches(nodeIDs)
	if err != nil {
		return nil, nil, err
	}

	return resultNodes, resultEdges, nil
}

func (g *sqliteGraph) queryNodesInBatches(nodeIDs map[string]struct{}) ([]graph.Node, error) {
	var resultNodes []graph.Node
	keys := make([]string, 0, len(nodeIDs))
	for key := range nodeIDs {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Function to chunk keys into batches of 999
	chunkSize := 999
	for i := 0; i < len(keys); i += chunkSize {
		// Get the chunk of keys
		end := i + chunkSize
		if end > len(keys) {
			end = len(keys)
		}
		chunk := keys[i:end]

		// Build the query with the chunk of keys
		placeholder := strings.Join(chunk, "\", \"")
		queryNodes := `SELECT n.id, n.data FROM nodes n WHERE n.id IN ("` + placeholder + `") ORDER BY n.id`

		// Execute the query
		rows, err := g.db.Query(queryNodes)
		if err != nil {
			return nil, fmt.Errorf("failed to query nodes: %v", err)
		}
		defer rows.Close()

		// Process the results
		for rows.Next() {
			var node graph.Node
			if err := rows.Scan(&node.ID, &node.Data); err != nil {
				return nil, fmt.Errorf("failed to scan row: %v", err)
			}
			resultNodes = append(resultNodes, node)
		}

		// Check for errors after iterating over rows
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("row iteration error: %v", err)
		}
	}

	return resultNodes, nil
}

// Helper function to get dependency keys as a list of strings
func getDependencyKeys(dependencies map[string]bool) []string {
	var keys []string
	for key := range dependencies {
		keys = append(keys, key)
	}
	return keys
}

// Close closes the database connection
func (db *sqliteGraph) Close() error {
	return db.db.Close()
}
