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

func NewSqliteGraph(dbPath string) (graph.GraphDB, error) {
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
		params TEXT,
		PRIMARY KEY (from_id, to_id, type)
	);
	`)
	if err != nil {
		return nil, fmt.Errorf("could not create tables: %v", err)
	}

	return &sqliteGraph{db: db}, nil
}

// PutNode inserts or updates a node
func (db *sqliteGraph) PutNode(id string, node graph.Node) error {

	_, err := db.db.Exec(`
		INSERT OR REPLACE INTO nodes (id, data)
		VALUES (?, ?);
	`, id, node.Data)
	return err
}

// PutNodes inserts or updates multiple nodes
func (db *sqliteGraph) PutNodes(nodes []graph.Node) error {
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, node := range nodes {
		err = db.PutNode(node.ID, node)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// PutEdge inserts or updates an edge
func (db *sqliteGraph) PutEdge(fromID, toID, edgeType string, params map[string]string) error {
	paramsStr := fmt.Sprintf("%v", params)

	_, err := db.db.Exec(`
		INSERT OR REPLACE INTO edges (from_id, to_id, type, params)
		VALUES (?, ?, ?, ?);
	`, fromID, toID, edgeType, paramsStr)
	return err
}

// PutEdges inserts or updates multiple edges
func (db *sqliteGraph) PutEdges(edges []graph.Edge) error {
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, edge := range edges {
		err = db.PutEdge(edge.From, edge.To, edge.Type, edge.Params)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// RemoveNode removes a node and its associated edges
func (db *sqliteGraph) RemoveNode(nodeID string) error {
	tx, err := db.db.Begin()
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
func (db *sqliteGraph) RemoveNodes(ids ...string) error {
	tx, err := db.db.Begin()
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
func (db *sqliteGraph) RemoveEdge(fromID, toID, edgeType string) error {
	_, err := db.db.Exec("DELETE FROM edges WHERE from_id = ? AND to_id = ? AND type = ?", fromID, toID, edgeType)
	return err
}

// RemoveEdges removes multiple edges
func (db *sqliteGraph) RemoveEdges(edges []graph.Edge) error {
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, edge := range edges {
		err = db.RemoveEdge(edge.From, edge.To, edge.Type)
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

// GetEdge retrieves an edge by the from and to node IDs and the edge type
func (db *sqliteGraph) GetEdge(fromID, toID, edgeType string) (graph.Edge, error) {
	var params string
	err := db.db.QueryRow("SELECT from_id, to_id, type, params FROM edges WHERE from_id = ? AND to_id = ? AND type = ?", fromID, toID, edgeType).Scan(&fromID, &toID, &edgeType, &params)
	if err != nil {
		return graph.Edge{}, err
	}

	return graph.Edge{From: fromID, To: toID, Type: edgeType, Params: map[string]string{"params": params}}, nil
}

func (db *sqliteGraph) Traverse(nodeID string, dependencies map[string]bool, depth int) ([]graph.Node, []graph.Edge, error) {
	// Start by creating the recursive CTE
	// This will allow traversing nodes up to the specified depth

	queryEdges := `
-- Insert the results of the recursive query into the temporary table
WITH RECURSIVE _e AS (
    -- Base case: start with the initial node
    SELECT 
        e.from_id, 
        e.to_id, 
        e.type, 
        e.params,
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
        e.params,
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
    _e.type, 
    _e.params
FROM _e;
`

	// Prepare the query arguments
	args := []interface{}{nodeID, depth}

	// Execute the query
	nodeIDs := make(map[string]struct{})
	nodeIDs[nodeID] = struct{}{}

	var resultEdges []graph.Edge
	rows, err := db.db.Query(queryEdges, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query edges: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fromID, toID, edgeType, edgeParams string
		if err := rows.Scan(&fromID, &toID, &edgeType, &edgeParams); err != nil {
			return nil, nil, fmt.Errorf("failed to scan edge %v", err)
		}
		resultEdges = append(resultEdges, graph.Edge{
			From:   fromID,
			To:     toID,
			Type:   edgeType,
			Params: map[string]string{"params": edgeParams},
		})
		nodeIDs[fromID] = struct{}{}
		nodeIDs[toID] = struct{}{}
	}

	// Check for errors in the rows iteration
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("failed to iterate edges: %v", err)
	}

	resultNodes, err := db.queryNodesInBatches(nodeIDs)
	if err != nil {
		return nil, nil, err
	}

	return resultNodes, resultEdges, nil
}

func (db *sqliteGraph) queryNodesInBatches(nodeIDs map[string]struct{}) ([]graph.Node, error) {
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
		rows, err := db.db.Query(queryNodes)
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
