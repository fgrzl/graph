package sqlite

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/fgrzl/graph"
	_ "github.com/mattn/go-sqlite3"
)

type GraphDBSQLite struct {
	db *sql.DB
}

func NewGraphDBSQLite(dbPath string) (graph.GraphDB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("could not open SQLite database: %v", err)
	}

	// Create the schema if it doesn't exist
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS nodes (
		id TEXT PRIMARY KEY,
		data TEXT
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

	return &GraphDBSQLite{db: db}, nil
}

// PutNode inserts or updates a node
func (db *GraphDBSQLite) PutNode(id string, node graph.Node) error {
	data := fmt.Sprintf("%v", node.Data)

	_, err := db.db.Exec(`
		INSERT OR REPLACE INTO nodes (id, data)
		VALUES (?, ?);
	`, id, data)
	return err
}

// PutNodes inserts or updates multiple nodes
func (db *GraphDBSQLite) PutNodes(nodes []graph.Node) error {
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
func (db *GraphDBSQLite) PutEdge(fromID, toID, edgeType string, params map[string]string) error {
	paramsStr := fmt.Sprintf("%v", params)

	_, err := db.db.Exec(`
		INSERT OR REPLACE INTO edges (from_id, to_id, type, params)
		VALUES (?, ?, ?, ?);
	`, fromID, toID, edgeType, paramsStr)
	return err
}

// PutEdges inserts or updates multiple edges
func (db *GraphDBSQLite) PutEdges(edges []graph.Edge) error {
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
func (db *GraphDBSQLite) RemoveNode(nodeID string) error {
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
func (db *GraphDBSQLite) RemoveNodes(ids ...string) error {
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
func (db *GraphDBSQLite) RemoveEdge(fromID, toID, edgeType string) error {
	_, err := db.db.Exec("DELETE FROM edges WHERE from_id = ? AND to_id = ? AND type = ?", fromID, toID, edgeType)
	return err
}

// RemoveEdges removes multiple edges
func (db *GraphDBSQLite) RemoveEdges(edges []graph.Edge) error {
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
func (db *GraphDBSQLite) GetNode(id string) (graph.Node, error) {
	var data string
	err := db.db.QueryRow("SELECT id, data FROM nodes WHERE id = ?", id).Scan(&id, &data)
	if err != nil {
		return graph.Node{}, err
	}

	return graph.Node{ID: id, Data: map[string]string{"data": data}}, nil
}

// GetEdge retrieves an edge by the from and to node IDs and the edge type
func (db *GraphDBSQLite) GetEdge(fromID, toID, edgeType string) (graph.Edge, error) {
	var params string
	err := db.db.QueryRow("SELECT from_id, to_id, type, params FROM edges WHERE from_id = ? AND to_id = ? AND type = ?", fromID, toID, edgeType).Scan(&fromID, &toID, &edgeType, &params)
	if err != nil {
		return graph.Edge{}, err
	}

	return graph.Edge{From: fromID, To: toID, Type: edgeType, Params: map[string]string{"params": params}}, nil
}

func (db *GraphDBSQLite) Traverse(nodeID string, dependencies map[string]bool, depth int) ([]graph.Node, []graph.Edge, error) {
	// Start by creating the recursive CTE
	// This will allow traversing nodes up to the specified depth
	query := `
WITH RECURSIVE traverse(id, depth, from_id, to_id, edge_type, edge_params) AS (
    -- Anchor member: start from the initial node
    SELECT e.from_id, 0, e.from_id, e.to_id, e.type, e.params
    FROM edges e
    WHERE e.from_id = ?

    UNION ALL

    -- Recursive member: traverse from the connected nodes
    SELECT e.from_id, t.depth + 1, e.from_id, e.to_id, e.type, e.params
    FROM edges e
    JOIN traverse t ON t.to_id = e.from_id
    WHERE t.depth < ? AND e.to_id IN (SELECT id FROM nodes WHERE id IN (?))
)
-- Select both the nodes and edges
SELECT DISTINCT
    n.id, n.data, t.from_id, t.to_id, t.edge_type, t.edge_params
FROM traverse t
-- Include the starting node (anchor member)
JOIN nodes n ON n.id = t.to_id OR n.id = t.from_id
ORDER BY t.depth, n.id;
	`

	// Prepare the query arguments
	args := []interface{}{nodeID, depth, nodeID}

	// If there are dependencies, include them in the query
	if len(dependencies) > 0 {
		dependencyKeys := make([]string, 0, len(dependencies))
		for dep := range dependencies {
			dependencyKeys = append(dependencyKeys, dep)
		}
		args[2] = strings.Join(dependencyKeys, ",")
	}

	// Execute the query
	rows, err := db.db.Query(query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to execute traverse query: %v", err)
	}
	defer rows.Close()

	// Process the results
	var resultNodes []graph.Node
	var resultEdges []graph.Edge
	seenEdges := make(map[string]bool)

	// Fetch the nodes and edges from the result
	for rows.Next() {
		var nodeID, nodeData, fromID, toID, edgeType, edgeParams string
		if err := rows.Scan(&nodeID, &nodeData, &fromID, &toID, &edgeType, &edgeParams); err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %v", err)
		}

		// Add nodes to the result
		resultNodes = append(resultNodes, graph.Node{ID: nodeID, Data: map[string]string{"data": nodeData}})

		// Add edges only if they have not been seen before
		if fromID != "" && toID != "" {
			edgeKey := fmt.Sprintf("%s->%s", fromID, toID)
			if !seenEdges[edgeKey] {
				resultEdges = append(resultEdges, graph.Edge{
					From:   fromID,
					To:     toID,
					Type:   edgeType,
					Params: map[string]string{"params": edgeParams},
				})
				seenEdges[edgeKey] = true
			}
		}
	}

	// Check for errors in the rows iteration
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("failed to iterate rows: %v", err)
	}

	return resultNodes, resultEdges, nil
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
func (db *GraphDBSQLite) Close() error {
	return db.db.Close()
}
