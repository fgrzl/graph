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

// Traverse traverses the graph from a starting node, respecting dependencies and depth
func (db *GraphDBSQLite) Traverse(nodeID string, dependencies map[string]bool, depth int) ([]graph.Node, []graph.Edge, error) {
	visited := make(map[string]bool)
	var resultNodes []graph.Node
	var resultEdges []graph.Edge

	var nodesToVisit []string
	nodesToVisit = append(nodesToVisit, nodeID)

	nodeCache := make(map[string]graph.Node)
	edgeCache := make(map[string][]graph.Edge)

	// Fetch node data for nodesToVisit
	err := db.getNodeData(nodesToVisit, nodeCache)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch node data: %v", err)
	}

	// Get the edges for nodes to visit
	err = db.getEdgesForNodes(nodesToVisit, edgeCache)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch edge data: %v", err)
	}

	// Depth-first traversal
	var dfs func(id string, currentDepth int) error
	dfs = func(id string, currentDepth int) error {
		if currentDepth > depth || visited[id] {
			return nil
		}
		visited[id] = true

		// Add the node from cache to the result
		if node, exists := nodeCache[id]; exists {
			resultNodes = append(resultNodes, node)
		}

		// Add edges from cache
		if edges, exists := edgeCache[id]; exists {
			for _, edge := range edges {
				resultEdges = append(resultEdges, edge)
				// Add the target node of the edge to the list of nodes to visit
				if !visited[edge.To] {
					nodesToVisit = append(nodesToVisit, edge.To)
				}
			}
		}

		// Recursively visit connected nodes
		for _, edge := range edgeCache[id] {
			if !visited[edge.To] {
				err := dfs(edge.To, currentDepth+1)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

	// Start DFS traversal
	err = dfs(nodeID, 0)
	if err != nil {
		return nil, nil, err
	}

	// Return nodes and edges
	return resultNodes, resultEdges, nil
}

// Helper to get node data in batch
func (db *GraphDBSQLite) getNodeData(ids []string, nodeCache map[string]graph.Node) error {
	query := "SELECT id, data FROM nodes WHERE id IN (" + strings.Join(make([]string, len(ids)), "?") + ")"
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}

	rows, err := db.db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id, data string
		if err := rows.Scan(&id, &data); err != nil {
			return err
		}
		nodeCache[id] = graph.Node{ID: id, Data: map[string]string{"data": data}}
	}
	return rows.Err()
}

// Helper to get edge data in batch
func (db *GraphDBSQLite) getEdgesForNodes(ids []string, edgeCache map[string][]graph.Edge) error {
	query := "SELECT from_id, to_id, type, params FROM edges WHERE from_id IN (" + strings.Join(make([]string, len(ids)), "?") + ")"
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}

	rows, err := db.db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var fromID, toID, edgeType, params string
		if err := rows.Scan(&fromID, &toID, &edgeType, &params); err != nil {
			return err
		}
		edgeCache[fromID] = append(edgeCache[fromID], graph.Edge{From: fromID, To: toID, Type: edgeType, Params: map[string]string{"params": params}})
	}
	return rows.Err()
}

// Close closes the database connection
func (db *GraphDBSQLite) Close() error {
	return db.db.Close()
}
