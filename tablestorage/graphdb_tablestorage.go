package tablestorage

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/fgrzl/graph"
)

type azureTableGraph struct {
	tableClient *aztables.Client
	ctx         context.Context
}

func NewAzureTableGraph(connectionString string, tableName string) (graph.GraphDB, error) {
	client, err := aztables.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Table client: %v", err)
	}
	return &azureTableGraph{
		tableClient: client,
		ctx:         context.Background(),
	}, nil
}

// Helper function to get the key for nodes
func getNodeKey(id string) string {
	return fmt.Sprintf("node:%s", id) // PartitionKey is "node:{nodeID}"
}

// Helper function to get the key for edges
func getEdgeKey(toID string) string {
	return toID // RowKey is just the toID
}

func (db *azureTableGraph) PutNode(id string, node graph.Node) error {
	entity := aztables.Entity{
		PartitionKey: getNodeKey(id), // PartitionKey is "node:{nodeID}"
		RowKey:       "",             // RowKey is empty for nodes
		Properties: map[string]any{
			"Data": node.Data,
		},
	}
	_, err := db.tableClient.UpsertEntity(db.ctx, &entity, aztables.UpsertOptions{})
	return err
}

func (db *azureTableGraph) PutNodes(nodes []graph.Node) error {
	batch := db.tableClient.NewBatch()
	for _, node := range nodes {
		entity := aztables.Entity{
			PartitionKey: getNodeKey(node.ID), // PartitionKey is "node:{nodeID}"
			RowKey:       "",                  // RowKey is empty for nodes
			Properties: map[string]any{
				"Data": node.Data,
			},
		}
		batch.AddUpsertEntity(&entity, aztables.UpsertOptions{})
	}
	_, err := batch.Submit(db.ctx)
	return err
}

func (db *azureTableGraph) GetNode(id string) (graph.Node, error) {
	entity, err := db.tableClient.GetEntity(db.ctx, getNodeKey(id), "", nil) // RowKey is empty for nodes
	if err != nil {
		return graph.Node{}, fmt.Errorf("failed to retrieve node %s: %v", id, err)
	}
	return graph.Node{
		ID:   id,
		Data: entity.Properties["Data"],
	}, nil
}

func (db *azureTableGraph) RemoveNode(id string) error {
	_, err := db.tableClient.DeleteEntity(db.ctx, getNodeKey(id), "", nil) // RowKey is empty for nodes
	if err != nil {
		return fmt.Errorf("failed to remove node %s: %v", id, err)
	}
	// Remove associated edges
	edgesToDelete, err := db.GetEdgesRelatedToNode(id)
	if err != nil {
		return err
	}
	for _, edge := range edgesToDelete {
		err := db.RemoveEdge(edge.From, edge.To, edge.Type)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *azureTableGraph) RemoveNodes(ids ...string) error {
	for _, id := range ids {
		err := db.RemoveNode(id)
		if err != nil {
			return fmt.Errorf("failed to remove node %s: %v", id, err)
		}
	}
	return nil
}

func (db *azureTableGraph) PutEdge(fromID, toID, edgeType string, params map[string]string) error {
	entity := aztables.Entity{
		PartitionKey: fmt.Sprintf("edge:%s", fromID), // PartitionKey is "edge:{fromID}"
		RowKey:       getEdgeKey(toID),               // RowKey is "toID"
		Properties: map[string]any{
			"From":   fromID,
			"To":     toID,
			"Type":   edgeType,
			"Params": params,
		},
	}
	_, err := db.tableClient.UpsertEntity(db.ctx, &entity, aztables.UpsertOptions{})
	return err
}

func (db *azureTableGraph) PutEdges(edges []graph.Edge) error {
	batch := db.tableClient.NewBatch()
	for _, edge := range edges {
		entity := aztables.Entity{
			PartitionKey: fmt.Sprintf("edge:%s", edge.From), // PartitionKey is "edge:{fromID}"
			RowKey:       getEdgeKey(edge.To),               // RowKey is "toID"
			Properties: map[string]any{
				"From":   edge.From,
				"To":     edge.To,
				"Type":   edge.Type,
				"Params": edge.Params,
			},
		}
		batch.AddUpsertEntity(&entity, aztables.UpsertOptions{})
	}
	_, err := batch.Submit(db.ctx)
	return err
}

func (db *azureTableGraph) GetEdge(fromID, toID, edgeType string) (graph.Edge, error) {
	entity, err := db.tableClient.GetEntity(db.ctx, fmt.Sprintf("edge:%s", fromID), getEdgeKey(toID), nil)
	if err != nil {
		return graph.Edge{}, fmt.Errorf("failed to retrieve edge %s-%s: %v", fromID, toID, err)
	}
	return graph.Edge{
		From:   fromID,
		To:     toID,
		Type:   edgeType,
		Params: entity.Properties["Params"].(map[string]string),
	}, nil
}

func (db *azureTableGraph) RemoveEdge(fromID, toID, edgeType string) error {
	// Delete the edge from <fromID> to <toID>
	_, err := db.tableClient.DeleteEntity(db.ctx, fmt.Sprintf("edge:%s", fromID), getEdgeKey(toID), nil)
	if err != nil {
		return fmt.Errorf("failed to delete edge %s-%s: %v", fromID, toID, err)
	}
	// Also delete the reverse edge (from "toID" to "fromID")
	_, err = db.tableClient.DeleteEntity(db.ctx, fmt.Sprintf("edge:%s", toID), getEdgeKey(fromID), nil)
	return err
}

func (db *azureTableGraph) RemoveEdges(edges []graph.Edge) error {
	for _, edge := range edges {
		err := db.RemoveEdge(edge.From, edge.To, edge.Type)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *azureTableGraph) Traverse(nodeID string, dependencies map[string]bool, maxDepth int) ([]graph.Node, []graph.Edge, error) {
	visitedNodes := make(map[string]bool)
	visitedEdges := make(map[string]bool)
	var resultNodes []graph.Node
	var resultEdges []graph.Edge

	stack := []struct {
		nodeID string
		depth  int
	}{{nodeID, maxDepth}}

	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if visitedNodes[current.nodeID] || current.depth <= 0 {
			continue
		}
		visitedNodes[current.nodeID] = true

		node, err := db.GetNode(current.nodeID)
		if err != nil {
			return nil, nil, err
		}
		resultNodes = append(resultNodes, node)

		edgesToVisit, err := db.GetEdgesRelatedToNode(current.nodeID)
		if err != nil {
			return nil, nil, err
		}

		for _, edge := range edgesToVisit {
			if visitedEdges[edge.From+edge.To+edge.Type] {
				continue
			}
			visitedEdges[edge.From+edge.To+edge.Type] = true
			resultEdges = append(resultEdges, edge)

			// Add the neighboring node to the stack
			if !visitedNodes[edge.To] {
				if current.depth > 1 {
					stack = append(stack, struct {
						nodeID string
						depth  int
					}{nodeID: edge.To, depth: current.depth - 1})
				} else {
					// Fetch the "to" node data if not traversing deeper
					toNode, err := db.GetNode(edge.To)
					if err != nil {
						return nil, nil, err
					}
					resultNodes = append(resultNodes, toNode)
					visitedNodes[edge.To] = true
				}
			}
		}
	}
	return resultNodes, resultEdges, nil
}

func (db *azureTableGraph) Close() error {
	// Azure Table Storage doesn't require explicit cleanup, so nothing to do here
	return nil
}

func (db *azureTableGraph) GetEdgesRelatedToNode(nodeID string) ([]graph.Edge, error) {
	var edges []graph.Edge
	prefix := fmt.Sprintf("edge:%s:", nodeID)
	resp, err := db.tableClient.ListEntities(db.ctx, &aztables.ListEntitiesOptions{
		Filter: fmt.Sprintf("PartitionKey eq 'edge:%s' and RowKey ge '%s' and RowKey lt '%s~'", nodeID, prefix, prefix+"~"),
	})
	if err != nil {
		return nil, err
	}

	for _, entity := range resp.Entities {
		from := entity.Properties["From"].(string)
		to := entity.Properties["To"].(string)
		edgeType := entity.Properties["Type"].(string)
		edges = append(edges, graph.Edge{
			From: from,
			To:   to,
			Type: edgeType,
		})
	}
	return edges, nil
}
