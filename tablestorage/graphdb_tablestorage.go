package azure

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/aztable"
	"github.com/fgrzl/graph"
)

type azureTableGraph struct {
	tableClient *aztable.Client
	ctx         context.Context
}

func NewAzureTableGraph(connectionString string, tableName string) (graph.GraphDB, error) {
	client, err := aztable.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Table client: %v", err)
	}
	return &azureTableGraph{
		tableClient: client,
		ctx:         context.Background(),
	}, nil
}

func (db *azureTableGraph) PutNode(id string, node graph.Node) error {
	entity := aztable.Entity{
		PartitionKey: "node",
		RowKey:       id,
		Properties: map[string]any{
			"Data": node.Data,
		},
	}
	_, err := db.tableClient.UpsertEntity(db.ctx, &entity, aztable.UpsertOptions{})
	return err
}

func (db *azureTableGraph) PutNodes(nodes []graph.Node) error {
	batch := db.tableClient.NewBatch()
	for _, node := range nodes {
		entity := aztable.Entity{
			PartitionKey: "node",
			RowKey:       node.ID,
			Properties: map[string]any{
				"Data": node.Data,
			},
		}
		batch.UpsertEntity(&entity, aztable.EntityUpsertOptions{})
	}
	_, err := batch.Submit(db.ctx)
	return err
}

func (db *azureTableGraph) GetNode(id string) (graph.Node, error) {
	entity, err := db.tableClient.GetEntity(db.ctx, "node", id, nil)
	if err != nil {
		return graph.Node{}, fmt.Errorf("failed to retrieve node %s: %v", id, err)
	}
	return graph.Node{
		ID:   id,
		Data: entity.Properties["Data"],
	}, nil
}

func (db *azureTableGraph) RemoveNode(id string) error {
	_, err := db.tableClient.DeleteEntity(db.ctx, "node", id, nil)
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
	edgeKey := getEdgeKey(fromID, toID, edgeType)
	entity := aztable.Entity{
		PartitionKey: "edge",
		RowKey:       edgeKey,
		Properties: map[string]any{
			"From":   fromID,
			"To":     toID,
			"Type":   edgeType,
			"Params": params,
		},
	}
	_, err := db.tableClient.UpsertEntity(db.ctx, &entity, aztable.UpsertOptions{})
	return err
}

func (db *azureTableGraph) PutEdges(edges []graph.Edge) error {
	batch := db.tableClient.NewBatch()
	for _, edge := range edges {
		edgeKey := getEdgeKey(edge.From, edge.To, edge.Type)
		entity := aztable.Entity{
			PartitionKey: "edge",
			RowKey:       edgeKey,
			Properties: map[string]any{
				"From":   edge.From,
				"To":     edge.To,
				"Type":   edge.Type,
				"Params": edge.Params,
			},
		}
		batch.UpsertEntity(&entity, aztable.EntityUpsertOptions{})
	}
	_, err := batch.Submit(db.ctx)
	return err
}

func (db *azureTableGraph) GetEdge(fromID, toID, edgeType string) (graph.Edge, error) {
	edgeKey := getEdgeKey(fromID, toID, edgeType)
	entity, err := db.tableClient.GetEntity(db.ctx, "edge", edgeKey, nil)
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
	edgeKey := getEdgeKey(fromID, toID, edgeType)
	_, err := db.tableClient.DeleteEntity(db.ctx, "edge", edgeKey, nil)
	if err != nil {
		return fmt.Errorf("failed to delete edge %s-%s: %v", fromID, toID, err)
	}
	// Also delete reverse edge
	reverseEdgeKey := getEdgeKey(toID, fromID, edgeType)
	_, err = db.tableClient.DeleteEntity(db.ctx, "edge", reverseEdgeKey, nil)
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
	resp, err := db.tableClient.ListEntities(db.ctx, &aztable.ListEntitiesOptions{
		Filter: fmt.Sprintf("PartitionKey eq 'edge' and RowKey ge '%s' and RowKey lt '%s~'", prefix, prefix+"~"),
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

func getEdgeKey(fromID, toID, edgeType string) string {
	return fmt.Sprintf("%s:%s:%s", fromID, toID, edgeType)
}
