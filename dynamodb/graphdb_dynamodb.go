package dynamo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fgrzl/graph"
)

type dynamoDBGraph struct {
	client    *dynamodb.Client
	tableName string
}

func NewDynamoDBGraph(region, tableName string) (graph.GraphDB, error) {
	cfg, err := config.LoadDefaultConfig(config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config, %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	return &dynamoDBGraph{
		client:    client,
		tableName: tableName,
	}, nil
}

func (db *dynamoDBGraph) PutNode(id string, node graph.Node) error {
	_, err := db.client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: &db.tableName,
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "NODE#" + id},
			"SK":   &types.AttributeValueMemberS{Value: "NODE"},
			"Data": &types.AttributeValueMemberS{Value: node.Data},
		},
	})
	return err
}

func (db *dynamoDBGraph) PutNodes(nodes []graph.Node) error {
	for _, node := range nodes {
		err := db.PutNode(node.ID, node)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *dynamoDBGraph) GetNode(id string) (graph.Node, error) {
	result, err := db.client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: &db.tableName,
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "NODE#" + id},
			"SK": &types.AttributeValueMemberS{Value: "NODE"},
		},
	})
	if err != nil {
		return graph.Node{}, err
	}

	if result.Item == nil {
		return graph.Node{}, fmt.Errorf("node %s not found", id)
	}

	node := graph.Node{
		ID:   id,
		Data: result.Item["Data"].(*types.AttributeValueMemberS).Value,
	}
	return node, nil
}

func (db *dynamoDBGraph) RemoveNode(id string) error {
	_, err := db.client.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		TableName: &db.tableName,
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "NODE#" + id},
			"SK": &types.AttributeValueMemberS{Value: "NODE"},
		},
	})
	if err != nil {
		return err
	}

	// Remove edges associated with the node
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

func (db *dynamoDBGraph) RemoveNodes(ids ...string) error {
	for _, id := range ids {
		err := db.RemoveNode(id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *dynamoDBGraph) PutEdge(fromID, toID, edgeType string, params map[string]string) error {
	_, err := db.client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: &db.tableName,
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "EDGE#" + fromID + "#" + toID},
			"SK":   &types.AttributeValueMemberS{Value: "EDGE"},
			"From": &types.AttributeValueMemberS{Value: fromID},
			"To":   &types.AttributeValueMemberS{Value: toID},
			"Type": &types.AttributeValueMemberS{Value: edgeType},
			"Params": &types.AttributeValueMemberM{
				Value: params,
			},
		},
	})
	return err
}

func (db *dynamoDBGraph) PutEdges(edges []graph.Edge) error {
	for _, edge := range edges {
		err := db.PutEdge(edge.From, edge.To, edge.Type, edge.Params)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *dynamoDBGraph) GetEdge(fromID, toID, edgeType string) (graph.Edge, error) {
	result, err := db.client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: &db.tableName,
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "EDGE#" + fromID + "#" + toID},
			"SK": &types.AttributeValueMemberS{Value: "EDGE"},
		},
	})
	if err != nil {
		return graph.Edge{}, err
	}

	if result.Item == nil {
		return graph.Edge{}, fmt.Errorf("edge %s-%s-%s not found", fromID, toID, edgeType)
	}

	params := result.Item["Params"].(*types.AttributeValueMemberM).Value
	edge := graph.Edge{
		From:   fromID,
		To:     toID,
		Type:   edgeType,
		Params: params,
	}

	return edge, nil
}

func (db *dynamoDBGraph) RemoveEdge(fromID, toID, edgeType string) error {
	_, err := db.client.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		TableName: &db.tableName,
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "EDGE#" + fromID + "#" + toID},
			"SK": &types.AttributeValueMemberS{Value: "EDGE"},
		},
	})
	return err
}

func (db *dynamoDBGraph) RemoveEdges(edges []graph.Edge) error {
	for _, edge := range edges {
		err := db.RemoveEdge(edge.From, edge.To, edge.Type)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *dynamoDBGraph) Traverse(nodeID string, dependencies map[string]bool, maxDepth int) ([]graph.Node, []graph.Edge, error) {
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

func (db *dynamoDBGraph) Close() error {
	// DynamoDB client does not require explicit cleanup, so no-op here
	return nil
}

func (db *dynamoDBGraph) GetEdgesRelatedToNode(nodeID string) ([]graph.Edge, error) {
	var edges []graph.Edge
	output, err := db.client.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              &db.tableName,
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "EDGE#" + nodeID},
		},
	})

	if err != nil {
		return nil, err
	}

	for _, item := range output.Items {
		from := item["From"].(*types.AttributeValueMemberS).Value
		to := item["To"].(*types.AttributeValueMemberS).Value
		edgeType := item["Type"].(*types.AttributeValueMemberS).Value
		params := item["Params"].(*types.AttributeValueMemberM).Value
		edges = append(edges, graph.Edge{
			From:   from,
			To:     to,
			Type:   edgeType,
			Params: params,
		})
	}

	return edges, nil
}
