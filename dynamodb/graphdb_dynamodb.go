package graph

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// convertToAttributeValueMap converts a map of strings to a map of DynamoDB AttributeValues
func convertToAttributeValueMap(input map[string]string) map[string]*dynamodb.AttributeValue {
	avMap := make(map[string]*dynamodb.AttributeValue)
	for k, v := range input {
		avMap[k] = &dynamodb.AttributeValue{S: aws.String(v)}
	}
	return avMap
}

type GraphDBDynamoDB struct {
	svc       *dynamodb.DynamoDB
	nodeTable string
	edgeTable string
}

// NewGraphDBDynamoDB creates a new instance of GraphDBDynamoDB
func NewGraphDBDynamoDB(session *session.Session, nodeTable, edgeTable string) *GraphDBDynamoDB {
	return &GraphDBDynamoDB{
		svc:       dynamodb.New(session),
		nodeTable: nodeTable,
		edgeTable: edgeTable,
	}
}

// Node structure
type Node struct {
	ID   string            `json:"id"`
	Data map[string]string `json:"data"`
}

// Edge structure
type Edge struct {
	From   string            `json:"from"`
	To     string            `json:"to"`
	Type   string            `json:"type"`
	Params map[string]string `json:"params"`
}

// PutNode adds a single node to the database
func (db *GraphDBDynamoDB) PutNode(id string, node Node) error {
	_, err := db.svc.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(db.nodeTable),
		Item: map[string]*dynamodb.AttributeValue{
			"id":   {S: aws.String(id)},
			"data": {M: aws.StringMap(node.Data)},
		},
	})
	return err
}

// PutNodes adds multiple nodes using batch write
func (db *GraphDBDynamoDB) PutNodes(nodes []Node) error {
	var writeRequests []*dynamodb.WriteRequest
	for _, node := range nodes {
		writeRequests = append(writeRequests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"id":   {S: aws.String(node.ID)},
					"data": {M: aws.StringMap(node.Data)},
				},
			},
		})
	}

	_, err := db.svc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			db.nodeTable: writeRequests,
		},
	})
	return err
}
			"params":  {M: convertToAttributeValueMap(params)},
// PutEdge adds a single edge to the database
func (db *GraphDBDynamoDB) PutEdge(from, to, edgeType string, params map[string]string) error {
	_, err := db.svc.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(db.edgeTable),
		Item: map[string]*dynamodb.AttributeValue{
			"from_id": {S: aws.String(from)},
			"to_id":   {S: aws.String(to)},
			"type":    {S: aws.String(edgeType)},
			"params":  {M: aws.StringMap(params)},
		},
	})
	return err
}

// PutEdges adds multiple edges using batch write
func (db *GraphDBDynamoDB) PutEdges(edges []Edge) error {
	var writeRequests []*dynamodb.WriteRequest
	for _, edge := range edges {
		writeRequests = append(writeRequests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"from_id": {S: aws.String(edge.From)},
					"to_id":   {S: aws.String(edge.To)},
					"type":    {S: aws.String(edge.Type)},
					"params":  {M: aws.StringMap(edge.Params)},
				},
			},
		})
	}

	_, err := db.svc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			db.edgeTable: writeRequests,
		},
	})
	return err
}

// GetNode retrieves a node by its ID
func (db *GraphDBDynamoDB) GetNode(id string) (Node, error) {
	result, err := db.svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(db.nodeTable),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {S: aws.String(id)},
		},
	})
	if err != nil {
		return Node{}, err
	}

	var node Node
	if result.Item == nil {
		return node, nil // Node not found
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &node)
	if err != nil {
		return node, err
	}

	return node, nil
}

// GetNodes retrieves multiple nodes by their IDs using batch get
func (db *GraphDBDynamoDB) GetNodes(ids []string) ([]Node, error) {
	var keys []map[string]*dynamodb.AttributeValue
	for _, id := range ids {
		keys = append(keys, map[string]*dynamodb.AttributeValue{
			"id": {S: aws.String(id)},
		})
	}

	output, err := db.svc.BatchGetItem(&dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			db.nodeTable: {Keys: keys},
		},
	})
	if err != nil {
		return nil, err
	}

	var nodes []Node
	for _, item := range output.Responses[db.nodeTable] {
		var node Node
		if err := dynamodbattribute.UnmarshalMap(item, &node); err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}

	return nodes, nil
}

// GetEdge retrieves an edge between two nodes
func (db *GraphDBDynamoDB) GetEdge(from, to, edgeType string) (Edge, error) {
	result, err := db.svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(db.edgeTable),
		Key: map[string]*dynamodb.AttributeValue{
			"from_id": {S: aws.String(from)},
			"to_id":   {S: aws.String(to)},
			"type":    {S: aws.String(edgeType)},
		},
	})
	if err != nil {
		return Edge{}, err
	}

	var edge Edge
	if result.Item == nil {
		return edge, nil // Edge not found
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &edge)
	if err != nil {
		return edge, err
	}

	return edge, nil
}

// GetEdges retrieves multiple edges using batch get
func (db *GraphDBDynamoDB) GetEdges(fromIDs, toIDs []string, edgeType string) ([]Edge, error) {
	var keys []map[string]*dynamodb.AttributeValue
	for i := 0; i < len(fromIDs); i++ {
		keys = append(keys, map[string]*dynamodb.AttributeValue{
			"from_id": {S: aws.String(fromIDs[i])},
			"to_id":   {S: aws.String(toIDs[i])},
			"type":    {S: aws.String(edgeType)},
		})
	}

	output, err := db.svc.BatchGetItem(&dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			db.edgeTable: {Keys: keys},
		},
	})
	if err != nil {
		return nil, err
	}

	var edges []Edge
	for _, item := range output.Responses[db.edgeTable] {
		var edge Edge
		if err := dynamodbattribute.UnmarshalMap(item, &edge); err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}

	return edges, nil
}

// Traverse performs a graph traversal starting from a node
func (db *GraphDBDynamoDB) Traverse(startNodeID string, dependencies map[string]bool, depth int) ([]Node, []Edge, error) {
	var nodes []Node
	var edges []Edge
	visited := make(map[string]bool)

	var dfs func(nodeID string, currentDepth int) error
	dfs = func(nodeID string, currentDepth int) error {
		if currentDepth > depth || visited[nodeID] {
			return nil
		}

		visited[nodeID] = true
		nodesList, err := db.GetNodes([]string{nodeID})
		if err != nil {
			return err
		}
		nodes = append(nodes, nodesList...)

		// Get edges for the current node
		edgeList, err := db.GetEdges([]string{nodeID}, []string{}, "") // Get edges from this node
		if err != nil {
			return err
		}
		edges = append(edges, edgeList...)

		// Traverse connected nodes recursively
		for _, edge := range edgeList {
			if !visited[edge.To] && dependencies[edge.To] {
				err := dfs(edge.To, currentDepth+1)
				if err != nil {
					return err
				}
			}
		}

		return nil
	}

	err := dfs(startNodeID, 0)
	if err != nil {
		return nil, nil, err
	}

	return nodes, edges, nil
}

// Close closes the database connection (optional)
func (db *GraphDBDynamoDB) Close() error {
	// No explicit close needed for DynamoDB connection as it's managed by AWS SDK
	return nil
}
