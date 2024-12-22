package redis

import (
	"fmt"

	"github.com/fgrzl/graph"
	"github.com/redis/go-redis/v9"
	"golang.org/x/net/context"
)

type redisGraph struct {
	client *redis.Client
	ctx    context.Context
}

// RemoveEdges implements graph.GraphDB.
func (db *redisGraph) RemoveEdges(edges []graph.Edge) error {
	panic("unimplemented")
}

func (db *redisGraph) RemoveNodes(ids ...string) error {
	pipe := db.client.Pipeline()

	for _, id := range ids {
		nodeKey := getNodeKey(id)
		edgeSetKey := getEdgeSetKey(id)

		// Delete the node
		pipe.Del(db.ctx, nodeKey)

		// Retrieve and delete all edges associated with the node
		edgeKeys, err := db.client.SMembers(db.ctx, edgeSetKey).Result()
		if err != nil {
			return fmt.Errorf("failed to retrieve edges for node %s: %v", id, err)
		}

		for _, edgeKey := range edgeKeys {
			pipe.Del(db.ctx, edgeKey)
		}

		// Delete the edge set
		pipe.Del(db.ctx, edgeSetKey)
	}

	_, err := pipe.Exec(db.ctx)
	return err
}

func NewRedisGraph(address string) (graph.GraphDB, error) {
	client := redis.NewClient(&redis.Options{
		Addr: address,
	})
	ctx := context.Background()

	// Test the connection
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	return &redisGraph{
		client: client,
		ctx:    ctx,
	}, nil
}

func (db *redisGraph) PutNode(id string, node graph.Node) error {
	nodeKey := getNodeKey(id)
	return db.client.HSet(db.ctx, nodeKey, "data", node.Data).Err()
}

func (db *redisGraph) PutNodes(nodes []graph.Node) error {
	pipe := db.client.Pipeline()
	for _, node := range nodes {
		nodeKey := getNodeKey(node.ID)
		pipe.HSet(db.ctx, nodeKey, "data", node.Data)
	}
	_, err := pipe.Exec(db.ctx)
	return err
}

func (db *redisGraph) PutEdge(fromID, toID, edgeType string, params map[string]string) error {
	edgeSetKey := getEdgeSetKey(fromID)
	edgeDataKey := getEdgeDataKey(fromID, toID, edgeType)

	pipe := db.client.Pipeline()
	pipe.SAdd(db.ctx, edgeSetKey, edgeDataKey)
	pipe.HSet(db.ctx, edgeDataKey, "to", toID, "type", edgeType)

	if params != nil {
		for k, v := range params {
			pipe.HSet(db.ctx, edgeDataKey, k, v)
		}
	}

	_, err := pipe.Exec(db.ctx)
	return err
}

func (db *redisGraph) PutEdges(edges []graph.Edge) error {
	pipe := db.client.Pipeline()
	for _, edge := range edges {
		edgeSetKey := getEdgeSetKey(edge.From)
		edgeDataKey := getEdgeDataKey(edge.From, edge.To, edge.Type)

		pipe.SAdd(db.ctx, edgeSetKey, edgeDataKey)
		pipe.HSet(db.ctx, edgeDataKey, "to", edge.To, "type", edge.Type)
	}
	_, err := pipe.Exec(db.ctx)
	return err
}

func (db *redisGraph) RemoveNode(id string) error {
	nodeKey := getNodeKey(id)
	edgeSetKey := getEdgeSetKey(id)

	// Start a pipeline for atomic operations
	pipe := db.client.Pipeline()
	pipe.Del(db.ctx, nodeKey)
	pipe.Del(db.ctx, edgeSetKey)

	// Retrieve and delete related edges
	edgeKeys, err := db.client.SMembers(db.ctx, edgeSetKey).Result()
	if err != nil {
		return err
	}
	for _, edgeKey := range edgeKeys {
		pipe.Del(db.ctx, edgeKey)
	}

	_, err = pipe.Exec(db.ctx)
	return err
}

func (db *redisGraph) RemoveEdge(fromID, toID, edgeType string) error {
	edgeSetKey := getEdgeSetKey(fromID)
	edgeDataKey := getEdgeDataKey(fromID, toID, edgeType)

	pipe := db.client.Pipeline()
	pipe.SRem(db.ctx, edgeSetKey, edgeDataKey)
	pipe.Del(db.ctx, edgeDataKey)

	_, err := pipe.Exec(db.ctx)
	return err
}

func (db *redisGraph) GetNode(id string) (graph.Node, error) {
	nodeKey := getNodeKey(id)
	data, err := db.client.HGet(db.ctx, nodeKey, "data").Result()
	if err == redis.Nil {
		return graph.Node{}, fmt.Errorf("node %s not found", id)
	}
	if err != nil {
		return graph.Node{}, err
	}

	return graph.Node{
		ID:   id,
		Data: []byte(data),
	}, nil
}

func (db *redisGraph) GetEdge(fromID, toID, edgeType string) (graph.Edge, error) {
	edgeDataKey := getEdgeDataKey(fromID, toID, edgeType)
	exists, err := db.client.Exists(db.ctx, edgeDataKey).Result()
	if err != nil || exists == 0 {
		return graph.Edge{}, fmt.Errorf("edge not found")
	}

	return graph.Edge{
		From: fromID,
		To:   toID,
		Type: edgeType,
	}, nil
}

func (db *redisGraph) Traverse(nodeID string, dependencies map[string]bool, maxDepth int) ([]graph.Node, []graph.Edge, error) {
	visitedNodes := make(map[string]bool)
	var resultNodes []graph.Node
	var resultEdges []graph.Edge

	queue := []struct {
		nodeID string
		depth  int
	}{{nodeID, maxDepth}}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if visitedNodes[current.nodeID] || current.depth <= 0 {
			continue
		}
		visitedNodes[current.nodeID] = true

		node, err := db.GetNode(current.nodeID)
		if err == nil {
			resultNodes = append(resultNodes, node)
		}

		edgeSetKey := getEdgeSetKey(current.nodeID)
		edgeKeys, err := db.client.SMembers(db.ctx, edgeSetKey).Result()
		if err != nil {
			return nil, nil, err
		}

		for _, edgeKey := range edgeKeys {
			toID, _ := db.client.HGet(db.ctx, edgeKey, "to").Result()
			edgeType, _ := db.client.HGet(db.ctx, edgeKey, "type").Result()

			resultEdges = append(resultEdges, graph.Edge{
				From: current.nodeID,
				To:   toID,
				Type: edgeType,
			})

			queue = append(queue, struct {
				nodeID string
				depth  int
			}{toID, current.depth - 1})
		}
	}

	return resultNodes, resultEdges, nil
}

func (db *redisGraph) Close() error {
	return db.client.Close()
}

func getNodeKey(nodeID string) string {
	return "node:" + nodeID
}

func getEdgeSetKey(nodeID string) string {
	return "edges:" + nodeID
}

func getEdgeDataKey(fromID, toID, edgeType string) string {
	return fmt.Sprintf("edge:%s:%s:%s", fromID, toID, edgeType)
}
