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
func (g *redisGraph) RemoveEdges(edges []graph.Edge) error {
	panic("unimplemented")
}

func (g *redisGraph) RemoveNodes(ids ...string) error {
	pipe := g.client.Pipeline()

	for _, id := range ids {
		nodeKey := getNodeKey(id)
		edgeSetKey := getEdgeSetKey(id)

		// Delete the node
		pipe.Del(g.ctx, nodeKey)

		// Retrieve and delete all edges associated with the node
		edgeKeys, err := g.client.SMembers(g.ctx, edgeSetKey).Result()
		if err != nil {
			return fmt.Errorf("failed to retrieve edges for node %s: %v", id, err)
		}

		for _, edgeKey := range edgeKeys {
			pipe.Del(g.ctx, edgeKey)
		}

		// Delete the edge set
		pipe.Del(g.ctx, edgeSetKey)
	}

	_, err := pipe.Exec(g.ctx)
	return err
}

func NewRedisGraph(address string) (graph.Graph, error) {
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

func (g *redisGraph) PutNode(node graph.Node) error {
	nodeKey := getNodeKey(node.ID)
	return g.client.HSet(g.ctx, nodeKey, "data", node.Data).Err()
}

func (g *redisGraph) PutNodes(nodes []graph.Node) error {
	pipe := g.client.Pipeline()
	for _, node := range nodes {
		nodeKey := getNodeKey(node.ID)
		pipe.HSet(g.ctx, nodeKey, "data", node.Data)
	}
	_, err := pipe.Exec(g.ctx)
	return err
}

func (g *redisGraph) PutEdge(edge graph.Edge) error {
	edgeSetKey := getEdgeSetKey(edge.From)
	edgeDataKey := getEdgeDataKey(edge.From, edge.To, edge.Type)

	pipe := g.client.Pipeline()
	pipe.SAdd(g.ctx, edgeSetKey, edgeDataKey)
	pipe.HSet(g.ctx, edgeDataKey, "to", edge.To, "type", edge.Type)

	_, err := pipe.Exec(g.ctx)
	return err
}

func (g *redisGraph) PutEdges(edges []graph.Edge) error {
	pipe := g.client.Pipeline()
	for _, edge := range edges {
		edgeSetKey := getEdgeSetKey(edge.From)
		edgeDataKey := getEdgeDataKey(edge.From, edge.To, edge.Type)

		pipe.SAdd(g.ctx, edgeSetKey, edgeDataKey)
		pipe.HSet(g.ctx, edgeDataKey, "to", edge.To, "type", edge.Type)
	}
	_, err := pipe.Exec(g.ctx)
	return err
}

func (g *redisGraph) RemoveNode(id string) error {
	nodeKey := getNodeKey(id)
	edgeSetKey := getEdgeSetKey(id)

	// Start a pipeline for atomic operations
	pipe := g.client.Pipeline()
	pipe.Del(g.ctx, nodeKey)
	pipe.Del(g.ctx, edgeSetKey)

	// Retrieve and delete related edges
	edgeKeys, err := g.client.SMembers(g.ctx, edgeSetKey).Result()
	if err != nil {
		return err
	}
	for _, edgeKey := range edgeKeys {
		pipe.Del(g.ctx, edgeKey)
	}

	_, err = pipe.Exec(g.ctx)
	return err
}

func (g *redisGraph) RemoveEdge(edge graph.Edge) error {
	edgeSetKey := getEdgeSetKey(edge.From)
	edgeDataKey := getEdgeDataKey(edge.From, edge.To, edge.Type)

	pipe := g.client.Pipeline()
	pipe.SRem(g.ctx, edgeSetKey, edgeDataKey)
	pipe.Del(g.ctx, edgeDataKey)

	_, err := pipe.Exec(g.ctx)
	return err
}

func (g *redisGraph) GetNode(id string) (graph.Node, error) {
	nodeKey := getNodeKey(id)
	data, err := g.client.HGet(g.ctx, nodeKey, "data").Result()
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

func (g *redisGraph) GetEdge(fromID, toID, edgeType string) (graph.Edge, error) {
	edgeDataKey := getEdgeDataKey(fromID, toID, edgeType)
	exists, err := g.client.Exists(g.ctx, edgeDataKey).Result()
	if err != nil || exists == 0 {
		return graph.Edge{}, fmt.Errorf("edge not found")
	}

	return graph.Edge{
		From: fromID,
		To:   toID,
		Type: edgeType,
	}, nil
}

func (g *redisGraph) Traverse(nodeID string, edgeTypes graph.EdgeTypes, maxDepth int) ([]graph.Node, []graph.Edge, error) {
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

		node, err := g.GetNode(current.nodeID)
		if err == nil {
			resultNodes = append(resultNodes, node)
		}

		edgeSetKey := getEdgeSetKey(current.nodeID)
		edgeKeys, err := g.client.SMembers(g.ctx, edgeSetKey).Result()
		if err != nil {
			return nil, nil, err
		}

		for _, edgeKey := range edgeKeys {
			toID, _ := g.client.HGet(g.ctx, edgeKey, "to").Result()
			edgeType, _ := g.client.HGet(g.ctx, edgeKey, "type").Result()

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

func (g *redisGraph) Close() error {
	return g.client.Close()
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
