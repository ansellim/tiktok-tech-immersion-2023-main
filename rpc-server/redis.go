package main

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
)

type RedisDatabaseClient struct {
	client *redis.Client
}

func (client *RedisDatabaseClient) InitializeClient(context context.Context, address, password string) error {
	newClient := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       0,
	})

	if err := newClient.Ping(context).Err(); err != nil {
		return err
	}

	client.client = newClient
	return nil
}

func (client *RedisDatabaseClient) SaveMessage(context context.Context, conversation string, message *Message) error {
	// Store the message in json
	text, err := json.Marshal(message)

	if err != nil {
		return err
	}

	member := &redis.Z{
		Score:  float64(message.Timestamp), // we sort by this key
		Member: text,                       // data
	}

	_, err = client.client.ZAdd(context, conversation, *member).Result()
	if err != nil {
		return err
	}

	return nil
}

func (client *RedisDatabaseClient) RetrieveConversationMessages(ctx context.Context, conversation string, start, end int64, reverse bool) ([]*Message, error) {
	var (
		rawMessages []string
		messages    []*Message
		err         error
	)

	if reverse {
		// Desc order with time -> first message is the latest message
		rawMessages, err = client.client.ZRevRange(ctx, conversation, start, end).Result()
		if err != nil {
			return nil, err
		}
	} else {
		// Asc order with time -> first message is the earliest message
		rawMessages, err = client.client.ZRange(ctx, conversation, start, end).Result()
		if err != nil {
			return nil, err
		}
	}

	for _, msg := range rawMessages {
		temp := &Message{}
		err := json.Unmarshal([]byte(msg), temp)
		if err != nil {
			return nil, err
		}
		messages = append(messages, temp)
	}

	return messages, nil
}
