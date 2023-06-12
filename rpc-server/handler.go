package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {

	timestamp := time.Now().Unix()
	message := &Message{
		Message:   req.Message.GetText(),
		Sender:    req.Message.GetSender(),
		Timestamp: timestamp,
	}

	conversation, err := getConversation(req.Message.GetChat())
	if err != nil {
		return nil, err
	}

	err = redisDatabase.SaveMessage(ctx, conversation, message)
	if err != nil {
		return nil, err
	}

	resp := rpc.NewSendResponse()
	resp.Code, resp.Msg = 0, "success"
	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	conversation, err := getConversation(req.GetChat())
	if err != nil {
		return nil, err
	}

	limit := int64(req.GetLimit())
	if limit == 0 {
		limit = 10 // default limit 10
	}
	start := req.GetCursor()
	end := start + limit // did not minus 1 on purpose for hasMore check later on

	messages, err := redisDatabase.RetrieveConversationMessages(ctx, conversation, start, end, req.GetReverse())
	if err != nil {
		return nil, err
	}

	respMessages := make([]*rpc.Message, 0)
	var counter int64 = 0
	var nextCursor int64 = 0
	hasMore := false
	for _, msg := range messages {
		if counter+1 > limit {
			// having extra value here means it has more data
			hasMore = true
			nextCursor = end
			break // do not return the last message
		}
		temp := &rpc.Message{
			Chat:     req.GetChat(),
			Text:     msg.Message,
			Sender:   msg.Sender,
			SendTime: msg.Timestamp,
		}
		respMessages = append(respMessages, temp)
		counter += 1
	}

	resp := rpc.NewPullResponse()
	resp.Messages = respMessages
	resp.Code = 0
	resp.Msg = "success"
	resp.HasMore = &hasMore
	resp.NextCursor = &nextCursor

	return resp, nil
}

func getConversation(chat string) (string, error) {
	var conversation string

	lowercase := strings.ToLower(chat)
	senders := strings.Split(lowercase, ":")

	if len(senders) != 2 {
		err := fmt.Errorf("Invalid conversation ID", chat)
		return "", err
	}

	sender1, sender2 := senders[0], senders[1]
	// Compare the sender and receiver alphabetically, and sort it asc to form the conversation ID
	if comp := strings.Compare(sender1, sender2); comp == 1 {
		conversation = fmt.Sprintf("%s:%s", sender2, sender1)
	} else {
		conversation = fmt.Sprintf("%s:%s", sender1, sender2)
	}

	return conversation, nil
}
