package clients

import (
	"blacklist/model"
	"errors"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type dynamo struct {
	client dynamodbiface.DynamoDBAPI
	table  string
}

func newSession() (*session.Session, error) {
	sess, err := session.NewSession()
	svc := session.Must(sess, err)
	return svc, err
}

func NewClient(table string) (*dynamo, error) {
	// Create AWS Session
	sess, err := newSession()
	if err != nil {
		return nil, err
	}
	dynamoClient := &dynamo{dynamodb.New(sess), table}
	return dynamoClient, nil
}

func (receiver *dynamo) GetRecordById(id *string) (*model.Record, error) {
	key := make(map[string]*dynamodb.AttributeValue)
	key["id"] = &dynamodb.AttributeValue{S: id}
	input := &dynamodb.GetItemInput{
		TableName: &receiver.table,
		Key:       key,
	}
	result, err := receiver.client.GetItem(input)
	if err != nil {
		return nil, err
	}
	record, err := model.FromDynamoItem(result.Item)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (receiver *dynamo) GetRecordBatchByIds(ids []*string) ([]*model.Record, error) {
	if len(ids) > 25 {
		return nil, errors.New("ids list has more than dynamo max batch (25)")
	}
	input := &dynamodb.BatchGetItemInput{
		RequestItems: receiver.getBatchRequestFromIds(ids),
	}
	result, err := receiver.client.BatchGetItem(input)
	if err != nil {
		return nil, err
	}
	dynamoRecords := result.Responses[receiver.table]
	records, err := receiver.parseDynamoRecords(dynamoRecords)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (receiver *dynamo) parseDynamoRecords(dynamoRecords []map[string]*dynamodb.AttributeValue) ([]*model.Record, error) {
	index := 0
	records := make([]*model.Record, 0, 25)
	for index < len(dynamoRecords) {
		record, err := model.FromDynamoItem(dynamoRecords[index])
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func (receiver *dynamo) getBatchRequestFromIds(ids []*string) map[string]*dynamodb.KeysAndAttributes {
	index := 0
	items := make([]map[string]*dynamodb.AttributeValue, 0, 25)
	for index < len(ids) {
		item := make(map[string]*dynamodb.AttributeValue)
		item["id"] = &dynamodb.AttributeValue{S: ids[index]}
		items = append(items, item)
		index++
	}
	keyAndAttributes := &dynamodb.KeysAndAttributes{
		Keys: items,
	}
	requestItems := make(map[string]*dynamodb.KeysAndAttributes)
	requestItems[receiver.table] = keyAndAttributes
	return requestItems
}

func (receiver *dynamo) SaveRecord(record *model.Record) (*model.Record, error) {
	input := &dynamodb.PutItemInput{
		TableName: &receiver.table,
		Item:      record.ToDynamoItem(),
	}
	output, err := receiver.client.PutItem(input)
	if err != nil {
		return nil, err
	}
	result, err := model.FromDynamoItem(output.Attributes)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (receiver *dynamo) SaveBatchRecords(records []*model.Record) ([]*model.Record, error) {
	if len(records) > 25 {
		return nil, errors.New("ids list has more than dynamo max batch (25)")
	}
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: receiver.getWriteBatchRequestFromModel(records),
	}
	result, err := receiver.client.BatchWriteItem(input)
	if err != nil {
		return nil, err
	}
	for len(result.UnprocessedItems) == 0 {
		result, err = receiver.client.BatchWriteItem(input)
		if err != nil {
			return nil, err
		}
	}
	return records, nil
}

func (receiver *dynamo) getWriteBatchRequestFromModel(records []*model.Record) map[string][]*dynamodb.WriteRequest {
	items := make(map[string][]*dynamodb.WriteRequest)
	index := 0
	requests := make([]*dynamodb.WriteRequest, 0, len(records))
	for index < len(records) {
		requests = append(requests, &dynamodb.WriteRequest{PutRequest: &dynamodb.PutRequest{Item: records[index].ToDynamoItem()}})
		index++
	}
	items[receiver.table] = requests
	return items
}
