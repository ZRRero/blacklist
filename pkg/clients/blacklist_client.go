package clients

import (
	"blacklist/models"
	"errors"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"log"
)

type BlacklistClient struct {
	client dynamodbiface.DynamoDBAPI
	table  string
}

func newSession() (*session.Session, error) {
	sess, err := session.NewSession()
	svc := session.Must(sess, err)
	return svc, err
}

func NewClient(table string) (*BlacklistClient, error) {
	// Create AWS Session
	sess, err := newSession()
	if err != nil {
		return nil, err
	}
	dynamoClient := &BlacklistClient{dynamodb.New(sess), table}
	return dynamoClient, nil
}

//Get

func (receiver *BlacklistClient) GetRecordById(id *string) (*models.Record, error) {
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
	if result.Item == nil {
		return nil, nil
	}
	record, err := models.FromDynamoItem(result.Item)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (receiver *BlacklistClient) GetRecordBatchByIds(ids []*string) ([]*models.Record, error) {
	if len(ids) > 25 {
		return nil, errors.New("ids list has more than BlacklistClient max batch (25)")
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

func (receiver *BlacklistClient) parseDynamoRecords(dynamoRecords []map[string]*dynamodb.AttributeValue) ([]*models.Record, error) {
	records := make([]*models.Record, 0, 25)
	for _, dynamoRecord := range dynamoRecords {
		record, err := models.FromDynamoItem(dynamoRecord)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func (receiver *BlacklistClient) getBatchRequestFromIds(ids []*string) map[string]*dynamodb.KeysAndAttributes {
	items := make([]map[string]*dynamodb.AttributeValue, 0, 25)
	for _, id := range ids {
		item := make(map[string]*dynamodb.AttributeValue)
		item["id"] = &dynamodb.AttributeValue{S: id}
		items = append(items, item)
	}
	keyAndAttributes := &dynamodb.KeysAndAttributes{
		Keys: items,
	}
	requestItems := make(map[string]*dynamodb.KeysAndAttributes)
	requestItems[receiver.table] = keyAndAttributes
	return requestItems
}

func (receiver *BlacklistClient) GetRecordsByQueries(queries []*models.Query) ([]*models.Record, error) {
	queryExpressionBuilder := expression.NewBuilder()
	for _, query := range queries {
		var keyExpression expression.KeyConditionBuilder
		switch query.Operand {
		case "EQUALS":
			keyExpression = expression.Key(query.Field).Equal(expression.Value(query.Value))
		case "GREATER_THAN":
			keyExpression = expression.Key(query.Field).GreaterThan(expression.Value(query.Value))
		case "LESSER_THAN":
			keyExpression = expression.Key(query.Field).LessThan(expression.Value(query.Value))
		case "BEGINS_WITH":
			keyExpression = expression.Key(query.Field).BeginsWith(query.Value)
		}
		queryExpressionBuilder.WithKeyCondition(keyExpression)
	}
	queryExpression, err := queryExpressionBuilder.Build()
	if err != nil {
		return nil, err
	}
	input := &dynamodb.QueryInput{
		KeyConditionExpression: queryExpression.KeyCondition(),
		TableName:              &receiver.table,
	}
	result, err := receiver.client.Query(input)
	if err != nil {
		return nil, err
	}
	records, err := receiver.parseDynamoRecords(result.Items)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (receiver *BlacklistClient) GetRecordsBetweenValues(queries []*models.BetweenQuery) ([]*models.Record, error) {
	queryExpressionBuilder := expression.NewBuilder()
	for _, query := range queries {
		keyExpression := expression.Key(query.Field).Between(expression.Value(query.Init), expression.Value(query.End))
		queryExpressionBuilder.WithKeyCondition(keyExpression)
	}
	queryExpression, err := queryExpressionBuilder.Build()
	if err != nil {
		return nil, err
	}
	input := &dynamodb.QueryInput{
		KeyConditionExpression: queryExpression.KeyCondition(),
		TableName:              &receiver.table,
	}
	result, err := receiver.client.Query(input)
	if err != nil {
		return nil, err
	}
	records, err := receiver.parseDynamoRecords(result.Items)
	if err != nil {
		return nil, err
	}
	return records, nil
}

//Save

func (receiver *BlacklistClient) SaveRecord(record *models.Record) (*models.Record, error) {
	log.Printf("Table: %s", receiver.table)
	input := &dynamodb.PutItemInput{
		TableName: &receiver.table,
		Item:      record.ToDynamoItem(),
	}
	_, err := receiver.client.PutItem(input)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (receiver *BlacklistClient) SaveBatchRecords(records []*models.Record) ([]*models.Record, error) {
	if len(records) > 25 {
		return nil, errors.New("ids list has more than BlacklistClient max batch (25)")
	}
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: receiver.getWriteBatchRequestFromModel(records),
	}
	result, err := receiver.client.BatchWriteItem(input)
	if err != nil {
		return nil, err
	}
	for len(result.UnprocessedItems) != 0 {
		input = &dynamodb.BatchWriteItemInput{
			RequestItems: result.UnprocessedItems,
		}
		result, err = receiver.client.BatchWriteItem(input)
		if err != nil {
			return nil, err
		}
	}
	return records, nil
}

func (receiver *BlacklistClient) getWriteBatchRequestFromModel(records []*models.Record) map[string][]*dynamodb.WriteRequest {
	items := make(map[string][]*dynamodb.WriteRequest)
	requests := make([]*dynamodb.WriteRequest, 0, len(records))
	for _, record := range records {
		requests = append(requests, &dynamodb.WriteRequest{PutRequest: &dynamodb.PutRequest{Item: record.ToDynamoItem()}})
	}
	items[receiver.table] = requests
	return items
}

//Delete

func (receiver *BlacklistClient) DeleteRecord(id *string) error {
	input := &dynamodb.DeleteItemInput{
		TableName: &receiver.table,
		Key:       map[string]*dynamodb.AttributeValue{"id": {S: id}},
	}
	_, err := receiver.client.DeleteItem(input)
	if err != nil {
		return err
	}
	return nil
}

func (receiver *BlacklistClient) DeleteBatchRecords(ids []*string) error {
	if len(ids) > 25 {
		return errors.New("ids list has more than BlacklistClient max batch (25)")
	}
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: receiver.getDeleteBatchRequestFromIds(ids),
	}
	result, err := receiver.client.BatchWriteItem(input)
	if err != nil {
		return err
	}
	for len(result.UnprocessedItems) != 0 {
		input = &dynamodb.BatchWriteItemInput{
			RequestItems: result.UnprocessedItems,
		}
		result, err = receiver.client.BatchWriteItem(input)
		if err != nil {
			return err
		}
	}
	return nil
}

func (receiver *BlacklistClient) getDeleteBatchRequestFromIds(ids []*string) map[string][]*dynamodb.WriteRequest {
	items := make(map[string][]*dynamodb.WriteRequest)
	requests := make([]*dynamodb.WriteRequest, 0, len(ids))
	for _, id := range ids {
		requests = append(requests, &dynamodb.WriteRequest{DeleteRequest: &dynamodb.DeleteRequest{Key: map[string]*dynamodb.AttributeValue{"id": {S: id}}}})
	}
	items[receiver.table] = requests
	return items
}
