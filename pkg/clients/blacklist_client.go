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

func (receiver *BlacklistClient) GetRecordsByQueries(queries []*models.Query, betweenQueries []*models.BetweenQuery) ([]*models.Record, error) {
	queryFilter, queriesInFilter := getFilterByQueries(queries)
	betweenFilter, queriesInBetweenFilter := getFilterByBetweenQueries(betweenQueries)
	var filter expression.ConditionBuilder
	if queriesInFilter == 0 && queriesInBetweenFilter == 0 {
		log.Print("As there are no queries a full scan will be performed")
	}
	if queriesInFilter > 0 && queriesInBetweenFilter == 0 {
		filter = queryFilter
	}
	if queriesInFilter == 0 && queriesInBetweenFilter > 0 {
		filter = betweenFilter
	}
	if queriesInFilter > 0 && queriesInBetweenFilter > 0 {
		filter = queryFilter.And(betweenFilter)
	}
	queryExpression, err := expression.NewBuilder().WithFilter(filter).Build()
	if err != nil {
		return nil, err
	}
	input := &dynamodb.ScanInput{
		ExpressionAttributeNames:  queryExpression.Names(),
		ExpressionAttributeValues: queryExpression.Values(),
		FilterExpression:          queryExpression.Filter(),
		TableName:                 &receiver.table,
	}
	result, err := receiver.client.Scan(input)
	if err != nil {
		return nil, err
	}
	records, err := receiver.parseDynamoRecords(result.Items)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func getFilterByQueries(queries []*models.Query) (expression.ConditionBuilder, int) {
	var filter expression.ConditionBuilder
	for index, query := range queries {
		var currentFilter expression.ConditionBuilder
		switch query.Operand {
		case "EQUALS":
			currentFilter = expression.Equal(expression.Name(query.Field), expression.Value(query.Value))
		case "GREATER_THAN":
			currentFilter = expression.GreaterThan(expression.Name(query.Field), expression.Value(query.Value))
		case "LESSER_THAN":
			currentFilter = expression.LessThan(expression.Name(query.Field), expression.Value(query.Value))
		case "BEGINS_WITH":
			currentFilter = expression.BeginsWith(expression.Name(query.Field), query.Value)
		}
		if index == 0 {
			filter = currentFilter
		} else {
			filter = filter.And(currentFilter)
		}
	}
	return filter, len(queries)
}

func getFilterByBetweenQueries(queries []*models.BetweenQuery) (expression.ConditionBuilder, int) {
	var filter expression.ConditionBuilder
	for index, query := range queries {
		currentFilter := expression.Between(expression.Name(query.Field), expression.Value(query.Init), expression.Value(query.End))
		if index == 0 {
			filter = currentFilter
		} else {
			filter = filter.And(currentFilter)
		}
	}
	return filter, len(queries)
}

//Save

func (receiver *BlacklistClient) SaveRecord(record *models.Record) (*models.Record, error) {
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
