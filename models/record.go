package models

import (
	blacklist "blacklist/tools/protos"
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"time"
)

type Record struct {
	recordId  string
	clientId  string
	productId string
	addedDate string
}

func NewRecord(recordId, clientId, productId string) *Record {
	return &Record{recordId: recordId, clientId: clientId, productId: productId, addedDate: time.Now().String()}
}

func (receiver *Record) Id() string {
	return fmt.Sprintf("%s:%s:%s", receiver.recordId, receiver.clientId, receiver.productId)
}

func FromDynamoItem(item map[string]*dynamodb.AttributeValue) (*Record, error) {
	return &Record{
		recordId:  *item["record_id"].S,
		clientId:  *item["client_id"].S,
		productId: *item["product_id"].S,
		addedDate: *item["added_date"].S,
	}, nil
}

func (receiver *Record) ToDynamoItem() map[string]*dynamodb.AttributeValue {
	id := receiver.Id()
	record := make(map[string]*dynamodb.AttributeValue)
	record["id"] = &dynamodb.AttributeValue{S: &id}
	record["record_id"] = &dynamodb.AttributeValue{S: &receiver.recordId}
	record["client_id"] = &dynamodb.AttributeValue{S: &receiver.clientId}
	record["product_id"] = &dynamodb.AttributeValue{S: &receiver.productId}
	record["added_date"] = &dynamodb.AttributeValue{S: &receiver.addedDate}
	return record
}

func (receiver *Record) ToDto() *blacklist.BlacklistRecordDto {
	return &blacklist.BlacklistRecordDto{
		RecordId:  receiver.recordId,
		ClientId:  receiver.clientId,
		ProductId: receiver.productId,
		AddedDate: receiver.addedDate,
	}
}
