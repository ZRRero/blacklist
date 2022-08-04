package model

import (
	blacklist "blacklist/protos"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"time"
)

type Record struct {
	id           string
	restrictions map[RestrictionKey]*Restriction
}

type Restriction struct {
	clientId  string
	addedDate string
	product   *Product
}

type Product struct {
	name string
	id   string
}

type RestrictionKey struct {
	clientId  string
	productId string
}

func NewRecord(id string, restrictions map[RestrictionKey]*Restriction) *Record {
	return &Record{id: id, restrictions: restrictions}
}

func FromDynamoItem(item map[string]*dynamodb.AttributeValue) (*Record, error) {
	restrictionsMap := make(map[RestrictionKey]*Restriction)
	for _, dynamoRestriction := range item["restrictions"].L {
		product := &Product{
			name: *dynamoRestriction.M["product"].M["name"].S,
			id:   *dynamoRestriction.M["product"].M["id"].S,
		}
		restriction := &Restriction{
			clientId:  *dynamoRestriction.M["clientId"].S,
			addedDate: *dynamoRestriction.M["addedDate"].S,
			product:   product,
		}
		restrictionsMap[RestrictionKey{clientId: *dynamoRestriction.M["clientId"].S,
			productId: *dynamoRestriction.M["product"].M["id"].S}] = restriction
	}
	return &Record{
		id:           *item["id"].S,
		restrictions: restrictionsMap,
	}, nil
}

func FromDto(dto *blacklist.BlacklistRecordDto) (*Record, error) {
	restrictionsList := make([]*Restriction, 0, len(dto.Restrictions))
	restrictionsMap := make(map[RestrictionKey]*Restriction)
	for _, dtoRestriction := range dto.Restrictions {
		product := &Product{
			name: dtoRestriction.Product.ProductName,
			id:   dtoRestriction.Product.ProductId,
		}
		restriction := &Restriction{
			clientId:  dtoRestriction.ClientId,
			addedDate: dtoRestriction.AddedDate,
			product:   product,
		}
		restrictionsList = append(restrictionsList, restriction)
		restrictionsMap[RestrictionKey{clientId: dtoRestriction.ClientId,
			productId: dtoRestriction.Product.ProductId}] = restriction
	}
	return &Record{
		id:           dto.Id,
		restrictions: restrictionsMap,
	}, nil
}

func (receiver *Record) ToDynamoItem() map[string]*dynamodb.AttributeValue {
	restrictions := make([]*dynamodb.AttributeValue, 0, len(receiver.restrictions))
	for _, restrictionElement := range receiver.restrictions {
		product := make(map[string]*dynamodb.AttributeValue)
		product["name"] = &dynamodb.AttributeValue{S: &restrictionElement.product.name}
		product["id"] = &dynamodb.AttributeValue{S: &restrictionElement.product.id}
		restriction := make(map[string]*dynamodb.AttributeValue)
		restriction["clientId"] = &dynamodb.AttributeValue{S: &restrictionElement.clientId}
		restriction["addedDate"] = &dynamodb.AttributeValue{S: &restrictionElement.addedDate}
		restriction["product"] = &dynamodb.AttributeValue{M: product}
		restrictions = append(restrictions, &dynamodb.AttributeValue{M: restriction})
	}
	record := make(map[string]*dynamodb.AttributeValue)
	record["id"] = &dynamodb.AttributeValue{S: &receiver.id}
	record["restrictions"] = &dynamodb.AttributeValue{L: restrictions}
	return record
}

func (receiver *Record) ToDto() *blacklist.BlacklistRecordDto {
	restrictionsDtoList := make([]*blacklist.RestrictionDto, 0, len(receiver.restrictions))
	for _, restrictionElement := range receiver.restrictions {
		productDto := &blacklist.ProductDto{
			ProductId:   restrictionElement.product.id,
			ProductName: restrictionElement.product.name,
		}
		restrictionDto := &blacklist.RestrictionDto{
			ClientId:  restrictionElement.clientId,
			AddedDate: restrictionElement.addedDate,
			Product:   productDto,
		}
		restrictionsDtoList = append(restrictionsDtoList, restrictionDto)
	}
	return &blacklist.BlacklistRecordDto{
		Id:           receiver.id,
		Restrictions: restrictionsDtoList,
	}
}

func (receiver *Record) SaveRestriction(clientId, productId, productName string) error {
	_, present := receiver.restrictions[RestrictionKey{clientId: clientId, productId: productId}]
	if present {
		return errors.New(fmt.Sprintf("the given clientId: %s and productId: %s are already in the Record", clientId, productId))
	}
	restriction := &Restriction{clientId: clientId, addedDate: time.Now().String(), product: &Product{name: productName, id: productId}}
	receiver.restrictions[RestrictionKey{clientId: clientId, productId: productId}] = restriction
	return nil
}

func (receiver *Record) DeleteRestriction(clientId, productId string) (int, error) {
	_, present := receiver.restrictions[RestrictionKey{clientId: clientId, productId: productId}]
	if !present {
		return 0, errors.New(fmt.Sprintf("the given clientId: %s and productId: %s are not in the Record", clientId, productId))
	}
	delete(receiver.restrictions, RestrictionKey{clientId: clientId, productId: productId})
	return len(receiver.restrictions), nil
}
