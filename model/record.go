package model

import (
	blacklist "blacklist/protos"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Record struct {
	id           string
	restrictions []*Restriction
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

func FromDynamoItem(item map[string]*dynamodb.AttributeValue) (*Record, error) {
	restrictionsIndex := 0
	restrictionsList := make([]*Restriction, 0, len(item["restrictions"].L))
	for restrictionsIndex < len(item["restrictions"].L) {
		product := &Product{
			name: *item["restrictions"].L[restrictionsIndex].M["product"].M["name"].S,
			id:   *item["restrictions"].L[restrictionsIndex].M["product"].M["id"].S,
		}
		restriction := &Restriction{
			clientId:  *item["restrictions"].L[restrictionsIndex].M["clientId"].S,
			addedDate: *item["restrictions"].L[restrictionsIndex].M["addedDate"].S,
			product:   product,
		}
		restrictionsList = append(restrictionsList, restriction)
	}
	record := &Record{
		id:           *item["id"].S,
		restrictions: restrictionsList,
	}
	return record, nil
}

func FromDto(dto *blacklist.BlacklistRecordDto) (*Record, error) {
	restrictionsIndex := 0
	restrictionsList := make([]*Restriction, 0, len(dto.Restrictions))
	for restrictionsIndex < len(dto.Restrictions) {
		product := &Product{
			name: dto.Restrictions[restrictionsIndex].Product.ProductName,
			id:   dto.Restrictions[restrictionsIndex].Product.ProductId,
		}
		restriction := &Restriction{
			clientId:  dto.Restrictions[restrictionsIndex].ClientId,
			addedDate: dto.Restrictions[restrictionsIndex].AddedDate,
			product:   product,
		}
		restrictionsList = append(restrictionsList, restriction)
		restrictionsIndex++
	}
	return &Record{
		id:           dto.Id,
		restrictions: restrictionsList,
	}, nil
}

func (receiver *Record) ToDynamoItem() map[string]*dynamodb.AttributeValue {
	restrictions := make([]*dynamodb.AttributeValue, 0, len(receiver.restrictions))
	restrictionIndex := 0
	for restrictionIndex < len(receiver.restrictions) {
		product := make(map[string]*dynamodb.AttributeValue)
		product["name"] = &dynamodb.AttributeValue{S: &receiver.restrictions[restrictionIndex].product.name}
		product["id"] = &dynamodb.AttributeValue{S: &receiver.restrictions[restrictionIndex].product.id}
		restriction := make(map[string]*dynamodb.AttributeValue)
		restriction["clientId"] = &dynamodb.AttributeValue{S: &receiver.restrictions[restrictionIndex].clientId}
		restriction["addedDate"] = &dynamodb.AttributeValue{S: &receiver.restrictions[restrictionIndex].addedDate}
		restriction["product"] = &dynamodb.AttributeValue{M: product}
		restrictions = append(restrictions, &dynamodb.AttributeValue{M: restriction})
	}
	record := make(map[string]*dynamodb.AttributeValue)
	record["id"] = &dynamodb.AttributeValue{S: &receiver.id}
	record["restrictions"] = &dynamodb.AttributeValue{L: restrictions}
	return record
}

func (receiver *Record) ToDto() *blacklist.BlacklistRecordDto {
	restrictionsIndex := 0
	restrictionsDtoList := make([]*blacklist.RestrictionDto, 0, len(receiver.restrictions))
	for restrictionsIndex < len(receiver.restrictions) {
		productDto := &blacklist.ProductDto{
			ProductId:   receiver.restrictions[restrictionsIndex].product.id,
			ProductName: receiver.restrictions[restrictionsIndex].product.name,
		}
		restrictionDto := &blacklist.RestrictionDto{
			ClientId:  receiver.restrictions[restrictionsIndex].clientId,
			AddedDate: receiver.restrictions[restrictionsIndex].addedDate,
			Product:   productDto,
		}
		restrictionsDtoList = append(restrictionsDtoList, restrictionDto)
		restrictionsIndex++
	}
	return &blacklist.BlacklistRecordDto{
		Id:           receiver.id,
		Restrictions: restrictionsDtoList,
	}
}
