package models

import blacklist "blacklist/tools/protos"

type Query struct {
	Field   string
	Operand string
	Value   string
}

func FromQueryRequest(request *blacklist.BlacklistRecordQueryRequest) *Query {
	return &Query{Field: request.Field.String(), Operand: request.Operation.String(), Value: request.Value}
}

type BetweenQuery struct {
	Field string
	Init  string
	End   string
}

func FromQueryBetweenRequest(request *blacklist.BlacklistRecordBetweenRequest) *BetweenQuery {
	return &BetweenQuery{
		Field: request.Field.String(),
		Init:  request.Init,
		End:   request.End,
	}
}
