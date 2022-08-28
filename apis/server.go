package apis

import (
	"blacklist/models"
	"blacklist/pkg/clients"
	"blacklist/tools/protos"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"io"
	"sync"
)

var (
	notFound          = "given record %s does not exist"
	maxLengthExceeded = "maximum batch size is %d and given batch has %d records"
	idFormat          = "%s:%s:%s"
)

func getIdFromRequest(request *blacklist.BlacklistRecordOperationRequest) string {
	return fmt.Sprintf(idFormat, request.RecordId, request.ClientId, request.ClientId)
}

type BlacklistServer struct {
	blacklist.UnimplementedBlacklistServer
	mu        sync.Mutex
	BatchSize int
	Table     string
}

func (receiver *BlacklistServer) GetBlacklistRecord(_ context.Context, request *blacklist.BlacklistRecordOperationRequest) (*blacklist.BlacklistRecordDto, error) {
	client, err := clients.NewClient(receiver.Table)
	if err != nil {
		return nil, err
	}
	id := getIdFromRequest(request)
	result, err := client.GetRecordById(&id)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New(fmt.Sprintf(notFound, id))
	}
	return result.ToDto(), nil
}

func (receiver *BlacklistServer) GetBlacklistRecordBatch(stream blacklist.Blacklist_GetBlacklistRecordBatchServer) error {
	client, err := clients.NewClient(receiver.Table)
	if err != nil {
		return err
	}
	for {
		in, err := stream.Recv()
		ids := make([]*string, 0, receiver.BatchSize)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if len(in.Requests) > receiver.BatchSize {
			return errors.New(fmt.Sprintf(maxLengthExceeded, receiver.BatchSize, len(in.Requests)))
		}
		for _, request := range in.Requests {
			id := getIdFromRequest(request)
			ids = append(ids, &id)
		}
		records, err := client.GetRecordBatchByIds(ids)
		if err != nil {
			return err
		}
		for _, record := range records {
			err = stream.Send(record.ToDto())
			if err != nil {
				return err
			}
		}
	}
}

func (receiver *BlacklistServer) GetBlacklistRecordsQuery(request *blacklist.BlacklistRecordQueriesRequest, stream blacklist.Blacklist_GetBlacklistRecordsQueryServer) error {
	client, err := clients.NewClient(receiver.Table)
	if err != nil {
		return err
	}
	queries := make([]*models.Query, 0, 10)
	betweenQueries := make([]*models.BetweenQuery, 0, 10)
	for _, query := range request.Queries {
		queries = append(queries, models.FromQueryRequest(query))
	}
	for _, betweenQuery := range request.BetweenQueries {
		betweenQueries = append(betweenQueries, models.FromQueryBetweenRequest(betweenQuery))
	}
	var result []*models.Record
	var lastRecord map[string]*dynamodb.AttributeValue
	for result, lastRecord, err = client.GetRecordsByQueries(queries, betweenQueries, nil); lastRecord != nil; result, lastRecord, err = client.GetRecordsByQueries(queries, betweenQueries, lastRecord) {
		if err != nil {
			return err
		}
		for _, record := range result {
			err := stream.Send(record.ToDto())
			if err != nil {
				return err
			}
		}
	}
	if err != nil {
		return err
	}
	for _, record := range result {
		err := stream.Send(record.ToDto())
		if err != nil {
			return err
		}
	}
	return nil
}

func (receiver *BlacklistServer) SaveBlacklistRecord(_ context.Context, request *blacklist.BlacklistRecordOperationRequest) (*blacklist.BlacklistRecordDto, error) {
	client, err := clients.NewClient(receiver.Table)
	if err != nil {
		return nil, err
	}
	record, err := client.SaveRecord(models.NewRecord(request.RecordId, request.ClientId, request.ProductId))
	if err != nil {
		return nil, err
	}
	return record.ToDto(), nil
}

func (receiver *BlacklistServer) SaveBlacklistRecordBatch(stream blacklist.Blacklist_SaveBlacklistRecordBatchServer) error {
	client, err := clients.NewClient(receiver.Table)
	if err != nil {
		return err
	}
	for {
		in, err := stream.Recv()
		records := make([]*models.Record, 0, receiver.BatchSize)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if len(in.Requests) > receiver.BatchSize {
			return errors.New(fmt.Sprintf(maxLengthExceeded, receiver.BatchSize, len(in.Requests)))
		}
		for _, request := range in.Requests {
			record := models.NewRecord(request.RecordId, request.ClientId, request.ProductId)
			records = append(records, record)
		}
		result, err := client.SaveBatchRecords(records)
		if err != nil {
			return err
		}
		for _, recordResult := range result {
			err = stream.Send(recordResult.ToDto())
			if err != nil {
				return err
			}
		}
	}
}

func (receiver *BlacklistServer) DeleteBlacklistRecord(_ context.Context, request *blacklist.BlacklistRecordOperationRequest) (*blacklist.Empty, error) {
	client, err := clients.NewClient(receiver.Table)
	if err != nil {
		return nil, err
	}
	id := getIdFromRequest(request)
	err = client.DeleteRecord(&id)
	if err != nil {
		return nil, err
	}
	return &blacklist.Empty{}, nil
}

func (receiver *BlacklistServer) DeleteBatchBlacklistRecord(stream blacklist.Blacklist_DeleteBatchBlacklistRecordServer) error {
	client, err := clients.NewClient(receiver.Table)
	if err != nil {
		return err
	}
	for {
		in, err := stream.Recv()
		ids := make([]*string, 0, receiver.BatchSize)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if len(in.Requests) > receiver.BatchSize {
			return errors.New(fmt.Sprintf(maxLengthExceeded, receiver.BatchSize, len(in.Requests)))
		}
		for _, request := range in.Requests {
			id := getIdFromRequest(request)
			ids = append(ids, &id)
		}
		err = client.DeleteBatchRecords(ids)
		if err != nil {
			return err
		}
	}
}
