package apis

import (
	"blacklist/models"
	"blacklist/pkg/clients"
	"blacklist/tools/protos"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

var (
	notFound          = "given record %s does not exist"
	maxLengthExceeded = "maximum batch size is %d and given batch has %d records"
)

type BlacklistServer struct {
	blacklist.UnimplementedBlacklistServer
	mu        sync.Mutex
	BatchSize int
	Table     string
}

func (receiver *BlacklistServer) GetBlacklistRecordById(_ context.Context, request *blacklist.BlacklistGetRequest) (*blacklist.BlacklistRecordDto, error) {
	client, err := clients.NewClient(receiver.Table)
	if err != nil {
		return nil, err
	}
	record, err := client.GetRecordById(&request.Id)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, errors.New(fmt.Sprintf(notFound, request.Id))
	}
	return record.ToDto(), nil
}

func (receiver *BlacklistServer) GetBlacklistRecordBatch(stream blacklist.Blacklist_GetBlacklistRecordBatchServer) error {
	for {
		in, err := stream.Recv()
		ids := make([]*string, 0, receiver.BatchSize)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if len(in.Ids) > receiver.BatchSize {
			return errors.New(fmt.Sprintf(maxLengthExceeded, receiver.BatchSize, len(in.Ids)))
		}
		for _, id := range in.Ids {
			appendId := id
			ids = append(ids, &appendId)
		}
		dtos, err := receiver.getFullRecordsBatch(ids)
		for _, dto := range dtos {
			err = stream.Send(dto)
			if err != nil {
				return err
			}
		}
	}
}

func (receiver *BlacklistServer) getFullRecordsBatch(ids []*string) ([]*blacklist.BlacklistRecordDto, error) {
	client, err := clients.NewClient(receiver.Table)
	if err != nil {
		return nil, err
	}
	result, err := client.GetRecordBatchByIds(ids)
	if err != nil {
		return nil, err
	}
	dtos, err := receiver.parseDynamoRecordsToBlacklistDto(result)
	if err != nil {
		return nil, err
	}
	return dtos, nil
}

func (receiver *BlacklistServer) parseDynamoRecordsToBlacklistDto(records []*models.Record) ([]*blacklist.BlacklistRecordDto, error) {
	result := make([]*blacklist.BlacklistRecordDto, 0, receiver.BatchSize)
	index := 0
	for index < len(records) {
		result = append(result, records[index].ToDto())
		index++
	}
	return result, nil
}

func (receiver *BlacklistServer) SaveBlacklistRecord(_ context.Context, request *blacklist.BlacklistRecordDto) (*blacklist.BlacklistRecordDto, error) {
	client, err := clients.NewClient(receiver.Table)
	if err != nil {
		return nil, err
	}
	record, err := models.FromDto(request)
	if err != nil {
		return nil, err
	}
	result, err := client.SaveRecord(record)
	if err != nil {
		return nil, err
	}
	return result.ToDto(), nil
}

func (receiver *BlacklistServer) SaveBlacklistRecordBatch(stream blacklist.Blacklist_SaveBlacklistRecordBatchServer) error {
	for {
		records := make([]*models.Record, 0, receiver.BatchSize)
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if len(in.Requests) > receiver.BatchSize {
			return errors.New(fmt.Sprintf(maxLengthExceeded, receiver.BatchSize, len(in.Requests)))
		}
		for _, dto := range in.Requests {
			record, err := models.FromDto(dto)
			if err != nil {
				return err
			}
			records = append(records, record)
		}
		dtos, err := receiver.saveRecordsBatch(records)
		for _, dto := range dtos {
			err = stream.Send(dto)
			if err != nil {
				return err
			}
		}
	}
}

func (receiver *BlacklistServer) saveRecordsBatch(records []*models.Record) ([]*blacklist.BlacklistRecordDto, error) {
	client, err := clients.NewClient(receiver.Table)
	if err != nil {
		return nil, err
	}
	result, err := client.SaveBatchRecords(records)
	if err != nil {
		return nil, err
	}
	dtos, err := receiver.parseDynamoRecordsToBlacklistDto(result)
	if err != nil {
		return nil, err
	}
	return dtos, nil
}

func (receiver *BlacklistServer) SaveRestrictionIntoRecord(_ context.Context, request *blacklist.SaveRestrictionRequest) (*blacklist.BlacklistRecordDto, error) {
	client, err := clients.NewClient(receiver.Table)
	if err != nil {
		return nil, err
	}
	return receiver.saveRestrictionIntoRecord(client, request)
}

func (receiver *BlacklistServer) saveRestrictionIntoRecord(client *clients.Dynamo, request *blacklist.SaveRestrictionRequest) (*blacklist.BlacklistRecordDto, error) {
	//Race condition here
	record, err := client.GetRecordById(&request.RecordId)
	if err != nil {
		return nil, err
	}
	if record == nil {
		record = models.NewRecord(request.RecordId, make(map[models.RestrictionKey]*models.Restriction))
	}
	err = record.SaveRestriction(request.ClientId, request.Product.ProductId, request.Product.ProductName)
	if err != nil {
		return nil, err
	}
	result, err := client.SaveRecord(record)
	if err != nil {
		return nil, err
	}
	return result.ToDto(), nil
}

func (receiver *BlacklistServer) SaveBatchRestrictionIntoRecord(stream blacklist.Blacklist_SaveBatchRestrictionIntoRecordServer) error {
	client, err := clients.NewClient(receiver.Table)
	if err != nil {
		return err
	}
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		result, err := receiver.saveRestrictionIntoRecord(client, in)
		if err != nil {
			return err
		}
		err = stream.Send(result)
		if err != nil {
			return err
		}
	}
}

func (receiver *BlacklistServer) DeleteRestrictionFromRecord(_ context.Context, request *blacklist.DeleteRestrictionRequest) (*blacklist.BlacklistRecordDto, error) {
	client, err := clients.NewClient(receiver.Table)
	if err != nil {
		return nil, err
	}
	return receiver.deleteRestrictionFromRecord(client, request)
}

func (receiver *BlacklistServer) deleteRestrictionFromRecord(client *clients.Dynamo, request *blacklist.DeleteRestrictionRequest) (*blacklist.BlacklistRecordDto, error) {
	//Race condition here
	record, err := client.GetRecordById(&request.RecordId)
	if err != nil {
		return nil, err
	}
	resultingRestrictions, err := record.DeleteRestriction(request.ClientId, request.ProductId)
	if resultingRestrictions > 0 {
		record, err = client.SaveRecord(record)
		if err != nil {
			return nil, err
		}
	} else {
		if err = client.DeleteRecord(&request.RecordId); err != nil {
			return nil, err
		}
	}
	return record.ToDto(), nil
}

func (receiver *BlacklistServer) DeleteBatchRestrictionFromRecord(stream blacklist.Blacklist_DeleteBatchRestrictionFromRecordServer) error {
	client, err := clients.NewClient(receiver.Table)
	in, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	for err != io.EOF {
		result, err := receiver.deleteRestrictionFromRecord(client, in)
		if err != nil {
			return err
		}
		err = stream.Send(result)
		if err != nil {
			return err
		}
		in, err = stream.Recv()
		if err != nil && err != io.EOF {
			return err
		}
	}
	return nil
}
