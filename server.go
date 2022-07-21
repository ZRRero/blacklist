package main

import (
	"blacklist/clients"
	"blacklist/model"
	blacklist "blacklist/protos"
	"context"
	"io"
	"sync"
)

type BlacklistServer struct {
	blacklist.UnimplementedBlacklistServer
	mu        sync.Mutex
	batchSize int
	table     string
}

func (receiver *BlacklistServer) GetBlacklistRecordById(_ context.Context, request *blacklist.BlacklistRequest) (*blacklist.BlacklistRecordDto, error) {
	client, err := clients.NewClient(receiver.table)
	if err != nil {
		return nil, err
	}
	record, err := client.GetRecordById(&request.Id)
	if err != nil {
		return nil, err
	}
	return record.ToDto(), nil
}

func (receiver *BlacklistServer) GetBlacklistRecordBatch(stream blacklist.Blacklist_GetBlacklistRecordBatchServer) error {
	in, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	ids := make([]*string, 0, receiver.batchSize)
	for err != io.EOF {
		ids = append(ids, &in.Id)
		if len(ids) > receiver.batchSize {
			dtos, err := receiver.getFullRecordsBatch(ids)
			dtoIndex := 0
			for dtoIndex < len(dtos) {
				err = stream.Send(dtos[dtoIndex])
				if err != nil {
					return err
				}
				dtoIndex++
			}
			ids = make([]*string, 0, receiver.batchSize)
		}
		in, err = stream.Recv()
		if err != nil && err != io.EOF {
			return err
		}
	}
	return nil
}

func (receiver *BlacklistServer) getFullRecordsBatch(ids []*string) ([]*blacklist.BlacklistRecordDto, error) {
	client, err := clients.NewClient(receiver.table)
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

func (receiver *BlacklistServer) parseDynamoRecordsToBlacklistDto(records []*model.Record) ([]*blacklist.BlacklistRecordDto, error) {
	result := make([]*blacklist.BlacklistRecordDto, 0, receiver.batchSize)
	index := 0
	for index < len(records) {
		result = append(result, records[index].ToDto())
		index++
	}
	return result, nil
}

func (receiver *BlacklistServer) SaveBlacklistRecord(_ context.Context, request *blacklist.BlacklistRecordDto) (*blacklist.BlacklistRecordDto, error) {
	client, err := clients.NewClient(receiver.table)
	if err != nil {
		return nil, err
	}
	record, err := model.FromDto(request)
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
	in, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	records := make([]*model.Record, 0, receiver.batchSize)
	for err != io.EOF {
		dto, err := model.FromDto(in)
		if err != nil {
			return err
		}
		records = append(records, dto)
		if len(records) > receiver.batchSize {
			dtos, err := receiver.saveRecordsBatch(records)
			dtoIndex := 0
			for dtoIndex < len(dtos) {
				err = stream.Send(dtos[dtoIndex])
				if err != nil {
					return err
				}
				dtoIndex++
			}
			records = make([]*model.Record, 0, receiver.batchSize)
		}
		in, err = stream.Recv()
		if err != nil && err != io.EOF {
			return err
		}
	}
	return nil
}

func (receiver *BlacklistServer) saveRecordsBatch(records []*model.Record) ([]*blacklist.BlacklistRecordDto, error) {
	client, err := clients.NewClient(receiver.table)
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
