package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	response := kvrpcpb.RawGetResponse{}

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		response.Error = err.Error()
		return &response, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	response.Value = val
	if err != nil {
		response.Error = err.Error()
	} else if val == nil {
		response.NotFound = true
	}

	return &response, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	response := kvrpcpb.RawPutResponse{}

	modify := storage.Modify{Data: storage.Put{Key: req.GetKey(), Value: req.GetValue(), Cf: req.GetCf()}}
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})

	if err != nil {
		response.Error = err.Error()
		return &response, err
	} else {
		return &response, nil
	}
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	response := kvrpcpb.RawDeleteResponse{}

	modify := storage.Modify{Data: storage.Delete{Key: req.GetKey(), Cf: req.GetCf()}}
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})
	if err != nil {
		response.Error = err.Error()
		return &response, err
	} else {
		return &response, nil
	}
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	response := kvrpcpb.RawScanResponse{Kvs: []*kvrpcpb.KvPair{}}

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		response.Error = err.Error()
		return &response, err
	}
	defer reader.Close()

	iterator := reader.IterCF(req.GetCf())
	defer iterator.Close()
	var cnt uint32
	cnt = 0
	for iterator.Seek(req.GetStartKey()); iterator.Valid() && cnt < req.GetLimit(); iterator.Next() {
		item := iterator.Item()
		k := item.KeyCopy(nil)
		v, err := item.ValueCopy(nil)
		if err != nil {
			response.Error = err.Error()
			return &response, err
		}

		response.Kvs = append(response.Kvs, &kvrpcpb.KvPair{Key: k, Value: v})
		cnt++
	}

	return &response, nil
}
