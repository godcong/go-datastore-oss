package oss

import (
	"bytes"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"strconv"
)

const (
	// listMax is the largest amount of objects you can request from S3 in a list
	// call.
	listMax = 1000

	// deleteMax is the largest amount of objects you can delete from S3 in a
	// delete objects call.
	deleteMax = 1000

	defaultWorkers = 100
)

var _ ds.Datastore = (*datastore)(nil)

type Config struct {
	Endpoint        string
	AccessKeyID     string
	AccessKeySecret string
	BucketName      string
}

type datastore struct {
	Config
	Bucket *oss.Bucket
}

func (s *datastore) GetSize(key ds.Key) (size int, err error) {
	headers, err := s.Bucket.GetObjectMeta(key.String())
	if err != nil {
		if err.Error() == "NoSuchKey" {
			return -1, ds.ErrNotFound
		}
		return -1, err
	}
	length := headers.Get("Content-Length")
	u, err := strconv.ParseUint(length, 10, 64)
	if err != nil {
		return -1, err
	}
	return int(u), nil
}

func (s *datastore) Close() error {
	return nil
}

func newDataStore(config Config, bucket *oss.Bucket) *datastore {
	return &datastore{Config: config, Bucket: bucket}
}

func NewOssDatastore(config Config) (*datastore, error) {
	client, err := oss.New(config.Endpoint, config.AccessKeyID, config.AccessKeySecret)
	if err != nil {
		return nil, fmt.Errorf("failed to create new client: %s", err)
	}

	bucket, err := client.Bucket(config.BucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket: %s", err)
	}
	return newDataStore(config, bucket), nil
}

func (s *datastore) Put(key ds.Key, value []byte) error {
	return s.Bucket.PutObject(key.String(), bytes.NewBuffer(value))
}

func (s *datastore) Get(key ds.Key) (value []byte, err error) {
	val, err := s.Bucket.GetObject(key.String())
	if err != nil {
		return nil, err
	}
	_, err = val.Read(value)
	return value, err
}

func (s *datastore) Has(key ds.Key) (exists bool, err error) {
	return s.Bucket.IsObjectExist(key.String())
}

func (s *datastore) Delete(key ds.Key) error {
	return s.Bucket.DeleteObject(key.String())
}

func (s *datastore) Query(q query.Query) (query.Results, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("s3ds: filters or orders are not supported")
	}

	limit := q.Limit + q.Offset
	if limit == 0 || limit > listMax {
		limit = listMax
	}
	lsRes, err := s.Bucket.ListObjects(oss.MaxKeys(limit), oss.Prefix(q.Prefix))
	if err != nil {
		return nil, err
	}

	index := q.Offset
	nextValue := func() (query.Result, bool) {
		for index >= len(lsRes.Objects) {
			if !lsRes.IsTruncated {
				return query.Result{}, false
			}

			index -= len(lsRes.Objects)

			lsRes, err = s.Bucket.ListObjects(
				oss.Prefix(q.Prefix),
				oss.MaxKeys(listMax),
				oss.Delimiter("/"),
				oss.Marker(lsRes.NextMarker),
			)
			if err != nil {
				return query.Result{Error: err}, false
			}
		}

		entry := query.Entry{
			Key: ds.NewKey(lsRes.Objects[index].Key).String(),
		}
		if !q.KeysOnly {
			value, err := s.Get(ds.NewKey(entry.Key))
			if err != nil {
				return query.Result{Error: err}, false
			}
			entry.Value = value
		}

		index++
		return query.Result{Entry: entry}, true
	}

	return query.ResultsFromIterator(q, query.Iterator{
		Close: func() error {
			return nil
		},
		Next: nextValue,
	}), nil
}
