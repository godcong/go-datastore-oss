package oss

import (
	"bytes"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("flatfs")

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

func NewOssBucket(config Config, bucket *oss.Bucket) *datastore {
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
	return NewOssBucket(config, bucket), nil
}

func (s *datastore) Put(k ds.Key, value []byte) error {
	return s.Bucket.PutObject(k.String(), bytes.NewBuffer(value))
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

}
