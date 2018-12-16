package oss

import (
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type Config struct {
	Endpoint        string
	AccessKeyID     string
	AccessKeySecret string
	BucketName      string
}

type OssBucket struct {
	Config
	Client *oss.Client
}

func NewOssBucket(config Config, client *oss.Client) *OssBucket {
	return &OssBucket{Config: config, Client: client}
}

func NewOssDatastore(config Config) (*OssBucket, error) {
	client, err := oss.New(config.Endpoint, config.AccessKeyID, config.AccessKeySecret)
	if err != nil {
		return nil, fmt.Errorf("failed to create new client: %s", err)
	}
	return NewOssBucket(config, client), nil
}
