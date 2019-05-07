package main

import (
	"fmt"
	ossds "github.com/godcong/go-datastore-oss"
	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
)

var Plugins = []plugin.Plugin{
	&OSSPlugin{},
}

type OSSPlugin struct{}

func main() {

}

func (oss OSSPlugin) Name() string {
	return "oss-datastore-plugin"
}

func (oss OSSPlugin) Version() string {
	return "0.0.1"
}

func (oss OSSPlugin) Init() error {
	return nil
}

func (oss OSSPlugin) DatastoreTypeName() string {
	return "ossds"
}

func (oss OSSPlugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(m map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		bucket, ok := m["bucket"].(string)
		if !ok {
			return nil, fmt.Errorf("ossds: no bucket specified")
		}

		idKey, ok := m["idKey"].(string)
		if !ok {
			return nil, fmt.Errorf("ossds: no idKey specified")
		}

		secretKey, ok := m["secretKey"].(string)
		if !ok {
			return nil, fmt.Errorf("ossds: no secretKey specified")
		}

		var endpoint string
		if v, ok := m["regionEndpoint"]; ok {
			endpoint, ok = v.(string)
			if !ok {
				return nil, fmt.Errorf("ossds: regionEndpoint not a string")
			}
		}
		var rootDirectory string
		if v, ok := m["rootDirectory"]; ok {
			rootDirectory, ok = v.(string)
			if !ok {
				return nil, fmt.Errorf("ossds: rootDirectory not a string")
			}
		}
		var workers int
		if v, ok := m["workers"]; ok {
			workersf, ok := v.(float64)
			workers = int(workersf)
			switch {
			case !ok:
				return nil, fmt.Errorf("ossds: workers not a number")
			case workers <= 0:
				return nil, fmt.Errorf("ossds: workers <= 0: %f", workersf)
			case float64(workers) != workersf:
				return nil, fmt.Errorf("ossds: workers is not an integer: %f", workersf)
			}
		}

		return &OSSConfig{
			cfg: ossds.Config{
				Endpoint:        endpoint,
				AccessKeyID:     idKey,
				AccessKeySecret: secretKey,
				BucketName:      bucket,
				RootDirectory:   rootDirectory,
				Workers:         workers,
			},
		}, nil
	}
}

type OSSConfig struct {
	cfg ossds.Config
}

func (ossc *OSSConfig) DiskSpec() fsrepo.DiskSpec {
	return fsrepo.DiskSpec{
		"bucket":        ossc.cfg.BucketName,
		"rootDirectory": ossc.cfg.RootDirectory,
	}
}

func (ossc *OSSConfig) Create(path string) (repo.Datastore, error) {
	return ossds.NewOssDatastore(ossc.cfg)
}
