package oss

import (
	"bytes"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"path"
	"strconv"
	"strings"
	"sync"
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
	RootDirectory   string
	Workers         int
}

type datastore struct {
	Config
	Bucket *oss.Bucket
}

func (s *datastore) Batch() (ds.Batch, error) {
	return &ossBatch{
		s:          s,
		ops:        make(map[string]batchOp),
		numWorkers: s.Workers,
	}, nil
}

func (s *datastore) GetSize(key ds.Key) (size int, err error) {
	headers, err := s.Bucket.GetObjectMeta(s.ossPath(key.String()))
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
	if config.Workers == 0 {
		config.Workers = defaultWorkers
	}

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
	fmt.Println("datastore put Path:", s.ossPath(key.String()))
	return s.Bucket.PutObject(s.ossPath(key.String()), bytes.NewBuffer(value))
}

func (s *datastore) Get(key ds.Key) (value []byte, err error) {
	fmt.Println("datastore get Path:", s.ossPath(key.String()))
	val, err := s.Bucket.GetObject(s.ossPath(key.String()))
	if err != nil {
		return nil, err
	}
	_, err = val.Read(value)
	return value, err
}

func (s *datastore) Has(key ds.Key) (exists bool, err error) {
	fmt.Println("datastore check Path:", s.ossPath(key.String()))
	return s.Bucket.IsObjectExist(s.ossPath(key.String()))
}

func (s *datastore) Delete(key ds.Key) error {
	fmt.Println("datastore delete Path:", s.ossPath(key.String()))
	return s.Bucket.DeleteObject(s.ossPath(key.String()))
}

func (s *datastore) Query(q query.Query) (query.Results, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("ossds: filters or orders are not supported")
	}

	limit := q.Limit + q.Offset
	if limit == 0 || limit > listMax {
		limit = listMax
	}
	lsRes, err := s.Bucket.ListObjects(oss.MaxKeys(limit), oss.Prefix(s.ossPath(q.Prefix)))
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
				oss.Prefix(s.ossPath(q.Prefix)),
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

func (s *datastore) ossPath(p string) string {
	return path.Join(s.RootDirectory, p)
}

type ossBatch struct {
	s          *datastore
	ops        map[string]batchOp
	numWorkers int
}

type batchOp struct {
	val    []byte
	delete bool
}

func (b *ossBatch) Put(key ds.Key, val []byte) error {
	fmt.Println("put Key:", key.String())
	b.ops[key.String()] = batchOp{
		val:    val,
		delete: false,
	}
	return nil
}

func (b *ossBatch) Delete(key ds.Key) error {
	fmt.Println("delete Key:", key.String())
	b.ops[key.String()] = batchOp{
		val:    nil,
		delete: true,
	}
	return nil
}

func (b *ossBatch) Commit() error {
	var (
		deleteObjs []string
		putKeys    []ds.Key
	)
	for k, op := range b.ops {
		if op.delete {
			deleteObjs = append(deleteObjs, k)
		} else {
			putKeys = append(putKeys, ds.NewKey(k))
		}
	}

	numJobs := len(putKeys) + (len(deleteObjs) / deleteMax)
	jobs := make(chan func() error, numJobs)
	results := make(chan error, numJobs)

	numWorkers := b.numWorkers
	if numJobs < numWorkers {
		numWorkers = numJobs
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	defer wg.Wait()

	for w := 0; w < numWorkers; w++ {
		go func() {
			defer wg.Done()
			worker(jobs, results)
		}()
	}

	for _, k := range putKeys {
		jobs <- b.newPutJob(k, b.ops[k.String()].val)
	}

	if len(deleteObjs) > 0 {
		for i := 0; i < len(deleteObjs); i += deleteMax {
			limit := deleteMax
			if len(deleteObjs[i:]) < limit {
				limit = len(deleteObjs[i:])
			}

			jobs <- b.newDeleteJob(deleteObjs[i : i+limit])
		}
	}
	close(jobs)

	var errs []string
	for i := 0; i < numJobs; i++ {
		err := <-results
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("ossds: failed batch operation:\n%s", strings.Join(errs, "\n"))
	}

	return nil
}

func (b *ossBatch) newPutJob(key ds.Key, value []byte) func() error {
	return func() error {
		return b.s.Put(key, value)
	}
}

func (b *ossBatch) newDeleteJob(objs []string) func() error {
	return func() error {
		_, err := b.s.Bucket.DeleteObjects(objs, oss.DeleteObjectsQuiet(true))
		if err != nil {
			return err
		}
		return nil
	}
}

func worker(jobs <-chan func() error, results chan<- error) {
	for j := range jobs {
		results <- j()
	}
}

var _ ds.Batching = (*datastore)(nil)
