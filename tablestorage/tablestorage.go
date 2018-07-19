package tablestorage

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/cenkalti/backoff"
	"github.com/vanbroup/gohaveazurestorage/gohaveazurestoragecommon"
)

type TableStorage struct {
	http             *gohaveazurestoragecommon.HTTP
	baseUrl          string
	secondaryBaseUrl string
}

type Query struct {
	ts               *TableStorage
	table            string
	selects          string
	filter           string
	top              string
	nextPartitionKey string
	nextRowKey       string
	lastError        error
	rows             chan json.RawMessage
	run              sync.Once
	close            chan bool
}

type entitiesResponse struct {
	Value []json.RawMessage
}

func New(http *gohaveazurestoragecommon.HTTP) *TableStorage {
	return &TableStorage{http: http}
}

func (tableStorage *TableStorage) GetTableACL(tableName string) (*gohaveazurestoragecommon.SignedIdentifiers, error) {
	body, _, err := tableStorage.http.Request("GET", tableName+"?comp=acl", "", nil, false, false, false, false)
	if err != nil {
		return nil, err
	}

	signedIdentifiers := &gohaveazurestoragecommon.SignedIdentifiers{}
	desrializeXML([]byte(body), signedIdentifiers)

	return signedIdentifiers, nil
}

func (tableStorage *TableStorage) SetTableACL(tableName string, signedIdentifiers *gohaveazurestoragecommon.SignedIdentifiers) error {
	xmlBytes, _ := xml.MarshalIndent(signedIdentifiers, "", "")
	_, _, err := tableStorage.http.Request("PUT", tableName+"?comp=acl", "", xmlBytes, false, false, true, false)
	return err
}

func (tableStorage *TableStorage) GetTableServiceProperties() (*gohaveazurestoragecommon.StorageServiceProperties, error) {
	body, _, err := tableStorage.http.Request("GET", "?comp=properties", "&restype=service", nil, false, false, true, false)
	if err != nil {
		return nil, err
	}

	storageServiceProperties := &gohaveazurestoragecommon.StorageServiceProperties{}
	desrializeXML([]byte(body), storageServiceProperties)
	return storageServiceProperties, nil
}

func (tableStorage *TableStorage) SetTableServiceProperties(storageServiceProperties *gohaveazurestoragecommon.StorageServiceProperties) error {
	xmlBytes, _ := xml.MarshalIndent(storageServiceProperties, "", "")
	_, _, err := tableStorage.http.Request("PUT", "?comp=properties", "&restype=service", append([]byte("<?xml version=\"1.0\" encoding=\"utf-8\"?>"), xmlBytes...), false, false, false, false)
	return err
}

func (tableStorage *TableStorage) GetTableServiceStats() (*gohaveazurestoragecommon.StorageServiceStats, error) {
	body, _, err := tableStorage.http.Request("GET", "?comp=stats", "&restype=service", nil, false, false, false, true)
	if err != nil {
		return nil, err
	}

	storageServiceStats := &gohaveazurestoragecommon.StorageServiceStats{}
	desrializeXML([]byte(body), storageServiceStats)

	return storageServiceStats, nil
}

func (tableStorage *TableStorage) QueryEntity(tableName string, partitionKey string, rowKey string, selects string) ([]byte, error) {
	body, _, err := tableStorage.http.Request("GET", tableName+"%28PartitionKey=%27"+partitionKey+"%27,RowKey=%27"+rowKey+"%27%29", "?$select="+selects, nil, false, true, false, false)
	return body, err
}

func (tableStorage *TableStorage) QueryEntities(tableName string, selects string, filter string, top string) ([]byte, error) {
	filter = strings.Replace(filter, " ", "%20", -1)
	body, _, err := tableStorage.http.Request("GET", tableName, "?$filter="+filter+"&$select="+selects+"&$top="+top, nil, false, true, false, false)
	return body, err
}

func (tableStorage *TableStorage) InsertEntity(tableName string, json []byte) error {
	_, _, err := tableStorage.http.Request("POST", tableName, "", json, false, true, false, false)
	return err
}

func (tableStorage *TableStorage) DeleteEntity(tableName string, partitionKey string, rowKey string) error {
	_, _, err := tableStorage.executeEntityRequest("DELETE", tableName, partitionKey, rowKey, nil, true)
	return err
}

func (tableStorage *TableStorage) UpdateEntity(tableName string, partitionKey string, rowKey string, json []byte) error {
	_, _, err := tableStorage.executeEntityRequest("PUT", tableName, partitionKey, rowKey, json, true)
	return err
}

func (tableStorage *TableStorage) MergeEntity(tableName string, partitionKey string, rowKey string, json []byte) error {
	_, _, err := tableStorage.executeEntityRequest("MERGE", tableName, partitionKey, rowKey, json, true)
	return err
}

func (tableStorage *TableStorage) InsertOrMergeEntity(tableName string, partitionKey string, rowKey string, json []byte) error {
	_, _, err := tableStorage.executeEntityRequest("MERGE", tableName, partitionKey, rowKey, json, false)
	return err
}

func (tableStorage *TableStorage) InsertOrReplaceEntity(tableName string, partitionKey string, rowKey string, json []byte) error {
	_, _, err := tableStorage.executeEntityRequest("PUT", tableName, partitionKey, rowKey, json, false)
	return err
}

type CreateTableArgs struct {
	TableName string
}

func (tableStorage *TableStorage) CreateTable(tableName string) error {
	json, _ := json.Marshal(CreateTableArgs{TableName: tableName})
	_, _, err := tableStorage.http.Request("POST", "Tables", "", json, false, true, false, false)
	return err
}

func (tableStorage *TableStorage) DeleteTable(tableName string) error {
	_, _, err := tableStorage.http.Request("DELETE", "Tables%28%27"+tableName+"%27%29", "", nil, false, false, true, false)
	return err
}

func (tableStorage *TableStorage) QueryTables() ([]byte, error) {
	body, _, err := tableStorage.http.Request("GET", "Tables", "", nil, false, true, false, false)
	return body, err
}

func (tableStorage *TableStorage) executeEntityRequest(httpVerb string, tableName string, partitionKey string, rowKey string, json []byte, useIfMatch bool) ([]byte, *http.Header, error) {
	return tableStorage.http.Request(httpVerb, tableName+"%28PartitionKey=%27"+partitionKey+"%27,RowKey=%27"+rowKey+"%27%29", "", json, useIfMatch, false, false, false)
}

func (tableStorage *TableStorage) NewQuery() *Query {
	// TODO: set rows to batch size
	return &Query{
		ts:    tableStorage,
		rows:  make(chan json.RawMessage, 1000),
		run:   sync.Once{},
		close: make(chan bool),
	}
}

func (q *Query) SetNextPartitionKey(key string) *Query {
	q.nextPartitionKey = key
	return q
}

func (q *Query) GetNextPartitionKey() string {
	return q.nextPartitionKey
}

func (q *Query) SetNextRowKey(key string) *Query {
	q.nextRowKey = key
	return q
}

func (q *Query) GetNextRowKey() string {
	return q.nextRowKey
}

func (q *Query) Filter(f string) *Query {
	q.filter = f
	return q
}

func (q *Query) Select(s string) *Query {
	q.selects = s
	return q
}

func (q *Query) Table(t string) *Query {
	q.table = t
	return q
}

func (q *Query) Top(t string) *Query {
	q.top = t
	return q
}

func (q *Query) Error() error {
	return q.lastError
}

func (q *Query) Close() {
	q.close <- true
	return
}

func (q *Query) Next(result interface{}) (more bool) {
	q.run.Do(q.getNext)

	r, more := <-q.rows
	if !more {
		return false
	}

	if err := json.Unmarshal(r, &result); err != nil {
		q.lastError = err
	}

	return true

}

func (q *Query) getNext() {
	go func() {
		for {
			select {
			default:
				exp := backoff.NewExponentialBackOff()
				ticker := backoff.NewTicker(exp)
				for range ticker.C {
					filter := strings.Replace(q.filter, " ", "%20", -1)

					var next string
					if q.nextRowKey != "" {
						next = next + "&NextRowKey=" + q.nextRowKey
					}
					if q.nextPartitionKey != "" {
						next = next + "&NextPartitionKey=" + q.nextPartitionKey
					}

					result, header, err := q.ts.http.Request("GET", q.table, "?$filter="+filter+"&$select="+q.selects+"&$top="+q.top+next, nil, false, true, false, false)
					if err != nil {
						q.lastError = fmt.Errorf("%s, retry in %s", err, exp.NextBackOff())
						continue
					}

					if result != nil && len(result) > 0 {
						var resp entitiesResponse
						if err := json.Unmarshal(result, &resp); err != nil {
							q.lastError = fmt.Errorf("%s, retry in %s", err, exp.NextBackOff())
							continue
						}

						for _, r := range resp.Value {
							q.rows <- r
						}
					}

					if header != nil {
						if q.nextRowKey == header.Get("X-Ms-Continuation-Nextrowkey") {
							q.lastError = fmt.Errorf("nextRowKey did not change, retry in %s", exp.NextBackOff())
						}
						q.nextPartitionKey = header.Get("X-Ms-Continuation-Nextpartitionkey")
						q.nextRowKey = header.Get("X-Ms-Continuation-Nextrowkey")
					}

					// end of backoff retry loop
					break
				}

			case <-q.close:
				break
			}

			if q.nextPartitionKey == "" || q.nextRowKey == "" {
				break
			}
		}

		close(q.rows)
	}()
}

func desrializeXML(bytes []byte, object interface{}) {
	err := xml.Unmarshal([]byte(bytes), &object)
	if err != nil {
		fmt.Printf("%s", err)
		os.Exit(1)
	}
}
