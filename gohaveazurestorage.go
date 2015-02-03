package gohaveazurestorage

import (
	"encoding/base64"
	"gohaveazurestorage/gohaveazurestoragecommon"
	"gohaveazurestorage/tablestorageproxy"
)

type GoHaveAzureStorage struct {
	account      string
	key          []byte
	dumpSessions bool
}

func New(account string, key string) *GoHaveAzureStorage {
	return NewWithDebug(account, key, false)
}

func NewWithDebug(account string, key string, dumpSessions bool) *GoHaveAzureStorage {
	decodedKey, _ := base64.StdEncoding.DecodeString(key)
	return &GoHaveAzureStorage{account: account, key: decodedKey, dumpSessions: dumpSessions}
}

func (goHaveAzureStorage *GoHaveAzureStorage) NewTableStorageProxy() *tablestorageproxy.TableStorageProxy {
	http := gohaveazurestoragecommon.NewHTTP("table", goHaveAzureStorage.account, goHaveAzureStorage.key, goHaveAzureStorage.dumpSessions)
	return tablestorageproxy.New(http)
}