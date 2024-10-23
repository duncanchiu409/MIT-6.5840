package kvsrv

import (
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvmap  map[string]string
	tokens map[string]string
}

func (kv *KVServer) CheckDuplication(tokenId string) bool {
	_, ok := kv.tokens[tokenId]
	if !ok {
		return false
	} else {
		return true
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.kvmap[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.kvmap[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	duplicated := kv.CheckDuplication(args.TokenId)

	if !duplicated {
		value, ok := kv.kvmap[args.Key]
		if ok {
			kv.tokens[args.TokenId] = value
			kv.kvmap[args.Key] = value + args.Value
			reply.Value = value
		} else {
			kv.tokens[args.TokenId] = ""
			kv.kvmap[args.Key] = args.Value
			reply.Value = ""
		}
	} else {
		oldValue := kv.tokens[args.TokenId]
		reply.Value = oldValue
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvmap = map[string]string{}
	kv.tokens = map[string]string{}
	return kv
}
