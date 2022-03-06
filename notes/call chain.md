server中包含raftstore

raftstore中

```golang
type RaftStorage struct {
	engines *engine_util.Engines
	config  *config.Config

	node          *raftstore.Node
	snapManager   *snap.SnapManager
	raftRouter    *raftstore.RaftstoreRouter
	raftSystem    *raftstore.Raftstore
	resolveWorker *worker.Worker
	snapWorker    *worker.Worker

	wg sync.WaitGroup
}

```

有读写请求时，先对request进行封装。

```golang

	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			reqs = append(reqs, &raft_cmdpb.Request{
				CmdType: raft_cmdpb.CmdType_Put,
				Put: &raft_cmdpb.PutRequest{
					Cf:    put.Cf,
					Key:   put.Key,
					Value: put.Value,
				}})
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			reqs = append(reqs, &raft_cmdpb.Request{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete: &raft_cmdpb.DeleteRequest{
					Cf:  delete.Cf,
					Key: delete.Key,
				}})
		}
	}

	header := &raft_cmdpb.RaftRequestHeader{
		RegionId:    ctx.RegionId,
		Peer:        ctx.Peer,
		RegionEpoch: ctx.RegionEpoch,
		Term:        ctx.Term,
	}
	request := &raft_cmdpb.RaftCmdRequest{
		Header:   header,
		Requests: reqs,
	}
```

