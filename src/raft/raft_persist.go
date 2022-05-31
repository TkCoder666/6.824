package raft

import (
	"bytes"

	"6.824/labgob"
)

const statePersistOffset int = 100

type RaftPersistState struct {
	CurrentTerm int
	VotedFor    int
}

func (rp *RaftPersister) serializeState(rf *Raft) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(RaftPersistState{
		CurrentTerm: rf.CurrentTerm,
		VotedFor:    rf.VotedFor,
	})
	if err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (rp *RaftPersister) serializeLog(rf *Raft) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.Log)
	if err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (rp *RaftPersister) persist(rf *Raft, persister *Persister) {
	stateBytes := rp.serializeState(rf)
	if len(stateBytes) > statePersistOffset {
		panic("serialized state byte count more than manually set boundary")
	}
	logBytes := rp.serializeLog(rf)
	image := make([]byte, statePersistOffset+len(logBytes))
	for i, b := range stateBytes {
		image[i] = b
	}
	for i, b := range logBytes {
		image[i+statePersistOffset] = b
	}
	persister.SaveRaftState(image)
}

func (rp *RaftPersister) loadState(rf *Raft, buffer []byte, offset int) {
	r := bytes.NewBuffer(buffer[offset:])
	d := labgob.NewDecoder(r)
	decoded := RaftPersistState{}
	err := d.Decode(&decoded)
	if err != nil {
		panic(err)
	}
	rf.CurrentTerm = decoded.CurrentTerm
	rf.VotedFor = decoded.VotedFor
}

func (rp *RaftPersister) loadLog(rf *Raft, buffer []byte, offset int) {
	r := bytes.NewBuffer(buffer[offset:])
	d := labgob.NewDecoder(r)
	decoded := make([]LogEntry, 0)
	err := d.Decode(&decoded)
	if err != nil {
		panic(err)
	}
	rf.Log = decoded
}
