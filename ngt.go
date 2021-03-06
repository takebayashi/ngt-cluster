package main

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/yahoojapan/gongt"

	"github.com/hashicorp/raft"
)

type RPCMessage struct {
	Op      string `json:"op"`
	Payload []byte `json:"payload"`
}

type NGTState struct {
	dimension int
	dir       string
	ngt       *gongt.NGT
	meta      map[string]string
	mu        *sync.Mutex
}

func NewNGTState(dir string, dimension int) (*NGTState, error) {
	m := &NGTState{dir: dir, dimension: dimension, meta: map[string]string{}, mu: &sync.Mutex{}}
	m.emptyIndexDir()
	return m, m.loadIndex(true)
}

func (s *NGTState) search(v []float64, n int, e float64) ([]gongt.SearchResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ngt.Search(v, n, e)
}

func (s *NGTState) insert(v []float64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ngt.InsertCommit(v, runtime.GOMAXPROCS(0))
}

func (s *NGTState) remove(i int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ngt.Remove(i); err != nil {
		return nil
	}
	return s.ngt.CreateAndSaveIndex(runtime.GOMAXPROCS(0))
}

// raft.FSM

func (s *NGTState) Apply(l *raft.Log) interface{} {
	rm := RPCMessage{}
	if err := json.Unmarshal(l.Data, &rm); err != nil {
		panic(err)
	}
	if rm.Op == "insert" {
		im := InsertRequest{}
		if err := json.Unmarshal(rm.Payload, &im); err != nil {
			panic(err)
		}
		if idx, err := s.insert(im.Vector); err != nil {
			panic(err)
		} else {
			return idx
		}
	} else if rm.Op == "remove" {
		req := RemoveRequest{}
		if err := json.Unmarshal(rm.Payload, &req); err != nil {
			panic(err)
		}
		if err := s.remove(req.ID); err != nil {
			panic(err)
		}
		return nil
	} else {
		panic("unknown op: " + rm.Op)
	}
}

func (s *NGTState) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ngt.SaveIndex(); err != nil {
		return nil, err
	}
	files, err := ioutil.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	archived := &bytes.Buffer{}
	tw := tar.NewWriter(archived)
	for _, f := range files {
		hdr := &tar.Header{
			Name: f.Name(),
			Mode: 0644,
			Size: f.Size(),
		}
		tw.WriteHeader(hdr)
		fr, err := os.Open(filepath.Join(s.dir, f.Name()))
		if err != nil {
			return nil, err
		}
		defer fr.Close()
		io.Copy(tw, fr)
	}
	return &NGTStateSnapshot{archived.Bytes()}, nil
}

func (s *NGTState) Restore(r io.ReadCloser) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emptyIndexDir()
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		outname := filepath.Join(s.dir, hdr.Name)
		outf, err := os.OpenFile(outname, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer outf.Close()
		if _, err := io.Copy(outf, tr); err != nil {
			return err
		}
	}
	return s.loadIndex(true)
}

func (s *NGTState) ensureIndexClosed() {
	if s.ngt != nil {
		s.ngt.Close()
		s.ngt = nil
	}
}

func (s *NGTState) emptyIndexDir() error {
	s.ensureIndexClosed()
	if err := os.RemoveAll(s.dir); err != nil {
		return nil
	}
	return os.MkdirAll(s.dir, os.ModePerm)
}

func (s *NGTState) loadIndex(save bool) error {
	s.ensureIndexClosed()
	s.ngt = gongt.New(s.dir).SetDimension(s.dimension).Open()
	if errs := s.ngt.GetErrors(); len(errs) > 0 {
		return errs[0]
	}
	if save {
		return s.ngt.CreateAndSaveIndex(1)
	}
	return nil
}

type NGTStateSnapshot struct {
	tar []byte
}

// raft.FSMSnapshot

func (s *NGTStateSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	_, err := io.Copy(sink, bytes.NewBuffer(s.tar))
	return err
}

func (*NGTStateSnapshot) Release() {

}
