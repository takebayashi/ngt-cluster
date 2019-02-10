package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/hashicorp/raft"
)

type InsertRequest struct {
	Vector []float64 `json:"vector"`
}

type InsertResponse struct {
	ID int `json:"id"`
}

type SearchRequest struct {
	Vector  []float64 `json:"vector"`
	Num     int       `json:"results"`
	Epsilon float64   `json:"epsilon"`
}

type RemoveRequest struct {
	ID int `json:"id"`
}

type MemberJoinRequest struct {
	RPCAddr  string `json:"rpc_addr"`
	HTTPAddr string `json:"http_addr"`
	ID       string `json:"id"`
}

var ErrNotLeader = errors.New("not leader")

type HTTPHandler struct {
	Raft  *raft.Raft
	State *NGTState
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/members" {
		h.handleJoin(w, r)
	} else if r.URL.Path == "/insert" {
		h.handleInsert(w, r)
	} else if r.URL.Path == "/search" {
		h.handleSearch(w, r)
	} else if r.URL.Path == "/remove" {
		h.handleRemove(w, r)
	}
}

func (h *HTTPHandler) writeError(w http.ResponseWriter, status int, err error) {
	w.WriteHeader(status)
	w.Write([]byte(err.Error()))
}

func (h *HTTPHandler) handleJoin(w http.ResponseWriter, r *http.Request) {
	if h.Raft.State() != raft.Leader {
		h.writeError(w, http.StatusInternalServerError, ErrNotLeader)
		return
	}
	m := &MemberJoinRequest{}
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	conf := h.Raft.GetConfiguration()
	if err := conf.Error(); err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	for _, server := range conf.Configuration().Servers {
		if server.ID == raft.ServerID(m.ID) && server.Address == raft.ServerAddress(m.RPCAddr) {
			w.WriteHeader(http.StatusOK)
			return
		} else if server.ID == raft.ServerID(m.ID) || server.Address == raft.ServerAddress(m.RPCAddr) {
			future := h.Raft.RemoveServer(server.ID, 0, 0)
			if err := future.Error(); err != nil {
				h.writeError(w, http.StatusInternalServerError, err)
				return
			}
		}
	}
	fut := h.Raft.AddVoter(raft.ServerID(m.ID), raft.ServerAddress(m.RPCAddr), 0, 0)
	if err := fut.Error(); err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
}

func (h *HTTPHandler) handleInsert(w http.ResponseWriter, r *http.Request) {
	if h.Raft.State() != raft.Leader {
		h.writeError(w, http.StatusInternalServerError, ErrNotLeader)
		return
	}
	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	serialized, err := json.Marshal(RPCMessage{Op: "insert", Payload: payload})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	fut := h.Raft.Apply(serialized, 1*time.Second)
	if err := fut.Error(); err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	json.NewEncoder(w).Encode(InsertResponse{ID: fut.Response().(int)})
}

func (h *HTTPHandler) handleSearch(w http.ResponseWriter, r *http.Request) {
	sr := SearchRequest{}
	if err := json.NewDecoder(r.Body).Decode(&sr); err != nil {
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	results, err := h.State.search(sr.Vector, sr.Num, sr.Epsilon)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	json.NewEncoder(w).Encode(results)
}

func (h *HTTPHandler) handleRemove(w http.ResponseWriter, r *http.Request) {
	if h.Raft.State() != raft.Leader {
		h.writeError(w, http.StatusInternalServerError, ErrNotLeader)
		return
	}
	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	serialized, err := json.Marshal(RPCMessage{Op: "remove", Payload: payload})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	fut := h.Raft.Apply(serialized, 1*time.Second)
	if fut.Error() != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}
