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
	Id int `json:"id"`
}

type SearchRequest struct {
	Vector  []float64 `json:"vector"`
	Num     int       `json:"results"`
	Epsilon float64   `json:"epsilon"`
}

type RemoveRequest struct {
	Id int `json:"id"`
}

type MemberJoinRequest struct {
	RpcAddr  string `json:"rpc_addr"`
	HttpAddr string `json:"http_addr"`
	Id       string `json:"id"`
}

var NotLeaderError = errors.New("not leader")

type HttpHandler struct {
	Raft  *raft.Raft
	State *NGTState
}

func (h *HttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

func (h *HttpHandler) writeError(w http.ResponseWriter, status int, err error) {
	w.WriteHeader(status)
	w.Write([]byte(err.Error()))
}

func (h *HttpHandler) handleJoin(w http.ResponseWriter, r *http.Request) {
	if h.Raft.State() != raft.Leader {
		h.writeError(w, http.StatusInternalServerError, NotLeaderError)
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
		if server.ID == raft.ServerID(m.Id) && server.Address == raft.ServerAddress(m.RpcAddr) {
			w.WriteHeader(http.StatusOK)
			return
		} else if server.ID == raft.ServerID(m.Id) || server.Address == raft.ServerAddress(m.RpcAddr) {
			future := h.Raft.RemoveServer(server.ID, 0, 0)
			if err := future.Error(); err != nil {
				h.writeError(w, http.StatusInternalServerError, err)
				return
			}
		}
	}
	fut := h.Raft.AddVoter(raft.ServerID(m.Id), raft.ServerAddress(m.RpcAddr), 0, 0)
	if err := fut.Error(); err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
}

func (h *HttpHandler) handleInsert(w http.ResponseWriter, r *http.Request) {
	if h.Raft.State() != raft.Leader {
		h.writeError(w, http.StatusInternalServerError, NotLeaderError)
		return
	}
	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	serialized, err := json.Marshal(RpcMessage{Op: "insert", Payload: payload})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	fut := h.Raft.Apply(serialized, 1*time.Second)
	if err := fut.Error(); err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	json.NewEncoder(w).Encode(InsertResponse{Id: fut.Response().(int)})
}

func (h *HttpHandler) handleSearch(w http.ResponseWriter, r *http.Request) {
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

func (h *HttpHandler) handleRemove(w http.ResponseWriter, r *http.Request) {
	if h.Raft.State() != raft.Leader {
		h.writeError(w, http.StatusInternalServerError, NotLeaderError)
		return
	}
	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	serialized, err := json.Marshal(RpcMessage{Op: "remove", Payload: payload})
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
