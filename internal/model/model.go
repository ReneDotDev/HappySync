// Package model defines the versioned, transport-independent synchronization model.
package model

const BlockSize = 4 * 1024 * 1024
const ProtocolVersion = 1

type Vector map[string]uint64
type Block struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}
type Manifest struct {
	Hash   string  `json:"hash"`
	Size   int64   `json:"size"`
	Blocks []Block `json:"blocks"`
}
type Record struct {
	Path      string   `json:"path"`
	ID        string   `json:"id"`
	Origin    string   `json:"origin"`
	Counter   uint64   `json:"counter"`
	Clock     Vector   `json:"clock"`
	Deleted   bool     `json:"deleted,omitempty"`
	Directory bool     `json:"directory,omitempty"`
	Imported  bool     `json:"imported,omitempty"`
	Manifest  Manifest `json:"manifest"`
	Mtime     int64    `json:"mtime,omitempty"`
}
type Entry struct {
	Path   string
	Record Record
}
type Baseline struct {
	Clock      Vector `json:"clock,omitempty"`
	Deleted    bool   `json:"deleted,omitempty"`
	SourceID   string `json:"source_id,omitempty"`
	SourcePath string `json:"source_path,omitempty"`
	Hash       string `json:"hash"`
	Directory  bool   `json:"directory,omitempty"`
	Size       int64  `json:"size"`
	Mtime      int64  `json:"mtime"`
}
type Snapshot struct {
	Records []Record `json:"records"`
}
