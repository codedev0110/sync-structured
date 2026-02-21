package model

// Stream represents a row from the streams table.
type Stream struct {
	ID                int
	Enabled           bool
	StreamType        int
	ServerImportOrder string
}
