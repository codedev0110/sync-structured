package model

import "time"

// Record represents a single recording row from the records table.
type Record struct {
	ID               int
	StreamID         int
	Path             string
	StartedAt        time.Time
	EndedAt          time.Time
	Duration         float64
	DurationRecorded float64
	ReturnCode       int
	StreamType       int
	URLIndex         int

	IsRecordApproved bool
	Processed        bool
	ConvertedToMP3   bool
	ConvertedToLow   bool
	RecordRate       float64

	ImportedRecordID int
	ImportedSourceID int

	SamplingRate int
	FrameWidth   int
	Shape        string
	FPS          float64
	FrameStep    float64
	VShape       string

	IsPreprocessed  bool
	IsRecordChecked bool
	IsDeleted       bool
}
