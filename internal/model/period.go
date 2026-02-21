package model

import "time"

// Period represents a time range for a stream (recorded or non-recorded).
type Period struct {
	Start    time.Time
	End      time.Time
	StreamID int
}
