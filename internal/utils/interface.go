package utils

import "time"

// Utils provides helpers used by the sync service (parameters, file copy, time, tasks).
// Implementation is provided by the caller (e.g. Python common.utils port).
// The db parameter is the local DB; implementation may type-assert to repository.DB.
type Utils interface {
	GetParameter(db interface{}, key string) string
	CopyFilesToDir(srcPattern, dstDir string, overwrite, printLog bool) bool

	BeginOfHour(t time.Time) time.Time
	EndOfHour(t time.Time) time.Time
	BeginOfDay(t time.Time) time.Time
	EndOfDay(t time.Time) time.Time

	UpdateCompletionPercentage(db interface{}, taskID int, percent float64)
	CreateTask(db interface{}, taskType string, checkRunning bool) int
	GetStreamNameByID(db interface{}, streamID int) string
}
