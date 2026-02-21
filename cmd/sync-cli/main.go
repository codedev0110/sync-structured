package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"myproject/internal/repository"
	"myproject/internal/service"
	"myproject/internal/utils"
)

func validDateTimeType(s string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04", s)
}

func checkStreamType(value string) (string, error) {
	if value != "audio" && value != "video" {
		return "", fmt.Errorf("stream type - `audio` or `video`")
	}
	return value, nil
}

func printHelp() {
	fmt.Println(`Usage:
  program period -start "YYYY-MM-DD HH:mm" -end "YYYY-MM-DD HH:mm" -stream_type audio|video [--sync] [--add_mode] [--no_task] [-stream_id N]
  program auto   -days N | -hours N -stream_type audio|video [--sync] [--add_mode] [--no_task] [-stream_id N]`)
}

func parseArgs() (service.Args, string) {
	if len(os.Args) < 3 {
		fmt.Println("Error: No argument specified.\n")
		printHelp()
		os.Exit(1)
	}

	sub := os.Args[1]
	var a service.Args
	var periodType string

	switch sub {
	case "period":
		fs := flag.NewFlagSet("period", flag.ExitOnError)
		startStr := fs.String("start", "", `start datetime "YYYY-MM-DD HH:mm"`)
		endStr := fs.String("end", "", `end datetime "YYYY-MM-DD HH:mm"`)
		streamType := fs.String("stream_type", "", "stream type - `audio` or `video`")
		streamID := fs.Int("stream_id", -1, "sync only stream with id")
		syncMode := fs.Bool("sync", false, "sync mode : update target database and copy files")
		addMode := fs.Bool("add_mode", false, "add mode : add all records from another servers")
		noTask := fs.Bool("no_task", false, "no task mode")
		_ = fs.Parse(os.Args[2:])

		if *startStr == "" || *endStr == "" {
			fmt.Println("start and end are required")
			printHelp()
			os.Exit(1)
		}
		st, err := validDateTimeType(*startStr)
		if err != nil {
			fmt.Println("Given Datetime not valid! Expected format, 'YYYY-MM-DD HH:mm'!")
			os.Exit(1)
		}
		et, err := validDateTimeType(*endStr)
		if err != nil {
			fmt.Println("Given Datetime not valid! Expected format, 'YYYY-MM-DD HH:mm'!")
			os.Exit(1)
		}
		if *streamType == "" {
			fmt.Println("stream_type is required")
			os.Exit(1)
		}
		stype, err := checkStreamType(*streamType)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		a = service.Args{
			StartDatetime: st,
			EndDatetime:   et,
			StreamType:    stype,
			StreamID:      *streamID,
			Sync:          *syncMode,
			AddMode:       *addMode,
			NoTask:        *noTask,
		}
		periodType = "period"

	case "auto":
		fs := flag.NewFlagSet("auto", flag.ExitOnError)
		autoDays := fs.Int("days", 0, "set days before for auto period")
		autoHours := fs.Int("hours", 0, "set hours before for auto period")
		streamType := fs.String("stream_type", "", "stream type - `audio` or `video`")
		streamID := fs.Int("stream_id", -1, "sync only stream with id")
		syncMode := fs.Bool("sync", false, "sync mode : update target database and copy files")
		addMode := fs.Bool("add_mode", false, "add mode : add all records from another servers")
		noTask := fs.Bool("no_task", false, "no task mode")
		_ = fs.Parse(os.Args[2:])

		if (*autoDays == 0 && *autoHours == 0) || (*autoDays != 0 && *autoHours != 0) {
			fmt.Println("You must set either days or hours (only one).")
			os.Exit(1)
		}
		if *streamType == "" {
			fmt.Println("stream_type is required")
			os.Exit(1)
		}
		stype, err := checkStreamType(*streamType)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		var daysPtr, hoursPtr *int
		if *autoDays != 0 {
			daysPtr = autoDays
		}
		if *autoHours != 0 {
			hoursPtr = autoHours
		}

		a = service.Args{
			AutoDays:   daysPtr,
			AutoHours:  hoursPtr,
			StreamType: stype,
			StreamID:   *streamID,
			Sync:       *syncMode,
			AddMode:    *addMode,
			NoTask:     *noTask,
		}
		periodType = "auto"

	default:
		fmt.Println("Unknown subcommand:", sub)
		printHelp()
		os.Exit(1)
	}

	return a, periodType
}

// stubUtils is a no-op implementation of utils.Utils for when DB/Utils are not yet wired.
type stubUtils struct{}

func (stubUtils) GetParameter(db interface{}, key string) string                    { return "" }
func (stubUtils) CopyFilesToDir(srcPattern, dstDir string, overwrite, printLog bool) bool { return false }
func (stubUtils) BeginOfHour(t time.Time) time.Time                               { return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()) }
func (stubUtils) EndOfHour(t time.Time) time.Time {
	return stubUtils{}.BeginOfHour(t).Add(time.Hour).Add(-time.Nanosecond)
}
func (stubUtils) BeginOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}
func (stubUtils) EndOfDay(t time.Time) time.Time {
	return stubUtils{}.BeginOfDay(t).Add(24*time.Hour).Add(-time.Nanosecond)
}
func (stubUtils) UpdateCompletionPercentage(db interface{}, taskID int, percent float64) {}
func (stubUtils) CreateTask(db interface{}, taskType string, checkRunning bool) int     { return -1 }
func (stubUtils) GetStreamNameByID(db interface{}, streamID int) string                 { return "" }

func main() {
	var localDB repository.DB = nil
	getRemoteDB := func(serverID int) repository.DB {
		return nil
	}
	var ut utils.Utils = stubUtils{}

	if localDB == nil {
		fmt.Fprintln(os.Stderr, "warning: no DB set; provide repository.DB and utils.Utils in main to run sync")
	}

	svc := service.NewSyncService(localDB, getRemoteDB, ut)
	args, periodType := parseArgs()
	fmt.Println(os.Args)
	fmt.Printf("%+v\n", args)
	svc.StartRecordProcessing(args, periodType)
}
