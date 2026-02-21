package service

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"myproject/internal/model"
	"myproject/internal/repository"
	"myproject/internal/utils"
)

// SyncService holds dependencies and implements record sync logic.
type SyncService struct {
	LocalDB    repository.DB
	GetRemoteDB func(serverID int) repository.DB
	Ut         utils.Utils
}

// NewSyncService creates a SyncService with the given dependencies.
func NewSyncService(localDB repository.DB, getRemoteDB func(serverID int) repository.DB, ut utils.Utils) *SyncService {
	return &SyncService{LocalDB: localDB, GetRemoteDB: getRemoteDB, Ut: ut}
}

func (s *SyncService) getStreamTypeSQL(streamType string) string {
	switch streamType {
	case "audio":
		return " and stream_type = 1 "
	case "video":
		return " and stream_type = 2 "
	default:
		return ""
	}
}

func parseServersOrder(serverOrderStr string) []int {
	parts := strings.Split(serverOrderStr, ",")
	var order []int
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			fmt.Println("Exception on get_server_order:", err)
			continue
		}
		order = append(order, v)
	}
	return order
}

// GetServersOrder returns streamID -> list of server IDs to try for import.
func (s *SyncService) GetServersOrder(streamType string) map[int][]int {
	serverOrderStr := s.Ut.GetParameter(s.LocalDB, fmt.Sprintf("server_order_%s_records_import", streamType))
	if serverOrderStr == "" {
		return map[int][]int{}
	}
	serversOrderGeneral := parseServersOrder(serverOrderStr)
	serversOrder := make(map[int][]int)
	sql := fmt.Sprintf(
		"select * from streams where enabled=true %s order by id",
		s.getStreamTypeSQL(streamType),
	)
	streams, err := s.LocalDB.SelectStreams(sql)
	if err != nil {
		fmt.Println("DB error in getServersOrder:", err)
		return serversOrder
	}
	for _, stream := range streams {
		streamServersOrders := parseServersOrder(stream.ServerImportOrder)
		if len(streamServersOrders) > 0 {
			serversOrder[stream.ID] = streamServersOrders
		} else {
			serversOrder[stream.ID] = serversOrderGeneral
		}
	}
	return serversOrder
}

func selectPathPrefix(svr, svrLocal int) string {
	if svr == svrLocal {
		return "/home/neurotime/stream_analyse/recording/"
	}
	return fmt.Sprintf("/mnt/fs_svr%d/recording/", svr)
}

func (s *SyncService) addRecordToImported(serverID, recordID int, imported map[int][]int) map[int][]int {
	if imported == nil {
		imported = make(map[int][]int)
	}
	imported[serverID] = append(imported[serverID], recordID)
	return imported
}

func (s *SyncService) insertRecord(d repository.DB, r model.Record, sourceServerID int) error {
	return repository.InsertRecord(d, r, sourceServerID)
}

func (s *SyncService) updateRecordNotApproved(d repository.DB, records []int) error {
	return repository.UpdateRecordNotApproved(d, records)
}

func (s *SyncService) disableResults(d repository.DB, records []int) error {
	return repository.DisableResults(d, records)
}

func joinRecordPeriods(periods []model.Period, start, end time.Time) []model.Period {
	found := false
	for i := range periods {
		p := &periods[i]
		if (p.Start.Before(start) && start.Before(p.End)) ||
			(p.Start.Before(end) && end.Before(p.End)) ||
			(start.Before(p.Start) || start.Equal(p.Start)) && (p.End.Before(end) || p.End.Equal(end)) {
			if start.Before(p.Start) {
				p.Start = start
			}
			if end.After(p.End) {
				p.End = end
			}
			found = true
			break
		}
	}
	if !found {
		periods = append(periods, model.Period{Start: start, End: end})
	}
	return periods
}

func appendAccordingRecordPeriods(periods []model.Period, notRecStart, notRecEnd time.Time) []model.Period {
	dh := notRecEnd.Sub(notRecStart).Hours()
	n := int(dh) + 2
	for i := 0; i < n; i++ {
		dt := notRecStart.Add(time.Duration(i) * time.Hour)
		d1 := time.Date(dt.Year(), dt.Month(), dt.Day(), dt.Hour(), 0, 0, 0, dt.Location())
		d2 := d1.Add(61 * time.Minute)
		if d1.Before(notRecStart) {
			d1 = notRecStart
		}
		if d2.After(notRecEnd) {
			d2 = notRecEnd
		}
		periods = append(periods, model.Period{Start: d1, End: d2})
		if notRecEnd.Sub(d2).Seconds() < 1 {
			break
		}
	}
	return periods
}

func extractNonRecordedPeriods(recorded []model.Period, start, end time.Time) []model.Period {
	var notRecorded []model.Period
	if len(recorded) == 0 {
		notRecorded = appendAccordingRecordPeriods(notRecorded, start, end)
		return notRecorded
	}
	if recorded[0].Start.After(start) {
		notRecorded = appendAccordingRecordPeriods(notRecorded, start, recorded[0].Start)
	}
	if len(recorded) > 1 {
		for i := 0; i < len(recorded)-1; i++ {
			notRecorded = appendAccordingRecordPeriods(notRecorded, recorded[i].End, recorded[i+1].Start)
		}
	}
	if end.After(recorded[len(recorded)-1].End) {
		notRecorded = appendAccordingRecordPeriods(notRecorded, recorded[len(recorded)-1].End, end)
	}
	return notRecorded
}

func (s *SyncService) getRecordingStatusInPeriodByStreamID(d repository.DB, streamID int, syncStart, syncEnd time.Time) ([]model.Period, []model.Period) {
	syncStart1 := syncStart.Add(-61 * time.Minute)
	sql := fmt.Sprintf(`
select * from records
where started_at > '%s' and started_at < '%s'
  and stream_id = %d
  and is_record_approved = true
order by stream_id, started_at
`, syncStart1.Format("2006-01-02 15:04:05"), syncEnd.Format("2006-01-02 15:04:05"), streamID)
	records, err := d.SelectRecords(sql)
	if err != nil {
		fmt.Println("DB error in getRecordingStatusInPeriodByStreamID:", err)
		return nil, nil
	}
	var recorded []model.Period
	for _, r := range records {
		recorded = joinRecordPeriods(recorded, r.StartedAt, r.EndedAt)
	}
	if len(recorded) > 0 {
		for i := 0; i < len(recorded); {
			rec := &recorded[i]
			if !rec.Start.Before(syncStart) {
				break
			} else if !rec.End.After(syncStart) {
				recorded = append(recorded[:i], recorded[i+1:]...)
				continue
			} else {
				rec.Start = syncStart
				break
			}
			i++
		}
	}
	nonRecorded := extractNonRecordedPeriods(recorded, syncStart, syncEnd)
	return recorded, nonRecorded
}

func (s *SyncService) getRecordingStatusInPeriod(d repository.DB, syncStart, syncEnd time.Time, streamType string, streamID int) (map[int][]model.Period, map[int][]model.Period) {
	sqlStreamID := ""
	if streamID >= 0 {
		sqlStreamID = fmt.Sprintf(" and id=%d ", streamID)
	}
	sqlStreamType := s.getStreamTypeSQL(streamType)
	sql := fmt.Sprintf(
		"select * from streams where enabled=true %s %s order by id",
		sqlStreamType, sqlStreamID,
	)
	streams, err := d.SelectStreams(sql)
	if err != nil {
		fmt.Println("DB error in getRecordingStatusInPeriod:", err)
		return nil, nil
	}
	recordedPeriods := make(map[int][]model.Period)
	nonRecordedPeriods := make(map[int][]model.Period)
	for _, st := range streams {
		rec, nonRec := s.getRecordingStatusInPeriodByStreamID(d, st.ID, syncStart, syncEnd)
		recordedPeriods[st.ID] = rec
		nonRecordedPeriods[st.ID] = nonRec
	}
	return recordedPeriods, nonRecordedPeriods
}

func sortPeriodsByStart(periods map[int][]model.Period) []model.Period {
	var result []model.Period
	for streamID, ps := range periods {
		for _, p := range ps {
			result = append(result, model.Period{Start: p.Start, End: p.End, StreamID: streamID})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Start.Before(result[j].Start)
	})
	return result
}

func isRecordInImported(serverID, recordID int, imported map[int][]int) bool {
	if imported == nil {
		return false
	}
	list, ok := imported[serverID]
	if !ok {
		return false
	}
	for _, id := range list {
		if id == recordID {
			return true
		}
	}
	return false
}

func (s *SyncService) isSimilarRecordExistsDB(r model.Record) bool {
	sql := fmt.Sprintf(`
select id from records
where started_at between '%s' and '%s'
  and ended_at   between '%s' and '%s'
  and record_rate between %f and %f
  and stream_id = %d
  and is_record_approved = true
`,
		r.StartedAt.Add(-10*time.Second).Format("2006-01-02 15:04:05"),
		r.StartedAt.Add(10*time.Second).Format("2006-01-02 15:04:05"),
		r.EndedAt.Add(-10*time.Second).Format("2006-01-02 15:04:05"),
		r.EndedAt.Add(10*time.Second).Format("2006-01-02 15:04:05"),
		r.RecordRate-0.01, r.RecordRate+0.01, r.StreamID,
	)
	records, err := s.LocalDB.SelectRecords(sql)
	if err != nil {
		fmt.Println("DB error in isSimilarRecordExistsDB:", err)
		return false
	}
	return len(records) > 0
}

func (s *SyncService) getCoveredRecords(r model.Record) []int {
	sql := fmt.Sprintf(`
select id from records
where started_at > '%s'
  and ended_at < '%s'
  and stream_id = %d
  and is_record_approved = true
`,
		r.StartedAt.Add(-15*time.Second).Format("2006-01-02 15:04:05"),
		r.EndedAt.Add(15*time.Second).Format("2006-01-02 15:04:05"),
		r.StreamID,
	)
	records, err := s.LocalDB.SelectRecords(sql)
	if err != nil {
		fmt.Println("DB error in getCoveredRecords:", err)
		return nil
	}
	var ids []int
	for _, rr := range records {
		ids = append(ids, rr.ID)
	}
	return ids
}

func (s *SyncService) isRecordInDB(importedRecordID, serverID int) bool {
	sql := fmt.Sprintf(`
select id from records
where imported_source_id = %d
  and imported_record_id = %d
  and is_record_approved = true
`, serverID, importedRecordID)
	records, err := s.LocalDB.SelectRecords(sql)
	if err != nil {
		fmt.Println("DB error in isRecordInDB:", err)
		return false
	}
	return len(records) > 0
}

// CopyRecords copies a record from serverID to local; returns status and updated imported map.
func (s *SyncService) CopyRecords(serverLocalID, disabledRecordID, serverID int, record model.Record, imported map[int][]int, isSyncMode bool) (string, map[int][]int) {
	status := ""
	disabledRecords := []int{}
	if disabledRecordID > 0 {
		disabledRecords = append(disabledRecords, disabledRecordID)
	}
	fmt.Println(imported)
	isImported := isRecordInImported(serverID, record.ID, imported)
	var similarExists bool
	if !isImported {
		similarExists = s.isSimilarRecordExistsDB(record)
	}
	if isImported || similarExists {
		var reason string
		if isImported {
			reason = "record already imported"
		} else {
			reason = "similar record exists"
		}
		fmt.Println("  >> NOT NEED IMPORT :", reason)
		status = "no_need"
	} else {
		src := selectPathPrefix(serverID, serverLocalID) + strings.ReplaceAll(record.Path, "./", "")
		dst := selectPathPrefix(serverLocalID, serverLocalID) + strings.ReplaceAll(record.Path, "./", "")
		fmt.Println("  >> copy", src)
		if isSyncMode {
			if s.isRecordInDB(record.ID, serverID) {
				status = "no_need"
				fmt.Println("  > record already imported")
			} else {
				base := strings.TrimSuffix(src, filepath.Ext(src))
				srcPattern := base + "*"
				dstDir := filepath.Dir(dst)
				copyResult := s.Ut.CopyFilesToDir(srcPattern, dstDir, false, true)
				if copyResult {
					status = "updated"
					fmt.Println("  > success copy to", dstDir)
					startDBTime := time.Now()
					disabledRecords = append(disabledRecords, s.getCoveredRecords(record)...)
					if err := s.insertRecord(s.LocalDB, record, serverID); err != nil {
						fmt.Println("  > insertRecord error:", err)
					}
					fmt.Printf("    disabled_records_list = %v\n", disabledRecords)
					if err := s.updateRecordNotApproved(s.LocalDB, disabledRecords); err != nil {
						fmt.Println("  > update_record_not_approved error:", err)
					}
					if err := s.disableResults(s.LocalDB, disabledRecords); err != nil {
						fmt.Println("  > disable_results error:", err)
					}
					fmt.Printf("  > db update duration : %v\n", time.Since(startDBTime))
					imported = s.addRecordToImported(serverID, record.ID, imported)
				} else {
					status = "no_success"
					fmt.Println("  > error copy from", srcPattern)
				}
			}
		} else {
			status = "updated"
			imported = s.addRecordToImported(serverID, record.ID, imported)
		}
	}
	return status, imported
}

func (s *SyncService) getRecordsAccordingServersOrder(order []int, sql string) (int, []model.Record) {
	for _, srv := range order {
		d := s.GetRemoteDB(srv)
		if d == nil {
			continue
		}
		recs, err := d.SelectRecords(sql)
		if err != nil {
			fmt.Println("DB error in getRecordsAccordingServersOrder:", err)
			continue
		}
		if len(recs) > 0 {
			return srv, recs
		}
	}
	return -1, nil
}

// AddRecordsFromOtherServers tries to add any records from other servers for the record's hour (add mode).
func (s *SyncService) AddRecordsFromOtherServers(streamType string, serverLocalID int, record model.Record, imported map[int][]int, serversOrder map[int][]int, isSyncMode bool) (string, map[int][]int) {
	startedAt := s.Ut.BeginOfHour(record.StartedAt)
	endedAt := s.Ut.EndOfHour(startedAt).Add(3 * time.Minute)
	streamID := record.StreamID
	sqlCondition := ""
	if streamType == "video" {
		sqlCondition = " and converted_to_low = true "
	}
	sql := fmt.Sprintf(`
select * from records
where ('%s' < started_at and ended_at < '%s')
  and duration > 0
  and stream_id = %d
  and is_record_approved = true
  and converted_to_mp3 = true
  %s
order by started_at
`, startedAt.Format("2006-01-02 15:04:05"), endedAt.Format("2006-01-02 15:04:05"), streamID, sqlCondition)
	srv, recs := s.getRecordsAccordingServersOrder(serversOrder[streamID], sql)
	status := ""
	if len(recs) > 0 {
		fmt.Printf("  > Start copy any records from server %d between '%s' and '%s' (add mode)\n",
			srv, startedAt.Format("2006-01-02 15:04:05"), endedAt.Format("2006-01-02 15:04:05"))
		for _, r := range recs {
			status, imported = s.CopyRecords(serverLocalID, -1, srv, r, imported, isSyncMode)
		}
	} else {
		fmt.Printf("  > Can't find any records from another servers between '%s' and '%s'\n",
			startedAt.Format("2006-01-02 15:04:05"), endedAt.Format("2006-01-02 15:04:05"))
		status = "no_find"
	}
	return status, imported
}

func getPeriodRate(r model.Record, periodStart, periodEnd time.Time) float64 {
	dt1 := maxTime(r.StartedAt, periodStart)
	dt2 := minTime(r.EndedAt, periodEnd)
	rate := dt2.Sub(dt1).Seconds() / periodEnd.Sub(periodStart).Seconds()
	if rate < 0 {
		return 0
	}
	return rate
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func mathRound(x float64, prec int) float64 {
	pow := mathPow10(prec)
	return float64(int64(x*pow+0.5)) / pow
}

func mathPow10(n int) float64 {
	p := 1.0
	for i := 0; i < n; i++ {
		p *= 10
	}
	return p
}

func (s *SyncService) getRecordsFromServer(serverID int, sql string, startedAt, endedAt time.Time) *model.Record {
	fmt.Println("start select get_records_from_server")
	startProcess := time.Now()
	d := s.GetRemoteDB(serverID)
	if d == nil {
		fmt.Println("nil DB for server", serverID)
		return nil
	}
	recs, err := d.SelectRecords(sql)
	if err != nil {
		fmt.Println("DB error in getRecordsFromServer:", err)
		return nil
	}
	fmt.Printf("end DB select with len = %d,    process duration : %v\n", len(recs), time.Since(startProcess))
	if len(recs) == 0 {
		return nil
	}
	maxIdx := 0
	if len(recs) > 1 {
		maxRate := 0.0
		for i, r := range recs {
			rate := getPeriodRate(r, startedAt, endedAt)
			if rate > maxRate {
				maxRate = rate
				maxIdx = i
			}
		}
	}
	r := recs[maxIdx]
	fmt.Printf("  > db-%d, found records = %d   max rate id = %d\n", serverID, len(recs), maxIdx)
	fmt.Println(" ", r.ID, " ", r.Path, " ", r.Duration, "min  ", r.RecordRate, " ",
		r.StartedAt.Format("2006-01-02 15:04:05"), "  ", r.EndedAt.Format("2006-01-02 15:04:05"))
	return &r
}

// SyncRecordsFromOtherServers finds the best record from other servers and copies it.
func (s *SyncService) SyncRecordsFromOtherServers(streamType string, serverLocalID int, record model.Record, imported map[int][]int, serversOrder map[int][]int, isNonRecordedPeriod, isSyncMode bool) (string, map[int][]int) {
	const deltaSec = 10
	recordID := record.ID
	recordRate := record.RecordRate + 0.001
	if recordRate > 0.999 {
		recordRate = 0.999
	}
	startedAt := record.StartedAt
	startedHour := s.Ut.BeginOfHour(startedAt)
	endedAt := record.EndedAt
	duration := record.Duration
	if duration > 60 {
		duration = 60
	}
	streamID := record.StreamID
	start1 := startedAt.Add(-deltaSec * time.Second)
	start2 := startedAt.Add(deltaSec * time.Second)
	sqlCondition := ""
	if streamType == "video" {
		sqlCondition = " and converted_to_low = true "
	}
	var sql string
	if isNonRecordedPeriod {
		sql = fmt.Sprintf(`
select * from records
where started_at < '%s'
  and ended_at > '%s'
  and started_at >= '%s'
  and stream_id = %d
  and is_record_approved = true
  and converted_to_mp3 = true
  and is_deleted = false
  and return_code=0
  %s
order by duration desc, started_at
`, endedAt.Format("2006-01-02 15:04:05"), startedAt.Format("2006-01-02 15:04:05"),
			startedHour.Format("2006-01-02 15:04:05"), streamID, sqlCondition)
	} else {
		sql = fmt.Sprintf(`
select * from records
where
  (
    (
      (
        ((started_at - (interval '1 sec' * %d)) < '%s')
        or (started_at between '%s' and '%s')
      )
      and '%s' < least(ended_at, started_at + (interval '1 min' * duration)) + (interval '1 sec' * %d)
    )
  )
  and duration > %f
  and record_rate > %f
  and started_at >= '%s'
  and stream_id = %d
  and is_record_approved = true
  and converted_to_mp3 = true
  and is_deleted = false
  and return_code=0
  %s
order by duration desc, started_at
`, deltaSec, startedAt.Format("2006-01-02 15:04:05"), start1.Format("2006-01-02 15:04:05"), start2.Format("2006-01-02 15:04:05"),
			endedAt.Format("2006-01-02 15:04:05"), deltaSec, duration, recordRate,
			startedHour.Format("2006-01-02 15:04:05"), streamID, sqlCondition)
	}
	fmt.Println(sql)
	results := make(map[int]model.Record)
	for _, serverID := range serversOrder[streamID] {
		res := s.getRecordsFromServer(serverID, sql, startedAt, endedAt)
		if res != nil {
			results[serverID] = *res
		}
	}
	if len(results) == 0 {
		fmt.Println("  > Can't find any records from another servers with more duration time")
		return "no_find", imported
	}
	maxRate := 0.0
	maxServerID := -1
	for sid, r := range results {
		compareRate := r.RecordRate * getPeriodRate(r, startedAt, endedAt)
		compareRate = mathRound(compareRate, 2)
		fmt.Printf("       %d  compare_rate=%f  %s  %f  %s  %s\n",
			r.ID, compareRate, r.Path, r.RecordRate,
			r.StartedAt.Format("2006-01-02 15:04:05"), r.EndedAt.Format("2006-01-02 15:04:05"))
		if compareRate > maxRate {
			maxRate = compareRate
			maxServerID = sid
		}
	}
	if maxRate <= 0 {
		fmt.Println("  > Can't find records from any server with record_rate>0")
		return "no_find", imported
	}
	fmt.Println("  >> get max result from db", maxServerID)
	best := results[maxServerID]
	status, imported2 := s.CopyRecords(serverLocalID, recordID, maxServerID, best, imported, isSyncMode)
	return status, imported2
}

// StartRecordProcessing runs the full sync: problem records first, then non-recorded periods.
func (s *SyncService) StartRecordProcessing(args Args, periodType string) {
	startProcessing := time.Now()
	fmt.Println("\nSTARTED at", startProcessing)
	isSyncMode := args.Sync
	streamID := args.StreamID
	streamType := args.StreamType
	isAddMode := args.AddMode
	isNoTask := args.NoTask

	serverLocalIDStr := s.Ut.GetParameter(s.LocalDB, "server_number")
	serverLocalID, _ := strconv.Atoi(serverLocalIDStr)

	serversOrder := s.GetServersOrder(streamType)
	fmt.Println("servers order :", serversOrder)
	fmt.Println()
	if len(serversOrder) == 0 {
		fmt.Println("Not defined servers order")
		return
	}

	if streamType == "video" {
		if s.Ut.GetParameter(s.LocalDB, "is_video_processing") != "1" &&
			s.Ut.GetParameter(s.LocalDB, "is_band_processing") != "1" {
			fmt.Printf("Current server is not process %s files and can't import %s records\n", streamType, streamType)
			return
		}
	} else {
		if s.Ut.GetParameter(s.LocalDB, fmt.Sprintf("is_%s_processing", streamType)) != "1" {
			fmt.Printf("Current server is not process %s files and can't import %s records\n", streamType, streamType)
			return
		}
	}

	var taskID int
	if isSyncMode && !isNoTask {
		taskID = s.Ut.CreateTask(s.LocalDB, "records_sync", true)
		if taskID < 0 {
			fmt.Println("\n Another records sync process is running \n")
			return
		}
	} else {
		taskID = -1
	}

	var syncTimeStart, syncTimeEnd time.Time
	now := time.Now()
	switch periodType {
	case "period":
		syncTimeStart = args.StartDatetime
		syncTimeEnd = args.EndDatetime
	case "auto":
		if args.AutoDays != nil {
			dt1 := now.Add(-time.Duration(*args.AutoDays) * 24 * time.Hour)
			dt2 := now.Add(-24 * time.Hour)
			syncTimeStart = s.Ut.BeginOfDay(dt1)
			syncTimeEnd = s.Ut.EndOfDay(dt2)
		} else if args.AutoHours != nil {
			dt1 := now.Add(-time.Duration(*args.AutoHours) * time.Hour)
			dt2 := now.Add(-1 * time.Hour)
			syncTimeStart = s.Ut.BeginOfHour(dt1)
			syncTimeEnd = s.Ut.EndOfHour(dt2)
		}
	default:
		syncTimeStart = now
		syncTimeEnd = now
	}

	fmt.Println("local_server      =", serverLocalID)
	fmt.Println("task_id           =", taskID)
	fmt.Println("start sync time   =", syncTimeStart)
	fmt.Println("end sync time     =", syncTimeEnd)
	fmt.Println("stream_id         =", streamID)
	fmt.Println("stream_type       =", streamType)
	fmt.Println("sync_mode         =", isSyncMode)
	time.Sleep(5 * time.Second)

	sqlStreamID := ""
	if streamID >= 0 {
		sqlStreamID = fmt.Sprintf(" and stream_id=%d ", streamID)
	}
	sqlStreamType := s.getStreamTypeSQL(streamType)
	sqlQuery := fmt.Sprintf(`
select * from records
where started_at > '%s' and started_at < '%s'
  %s
  %s
  and is_record_approved=true and (duration<61 or record_rate<1)
order by started_at, stream_id
`, syncTimeStart.Format("2006-01-02 15:04:05"), syncTimeEnd.Format("2006-01-02 15:04:05"), sqlStreamID, sqlStreamType)
	fmt.Println(sqlQuery)

	records1, err := s.LocalDB.SelectRecords(sqlQuery)
	if err != nil {
		fmt.Println("DB error selecting problem records:", err)
		return
	}

	importedQuery := fmt.Sprintf(`
select * from records
where started_at > '%s' and started_at < '%s'
  %s
  %s
  and (is_record_approved=true or is_record_checked=true)
  and imported_record_id>0
order by started_at, stream_id
`, syncTimeStart.Format("2006-01-02 15:04:05"), syncTimeEnd.Format("2006-01-02 15:04:05"), sqlStreamID, sqlStreamType)
	importedRecords, err := s.LocalDB.SelectRecords(importedQuery)
	if err != nil {
		fmt.Println("DB error selecting imported records:", err)
		return
	}

	importedIDs := make(map[int][]int)
	for _, r := range importedRecords {
		importedIDs = s.addRecordToImported(r.ImportedSourceID, r.ImportedRecordID, importedIDs)
	}
	fmt.Println("Already imported:", importedIDs)

	fmt.Println("\n\n------------------------------------------------------------------------------------------------------------------------------------")
	fmt.Println("Start processing recorded periods at", time.Now())
	fmt.Println("------------------------------------------------------------------------------------------------------------------------------------")
	fmt.Printf("\nStart processing %d records\n", len(records1))

	n := 0
	nUpdated := 0
	nNoNeed := 0
	nNoFind := 0
	nNoSuccess := 0
	nn := len(records1)
	s.Ut.UpdateCompletionPercentage(s.LocalDB, taskID, 1)

	for _, r := range records1 {
		n++
		fmt.Println("\n\n", "process records :", n, "of", nn)
		fmt.Println(" ", r.ID, " ", r.Path, " ", r.Duration, "min  ", r.RecordRate, " ",
			r.StartedAt.Format("2006-01-02 15:04:05"), "  ", r.EndedAt.Format("2006-01-02 15:04:05"))
		status := ""
		if r.IsRecordApproved {
			status, importedIDs = s.SyncRecordsFromOtherServers(streamType, serverLocalID, r, importedIDs, serversOrder, false, isSyncMode)
		} else {
			fmt.Println("  >> NO need process : record already disabled by previous import")
			status = "no_need"
		}
		switch status {
		case "updated":
			nUpdated++
		case "no_need":
			nNoNeed++
		case "no_find":
			nNoFind++
		case "no_success":
			nNoSuccess++
		}
		s.Ut.UpdateCompletionPercentage(s.LocalDB, taskID, 50*float64(n)/float64(nn))
	}

	_, nonRecorded := s.getRecordingStatusInPeriod(s.LocalDB, syncTimeStart, syncTimeEnd, streamType, streamID)
	fmt.Println("\n\n------------------------------------------------------------------------------------------------------------------------------------")
	fmt.Println("Start processing for non-recorded periods at", time.Now())
	fmt.Println("------------------------------------------------------------------------------------------------------------------------------------")
	fmt.Println(importedIDs)
	sortedNonRecorded := sortPeriodsByStart(nonRecorded)
	nn = nn + len(sortedNonRecorded)

	for _, p := range sortedNonRecorded {
		n++
		fmt.Println("\n\n", "process non_recorded_periods :", n, " of", nn)
		startProcessTime := time.Now()
		fmt.Println(" stream_id =", p.StreamID, " ", s.Ut.GetStreamNameByID(s.LocalDB, p.StreamID), " ",
			p.Start.Format("2006-01-02 15:04:05"), "  ", p.End.Format("2006-01-02 15:04:05"), "  duration =", p.End.Sub(p.Start))
		status := ""
		if p.End.Sub(p.Start).Seconds() < 20 {
			status = "no_need"
			fmt.Println("  >> NOT NEED IMPORT")
		} else {
			r := model.Record{
				ID:             -1,
				StartedAt:      p.Start,
				EndedAt:        p.End,
				Duration:       p.End.Sub(p.Start).Minutes(),
				StreamID:       p.StreamID,
				RecordRate:     0,
				Path:           "",
				StreamType:     0,
				URLIndex:       0,
				ConvertedToMP3: true,
			}
			status, importedIDs = s.SyncRecordsFromOtherServers(streamType, serverLocalID, r, importedIDs, serversOrder, true, isSyncMode)
			if status == "no_find" && isAddMode {
				status, importedIDs = s.AddRecordsFromOtherServers(streamType, serverLocalID, r, importedIDs, serversOrder, isSyncMode)
			}
		}
		switch status {
		case "updated":
			nUpdated++
		case "no_need":
			nNoNeed++
		case "no_find":
			nNoFind++
		case "no_success":
			nNoSuccess++
		}
		s.Ut.UpdateCompletionPercentage(s.LocalDB, taskID, 100*float64(n)/float64(nn))
		fmt.Printf(" process duration : %v\n", time.Since(startProcessTime))
	}

	s.Ut.UpdateCompletionPercentage(s.LocalDB, taskID, 100)
	fmt.Println("\n\nDone!")
	fmt.Println("=============================================================")
	fmt.Println("local_server      =", serverLocalID)
	fmt.Println("task_id           =", taskID)
	fmt.Println("start sync time   =", syncTimeStart)
	fmt.Println("end sync time     =", syncTimeEnd)
	fmt.Println("stream_id         =", streamID)
	fmt.Println("stream_type       =", streamType)
	fmt.Println("sync_mode         =", isSyncMode)
	fmt.Println()
	fmt.Println("Total records with problems  =", n)
	fmt.Println("Updated records              =", nUpdated)
	fmt.Println("No need update records       =", nNoNeed)
	fmt.Println("Can't find records           =", nNoFind)
	fmt.Println("No success sync              =", nNoSuccess)
	fmt.Println("=============================================================")
	fmt.Println("STARTED   at", startProcessing)
	fmt.Println("FINISHED  at", time.Now())
	fmt.Println("DURATION  =", time.Since(startProcessing))
}

// Args holds CLI arguments for the sync command.
type Args struct {
	StartDatetime time.Time
	EndDatetime   time.Time
	AutoDays      *int
	AutoHours     *int
	StreamType    string
	StreamID      int
	Sync          bool
	AddMode       bool
	NoTask        bool
}
