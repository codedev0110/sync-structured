package repository

import "myproject/internal/model"

// InsertRecord inserts a record into the given DB with is_record_approved=true, processed=false.
func InsertRecord(d DB, r model.Record, sourceServerID int) error {
	sql := `
insert into records (
	stream_id, path, started_at, ended_at, duration, duration_recorded, return_code,
	stream_type, url_index, is_record_approved, processed, converted_to_mp3,
	converted_to_low, record_rate, imported_record_id, imported_source_id,
	sampling_rate, frame_width, shape, fps, frame_step, v_shape, is_preprocessed
) values (
	$1,$2,$3,$4,$5,$6,$7,
	$8,$9,$10,$11,$12,
	$13,$14,$15,$16,
	$17,$18,$19,$20,$21,$22,$23
)`
	_, err := d.Insert(sql,
		r.StreamID,
		r.Path,
		r.StartedAt,
		r.EndedAt,
		r.Duration,
		r.DurationRecorded,
		r.ReturnCode,
		r.StreamType,
		r.URLIndex,
		true,
		false,
		r.ConvertedToMP3,
		r.ConvertedToLow,
		r.RecordRate,
		r.ID,
		sourceServerID,
		r.SamplingRate,
		r.FrameWidth,
		r.Shape,
		r.FPS,
		r.FrameStep,
		r.VShape,
		r.IsPreprocessed,
	)
	return err
}

// UpdateRecordNotApproved sets is_record_approved = false for the given record IDs.
func UpdateRecordNotApproved(d DB, records []int) error {
	if len(records) == 0 {
		return nil
	}
	sql := "update records set is_record_approved = $1 where id = any($2)"
	return d.Update(sql, false, records)
}

// DisableResults sets is_approved = false and active_status = 7 for the given record_ids.
func DisableResults(d DB, records []int) error {
	if len(records) == 0 {
		return nil
	}
	sql := "update results set is_approved = $1, active_status=$2 where record_id = any($3)"
	return d.Update(sql, false, 7, records)
}
