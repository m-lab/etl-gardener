package job

import (
	"errors"
	"time"

	"github.com/m-lab/etl-gardener/tracker"
)

var (
	// ErrNoDateAvailable is returned if a DateIterator does not have a date available.
	ErrNoDateAvailable = errors.New("no date is available")

	// ErrBadDate is returned if a bad date is returned by a date iterator.
	ErrBadDate = errors.New("bad date")
)

type namedSaver interface {
	Save(v any) error
	Load(v any) error
}

// DateIterator defines the interface common to the Daily and Historical
// iterators.
//
// DateIterator implementations should prioritize process continuity and
// reliability after restart. For example, the sequence of operations should
// roughly follow: filter, save, read, update, return. By saving the current
// date before updating, we ensure that a mid-process restart would pickup where
// it left off, possibly at the expense of some reprocessing, but with assurance
// that no dates were skipped.
type DateIterator interface {
	Next() (time.Time, error)
}

// old: load, update, filter, save - may skip data.
// new: load, filter, save, update - may reprocess data.

// DailyIterator tracks state and dates for "daily" processing events.
type DailyIterator struct {
	Date  time.Time     // The next date to process.
	delay time.Duration // wait this long after UTC midnight before processing next date.
	saver namedSaver
}

// HistoricalIterator tracks state and dates for "historical" processing events.
type HistoricalIterator struct {
	Date  time.Time // The next date to process.
	start time.Time // Earliest date to process after reaching current date.
	saver namedSaver
}

// NewDailyIterator creates a new DailyIterator.  The delay is an additional
// offset after UTC midnight before the next Date is processed.  The starting
// Date will be yesterday or the date successfully loaded from the given saver.
func NewDailyIterator(delay time.Duration, saver namedSaver) *DailyIterator {
	d := &DailyIterator{
		Date:  tracker.YesterdayDate(),
		saver: saver,
		delay: delay,
	}
	// Attempt to load Date from previously saved data.
	tmp := &DailyIterator{}
	err := d.saver.Load(tmp)
	if err == nil && !tmp.Date.IsZero() {
		// Ignore load errors.
		d.Date = tmp.Date
	}
	return d
}

// Next returns the next "daily" date. Until 'delay' after UTC midnight, Next will
// return ErrNoDateAvailable. If err is nil, then the returned time is the next
// daily date. After every update, the new date is saved.
func (d *DailyIterator) Next() (time.Time, error) {
	// Do not proceed until after midnight+delay the next day.
	if time.Since(d.Date) < 24*time.Hour+d.delay {
		return time.Time{}, ErrNoDateAvailable // Zero time.
	}

	// Saving before advancing to next date ensures we process this date fully.
	err := d.saver.Save(d)
	if err != nil {
		return time.Time{}, err
	}

	r := d.Date
	// Advance to next "yesterday" date.
	d.Date = d.Date.UTC().AddDate(0, 0, 1).Truncate(24 * time.Hour)
	return r, nil
}

// NewHistoricalIterator creates a new HistoricalIterator. The iterator will
// begin from the given start unless a different date is successfully loaded
// from the given saver. The returned HistoricalIterator.Date is guaranteed to
// be later or equal to the start date.
func NewHistoricalIterator(start time.Time, saver namedSaver) *HistoricalIterator {
	h := &HistoricalIterator{
		Date:  start,
		start: start,
		saver: saver,
	}
	// Attempt to load Date from previously saved data.
	tmp := &DailyIterator{}
	err := h.saver.Load(tmp)
	if err == nil && !tmp.Date.IsZero() {
		// Ignore load errors.
		h.Date = tmp.Date
	}
	// Enforce consistent dates.
	if h.Date.Before(h.start) {
		h.Date = h.start
	}
	return h
}

// Next returns the next "historical" date. If this date is within 36h of
// current, real world date, then the historical cycle restarts. After every
// update, the new date is saved.
func (h *HistoricalIterator) Next() (time.Time, error) {
	// Start over when we reach yesterday.
	if time.Since(h.Date) < 36*time.Hour {
		h.Date = h.start
	}
	// Saving before advancing to next date ensures we process this date fully.
	err := h.saver.Save(h)
	if err != nil {
		return time.Time{}, err
	}
	r := h.Date
	// Advance to next date.
	h.Date = h.Date.UTC().AddDate(0, 0, 1).Truncate(24 * time.Hour)
	return r, nil
}

// JobIterator iterates over Job configurations for an underlying DateIterator sequence.
type JobIterator struct {
	date      DateIterator
	specs     []tracker.JobWithTarget // The job prefixes to be iterated through.
	nextIndex int
	lastDate  time.Time
}

// NewJobIterator creates a new JobIterator. For every date returned by the
// DateIterator, the JobIterator will enumerate every jobSpec for that date
// before advancing to the next date.
func NewJobIterator(date DateIterator, jobSpecs []tracker.JobWithTarget) *JobIterator {
	return &JobIterator{
		date:      date,
		specs:     jobSpecs,
		nextIndex: len(jobSpecs), // Guarantee first call to Next updates lastDate.
	}
}

// Len returns the number of Job specs.
func (j *JobIterator) Len() int {
	return len(j.specs)
}

// Next returns the next JobWithTarget for the current date. Next may return an
// error when the underlying DateIterator returns an error, e.g. ErrNoDateAvailable.
func (j *JobIterator) Next() (*tracker.JobWithTarget, error) {
	var t time.Time
	var err error
	if j.nextIndex >= len(j.specs) {
		t, err = j.date.Next()
		if err != nil {
			return nil, err
		}
		j.lastDate = t
		j.nextIndex = 0
	}

	// Copy the jobspec and set the date.
	jt := &tracker.JobWithTarget{}
	*jt = j.specs[j.nextIndex]
	jt.Job.Date = j.lastDate
	jt.ID = jt.Job.Key()
	j.nextIndex++ // increment index for next call.
	return jt, nil
}
