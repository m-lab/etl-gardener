package job

import (
	"errors"
	"path"
	"reflect"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/m-lab/etl-gardener/persistence"
	"github.com/m-lab/etl-gardener/tracker"
)

type noopSaver struct{}

func (f *noopSaver) Save(v any) error {
	return nil
}
func (f *noopSaver) Load(v any) error {
	return nil
}

type failSaver struct {
	err error
}

func (f *failSaver) Save(v any) error {
	return f.err
}
func (f *failSaver) Load(v any) error {
	return f.err
}

func TestDailyIterator(t *testing.T) {
	type timeOrErr struct {
		date time.Time
		err  error
	}
	failErr := errors.New("any error")
	dir := t.TempDir()
	tests := []struct {
		name  string
		now   time.Time // fake "now".
		saver namedSaver
		want  []timeOrErr
	}{
		{
			// The first time we create the DailyIterator, the success.json will be empty, but it will be saved.
			name:  "success-create",
			now:   time.Date(2022, time.July, 2, 0, 0, 0, 0, time.UTC),
			saver: persistence.NewLocalNamedSaver(path.Join(dir, "success.json")),
			want: []timeOrErr{
				{time.Date(2022, time.July, 1, 0, 0, 0, 0, time.UTC), nil},
				{time.Time{}, ErrNoDateAvailable},
			},
		},
		{
			// The second time we create the DailyIterator, the success.json will successfully load a value.
			name:  "success-reload",
			now:   time.Date(2022, time.July, 4, 0, 0, 0, 0, time.UTC),
			saver: persistence.NewLocalNamedSaver(path.Join(dir, "success.json")),
			want: []timeOrErr{
				{time.Date(2022, time.July, 1, 0, 0, 0, 0, time.UTC), nil},
				{time.Date(2022, time.July, 2, 0, 0, 0, 0, time.UTC), nil},
				{time.Date(2022, time.July, 3, 0, 0, 0, 0, time.UTC), nil},
				{time.Time{}, ErrNoDateAvailable},
			},
		},
		{
			name:  "error",
			now:   time.Now(),
			saver: &failSaver{err: failErr},
			want: []timeOrErr{
				{time.Time{}, failErr}, // doesn't matter, it will fail.
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monkey.Patch(time.Now, func() time.Time {
				return tt.now
			})
			defer monkey.Unpatch(time.Now)
			d := NewDailyIterator(0, tt.saver)

			for i := 0; i < len(tt.want); i++ {
				n, err := d.Next()
				if err != tt.want[i].err {
					t.Errorf("DailyIterator.Next returned error; got %v, want %v", err, tt.want[i].err)
				}
				if n != tt.want[i].date {
					t.Errorf("DailyIterator.Next returned wrong date; got %q, want %q", n, tt.want[i].date)
				}
			}
		})
	}
}

func TestHistoricalIterator(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name    string
		now     time.Time // fake "now".
		start   time.Time
		saver   namedSaver
		want    []time.Time
		wantErr bool
	}{
		{
			name:  "success",
			now:   time.Now(),
			start: time.Date(2019, time.June, 30, 0, 0, 0, 0, time.UTC),
			saver: persistence.NewLocalNamedSaver(path.Join(dir, "success.json")),
			want: []time.Time{
				time.Date(2019, time.June, 30, 0, 0, 0, 0, time.UTC),
				time.Date(2019, time.July, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2019, time.July, 2, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name:  "success-reload-continue",
			now:   time.Now(),
			start: time.Date(2019, time.July, 2, 0, 0, 0, 0, time.UTC),
			saver: persistence.NewLocalNamedSaver(path.Join(dir, "success.json")),
			want: []time.Time{
				time.Date(2019, time.July, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2019, time.July, 3, 0, 0, 0, 0, time.UTC),
				time.Date(2019, time.July, 4, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name:  "success-reload-with-later-start",
			now:   time.Now(),
			start: time.Date(2020, time.June, 30, 0, 0, 0, 0, time.UTC),
			saver: persistence.NewLocalNamedSaver(path.Join(dir, "success.json")),
			want: []time.Time{
				time.Date(2020, time.June, 30, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.July, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.July, 2, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name:  "success-restart-wrap-before-12hours",
			now:   time.Date(2022, time.July, 19, 0, 5, 0, 0, time.UTC), // 00:05-after UTC midnight.
			start: time.Date(2022, time.July, 17, 0, 0, 0, 0, time.UTC),
			saver: &noopSaver{},
			want: []time.Time{
				time.Date(2022, time.July, 17, 0, 0, 0, 0, time.UTC),
				time.Date(2022, time.July, 17, 0, 0, 0, 0, time.UTC), // We never reach the 18th.
				time.Date(2022, time.July, 17, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name:  "success-restart-wrap-after-12hours",
			now:   time.Date(2022, time.July, 19, 12, 5, 0, 0, time.UTC), // 12:05-after UTC midnight.
			start: time.Date(2022, time.July, 17, 0, 0, 0, 0, time.UTC),
			saver: &noopSaver{},
			want: []time.Time{
				time.Date(2022, time.July, 17, 0, 0, 0, 0, time.UTC),
				time.Date(2022, time.July, 18, 0, 0, 0, 0, time.UTC), // We reach the 18th, but reset on next attempt.
				time.Date(2022, time.July, 17, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name:    "error-failsave",
			now:     time.Now(),
			start:   time.Date(2019, time.June, 30, 0, 0, 0, 0, time.UTC),
			saver:   &failSaver{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monkey.Patch(time.Now, func() time.Time {
				return tt.now
			})
			defer monkey.Unpatch(time.Now)
			h := NewHistoricalIterator(tt.start, tt.saver)

			times := []time.Time{}
			for i := 0; i < 3; i++ {
				n, err := h.Next()
				if err != nil {
					break
				}
				times = append(times, n)
			}
			if tt.wantErr {
				return
			}
			if !reflect.DeepEqual(times, tt.want) {
				t.Errorf("NewHistoricalIterator() = got %v, want %v", times, tt.want)
			}
		})
	}
}

type incDateIterator struct {
	Date time.Time
}

func (f *incDateIterator) Next() (time.Time, error) {
	d := f.Date
	f.Date = f.Date.AddDate(0, 0, 1)
	return d, nil
}

type errAfter2DateIterator struct {
	Date time.Time
	i    int
}

func (f *errAfter2DateIterator) Next() (time.Time, error) {
	d := f.Date
	f.Date = f.Date.AddDate(0, 0, 1)
	f.i++
	if f.i > 2 {
		return time.Time{}, ErrNoDateAvailable
	}
	return d, nil
}

func TestNewJobIterator(t *testing.T) {
	type idOrErr struct {
		ID  tracker.Key
		err error
	}
	tests := []struct {
		name    string
		date    DateIterator
		specs   []tracker.JobWithTarget
		expect  []idOrErr
		wantErr bool
	}{
		{
			name: "success",
			date: &incDateIterator{Date: time.Date(2020, time.July, 1, 0, 0, 0, 0, time.UTC)},
			specs: []tracker.JobWithTarget{
				{Job: tracker.Job{Bucket: "bucket1", Experiment: "exp1", Datatype: "dt1"}, DailyOnly: true},
				{Job: tracker.Job{Bucket: "bucket2", Experiment: "exp2", Datatype: "dt2"}},
			},
			expect: []idOrErr{
				{ID: tracker.Key("bucket1/exp1/dt1/20200701")},
				{ID: tracker.Key("bucket2/exp2/dt2/20200701")},
				{ID: tracker.Key("bucket1/exp1/dt1/20200702")},
				{ID: tracker.Key("bucket2/exp2/dt2/20200702")},
				{ID: tracker.Key("bucket1/exp1/dt1/20200703")},
				{ID: tracker.Key("bucket2/exp2/dt2/20200703")},
			},
		},
		{
			name: "with-error",
			date: &errAfter2DateIterator{Date: time.Date(2020, time.July, 1, 0, 0, 0, 0, time.UTC)},
			specs: []tracker.JobWithTarget{
				{Job: tracker.Job{Bucket: "bucket1", Experiment: "exp1", Datatype: "dt1"}, DailyOnly: true},
			},
			expect: []idOrErr{
				{ID: tracker.Key("bucket1/exp1/dt1/20200701")},
				{ID: tracker.Key("bucket1/exp1/dt1/20200702")},
				{err: ErrNoDateAvailable},
				{err: ErrNoDateAvailable},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := NewJobIterator(tt.date, tt.specs)

			if j.Len() != len(tt.specs) {
				t.Errorf("JobIterator.Len() returned wrong length; got %d, want %d", j.Len(), len(tt.specs))
			}
			for i := 0; i < len(tt.expect); i++ {
				jt, err := j.Next()
				if jt != nil && jt.ID != tt.expect[i].ID {
					t.Errorf("JobIterator.Next() ID is not expected; got %q, want %q", jt.ID, tt.expect[i].ID)
				}
				if err != tt.expect[i].err {
					t.Errorf("JobIterator.Next() error is not expected; got %v, want %t", err, tt.expect[i].err)
				}
			}
		})
	}
}
