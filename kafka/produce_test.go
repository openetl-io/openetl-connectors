package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/openetl-io/openetl-connectors/records"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// stagedUpTo writes and pre-commits epochs 1 to n of generation 1 on w,
// three ids each.
func (l *lanes) stagedUpTo(w *writer, n uint64) map[uint64]destination.Staged {
	l.t.Helper()
	out := map[uint64]destination.Staged{}
	for i := uint64(1); i <= n; i++ {
		out[i] = l.stage(w, epochOf(1, i), int(i-1)*3, int(i)*3)
	}
	return out
}

// write writes epoch n's three ids on w.
func (l *lanes) write(ctx context.Context, w *writer, n uint64) error {
	b := idRows(l.table, int(n-1)*3, int(n)*3)
	defer b.Release()
	return w.Write(ctx, epochOf(1, n), l.table, b)
}

// preCommit pre-commits epoch n of generation 1 on w.
func (l *lanes) preCommit(w *writer, n uint64) destination.Staged {
	l.t.Helper()
	st, err := w.PreCommit(l.t.Context(), epochOf(1, n))
	if err != nil {
		l.t.Fatalf("epoch %d's pre-commit: %v", n, err)
	}
	return st
}

// commitFrom commits epochs from to the last of st on w, in order.
func (l *lanes) commitFrom(w *writer, from uint64, st map[uint64]destination.Staged) {
	l.t.Helper()
	for n := from; st[n] != nil; n++ {
		l.commit(w, epochOf(1, n), st[n])
	}
}

// With a transform, OpenETL has up to three epochs staged and not yet
// committed while it writes the next, which is on the first one's
// producer. That epoch's writes don't wait for the first one's commit,
// which comes after them on the writer's one stream from OpenETL: its rows
// go into its transaction once the commit has ended the first one's, and
// every epoch lands once.
func TestAnEpochOnAProducerStillOpenIsHeldForIt(t *testing.T) {
	l := newLanes(t)
	w := l.open(1, 0)
	st := l.stagedUpTo(w, 3)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := l.write(ctx, w, 4); err != nil {
		t.Fatalf("epoch 4's write, with epoch 1 open on its producer: %v", err)
	}
	l.commit(w, epochOf(1, 1), st[1])
	l.want("after epoch 1's commit", 0, 3)
	st[4] = l.preCommit(w, 4)
	l.commitFrom(w, 2, st)
	l.want("after the four epochs", 0, 12)
}

// A write of an epoch whose producer an earlier epoch's commit is ending
// waits for that commit, which nothing else holds up, and stops waiting
// once its context ends.
func TestAWriteWaitsForTheCommitEndingItsProducer(t *testing.T) {
	l := newLanes(t)
	w := l.open(1, 0)
	st := l.stagedUpTo(w, 3)
	ending, release := make(chan struct{}), make(chan struct{})
	l.c.ControlKey(int16(kmsg.EndTxn), func(kmsg.Request) (kmsg.Response, error, bool) {
		l.c.DropControl()
		close(ending)
		l.c.SleepControl(func() { <-release })
		return nil, nil, false
	})
	committed := make(chan error, 1)
	go func() {
		_, err := w.Commit(t.Context(), epochOf(1, 1), []destination.Staged{st[1]})
		committed <- err
	}()
	<-ending
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if err := l.write(ctx, w, 4); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("epoch 4's write, while epoch 1's commit was held up: %v", err)
	}
	wrote := make(chan error, 1)
	go func() { wrote <- l.write(t.Context(), w, 4) }()
	select {
	case err := <-wrote:
		t.Fatalf("epoch 4's write returned %v before epoch 1's commit", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	if err := <-committed; err != nil {
		t.Fatalf("epoch 1's commit: %v", err)
	}
	if err := <-wrote; err != nil {
		t.Fatalf("epoch 4's write: %v", err)
	}
	st[4] = l.preCommit(w, 4)
	l.commitFrom(w, 2, st)
	l.want("after the four epochs", 0, 12)
}

// A commit that fails leaves the writer to go on: an epoch held for its
// producer goes into a transaction of its own once the failed commit has
// ended the producer's.
func TestAnEpochHeldForAFailedCommitGoesOn(t *testing.T) {
	l := newLanes(t)
	w := l.open(1, 0)
	st := l.stagedUpTo(w, 3)
	if err := l.write(t.Context(), w, 4); err != nil {
		t.Fatalf("epoch 4's write, with epoch 1 open on its producer: %v", err)
	}
	l.c.ControlKey(int16(kmsg.EndTxn), func(req kmsg.Request) (kmsg.Response, error, bool) {
		r := req.(*kmsg.EndTxnRequest)
		if !r.Commit {
			return nil, nil, false
		}
		resp := r.ResponseKind().(*kmsg.EndTxnResponse)
		resp.ErrorCode = kerr.UnknownServerError.Code
		return resp, nil, true
	})
	if _, err := w.Commit(t.Context(), epochOf(1, 1), []destination.Staged{st[1]}); err == nil {
		t.Fatal("epoch 1 committed though the brokers failed its commit")
	}
	st[4] = l.preCommit(w, 4)
	l.commitFrom(w, 2, st)
	l.want("after epoch 1's commit failed", 3, 12)
}

// A row too large for a record fails its epoch with an error that names
// the topic and the limit, rather than the broker's code alone.
func TestARowTooLargeNamesItsTopicAndTheLimit(t *testing.T) {
	l := newLanes(t)
	w := l.open(1, 0)
	md := arrow.NewMetadata([]string{records.TableKey}, []string{l.table})
	sc := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "blob", Type: arrow.BinaryTypes.String}}, &md)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.StringBuilder).Append(strings.Repeat("x", 1100000))
	batch := b.NewRecordBatch()
	defer batch.Release()
	e := epochOf(1, 1)
	if err := w.Write(t.Context(), e, l.table, batch); err != nil {
		t.Fatal(err)
	}
	_, err := w.PreCommit(t.Context(), e)
	if err == nil || !strings.Contains(err.Error(), l.table) || !strings.Contains(err.Error(), "1000012") || codeOf(err) != "ERROR_CODE_DATA_INVALID" {
		t.Errorf("a row of 1.1 MB pre-committed with %s %v", codeOf(err), err)
	}
}

// A topic whose max.message.bytes is below the client's batches refuses
// batches of rows that each fit: its config is to change, not the rows.
func TestATopicLimitBelowTheClientsBatchesIsConfig(t *testing.T) {
	l := newLanes(t)
	cl, err := kgo.NewClient(kgo.SeedBrokers(l.addrs...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).CreateTopic(t.Context(), 1, 1, map[string]*string{"max.message.bytes": kadm.StringPtr("1000")}, l.table); err != nil {
		t.Fatal(err)
	}
	w := l.open(1, 0)
	e := epochOf(1, 1)
	b := idRows(l.table, 0, 2000)
	defer b.Release()
	if err := w.Write(t.Context(), e, l.table, b); err != nil {
		t.Fatal(err)
	}
	_, err = w.PreCommit(t.Context(), e)
	if codeOf(err) != "ERROR_CODE_CONFIG_INVALID" || !strings.Contains(err.Error(), "max.message.bytes") {
		t.Errorf("pre-commit: %s %v", codeOf(err), err)
	}
}

// A transaction open past its timeout, which the brokers abort, fails its
// epoch as transient, at the commit, at a commit again, as OpenETL retries
// one, or at the pre-commit: no newer writer owns the lane, as a fence
// would say.
func TestATransactionTheBrokersTimedOutIsNoFence(t *testing.T) {
	defer func(s time.Duration) { txnSlack = s }(txnSlack)
	txnSlack = 0
	l := newLanes(t)
	w := l.open(1, 0)
	e1, e2 := epochOf(1, 1), epochOf(1, 2)
	st := l.stage(w, e1, 0, 3)
	b := idRows(l.table, 3, 6)
	defer b.Release()
	if err := w.Write(t.Context(), e2, l.table, b); err != nil {
		t.Fatal(err)
	}
	time.Sleep(timeout(time.Second) + time.Second)
	for _, what := range []string{"commit", "commit again"} {
		_, err := w.Commit(t.Context(), e1, []destination.Staged{st})
		if errors.Is(err, destination.ErrFenced) || codeOf(err) != "ERROR_CODE_TRANSIENT" {
			t.Errorf("%s: %s %v", what, codeOf(err), err)
		}
	}
	_, err := w.PreCommit(t.Context(), e2)
	if errors.Is(err, destination.ErrFenced) || codeOf(err) != "ERROR_CODE_TRANSIENT" {
		t.Errorf("pre-commit: %s %v", codeOf(err), err)
	}
	l.want("after the brokers aborted both", 0, 0)
}

// A commit that the brokers fail, which Commit settles by aborting the
// transaction, fails its epoch as transient, and so does a commit again,
// as OpenETL retries one: no newer writer owns the lane, as a fence would
// say, and the next attempt writes the epoch again. The lane goes on.
func TestACommitTheBrokersFailedIsNoFence(t *testing.T) {
	l := newLanes(t)
	w := l.open(1, 0)
	e1, e2 := epochOf(1, 1), epochOf(1, 2)
	st := l.stage(w, e1, 0, 3)
	l.c.ControlKey(int16(kmsg.EndTxn), func(req kmsg.Request) (kmsg.Response, error, bool) {
		r := req.(*kmsg.EndTxnRequest)
		if !r.Commit {
			return nil, nil, false
		}
		resp := r.ResponseKind().(*kmsg.EndTxnResponse)
		resp.ErrorCode = kerr.UnknownServerError.Code
		return resp, nil, true
	})
	for _, what := range []string{"commit", "commit again"} {
		_, err := w.Commit(t.Context(), e1, []destination.Staged{st})
		if errors.Is(err, destination.ErrFenced) || codeOf(err) != "ERROR_CODE_TRANSIENT" {
			t.Errorf("%s: %s %v", what, codeOf(err), err)
		}
	}
	l.want("after the failed commit", 0, 0)
	l.commit(w, e2, l.stage(w, e2, 3, 6))
	l.want("after the next epoch", 3, 6)
}
