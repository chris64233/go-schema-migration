package schemamigration

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct{ t atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.t.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	return c
}
func (c *fakeClock) now() time.Time          { return time.Unix(0, c.t.Load()).UTC() }
func (c *fakeClock) advance(d time.Duration) { c.t.Add(int64(d)) }

type idGen struct{ n atomic.Int64 }

func (g *idGen) gen(prefix string) string {
	n := g.n.Add(1)
	return fmt.Sprintf("%s-%03d", prefix, n)
}

type testHarness struct {
	t     *testing.T
	svc   *Service
	store Store
	clock *fakeClock
	ctx   context.Context
}

func newHarness(t *testing.T, store Store, ttl time.Duration) *testHarness {
	t.Helper()
	clock := newFakeClock()
	ids := &idGen{}
	svc := New(store, WithClock(clock.now), WithIDGenerator(ids.gen), WithLeaseTTL(ttl))
	return &testHarness{t: t, svc: svc, store: store, clock: clock, ctx: context.Background()}
}

// mustPlan 创建并发布一个 v1->v2->v3 的三步计划，可配置每步属性。
func (h *testHarness) mustPlan(id string, requireConfirm []bool, rollbackable []bool) *Plan {
	h.t.Helper()
	steps := []Step{
		{FromVersion: "v1", ToVersion: "v2", RequireConfirm: requireConfirm[0], Rollbackable: rollbackable[0]},
		{FromVersion: "v2", ToVersion: "v3", RequireConfirm: requireConfirm[1], Rollbackable: rollbackable[1]},
		{FromVersion: "v3", ToVersion: "v4", RequireConfirm: requireConfirm[2], Rollbackable: rollbackable[2]},
	}
	p, err := h.svc.CreatePlan(h.ctx, id, "plan-"+id, steps)
	if err != nil {
		h.t.Fatalf("CreatePlan: %v", err)
	}
	p, err = h.svc.PublishPlan(h.ctx, id)
	if err != nil {
		h.t.Fatalf("PublishPlan: %v", err)
	}
	return p
}

// runStep 领取并成功回执一个前进步，返回回执后的执行态。
func (h *testHarness) runStep(tenant, worker string) (*Lease, *Execution) {
	h.t.Helper()
	l, err := h.svc.ClaimStep(h.ctx, tenant, worker)
	if err != nil {
		h.t.Fatalf("ClaimStep: %v", err)
	}
	e, err := h.svc.ReportStep(h.ctx, tenant, StepReceipt{
		StepIndex: l.StepIndex, Attempt: l.Attempt, LeaseToken: l.LeaseToken, Succeeded: true,
	})
	if err != nil {
		h.t.Fatalf("ReportStep: %v", err)
	}
	return l, e
}

func assertErrIs(t *testing.T, err error, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("want error %v, got %v", target, err)
	}
}
