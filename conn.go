package redis

import (
	"context"
	"log"
	"runtime"
	"sync/atomic"

	"github.com/redis/go-redis/v9/internal/pool"
)

type connSentinel struct {
	pool   pool.Pooler
	closed uint32
}

func (s *connSentinel) Close() error {
	if !atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return nil
	}
	return s.pool.Close()
}

type Conn struct {
	*statefulCmdable
	baseClient
	ctx context.Context

	sentinel *connSentinel
}

func newConn(opt *Options, connPool pool.Pooler) *Conn {
	c := &Conn{
		baseClient: baseClient{
			opt:      opt,
			connPool: connPool,
		},
	}
	c.statefulCmdable = newStatefulCmdable(c.Process)
	c.sentinel = &connSentinel{
		pool: connPool,
	}
	runtime.SetFinalizer(c.sentinel, func(s *connSentinel) {
		log.Printf("WARNING: go-redis: connection leak detected. Conn was not closed properly.")
		_ = s.Close()
	})
	return c
}

func (c *Conn) Process(ctx context.Context, cmd Cmder) error {
	return c.baseClient.process(ctx, cmd)
}

func (c *Conn) Close() error {
	if c.sentinel != nil {
		runtime.SetFinalizer(c.sentinel, nil)
		err := c.sentinel.Close()
		c.sentinel = nil
		return err
	}
	return c.baseClient.Close()
}

func (c *Conn) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipeline().Pipelined(ctx, fn)
}

func (c *Conn) Pipeline() Pipeliner {
	pipe := Pipeline{
		ctx: c.ctx,
	}
	pipe.init()
	pipe.setProcessor(c.Process)
	return &pipe
}

func (c *Conn) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.TxPipeline().TxPipelined(ctx, fn)
}

func (c *Conn) TxPipeline() Pipeliner {
	pipe := TxPipeline{
		Pipeline: Pipeline{
			ctx: c.ctx,
		},
	}
	pipe.init()
	pipe.setProcessor(c.Process)
	return &pipe
}