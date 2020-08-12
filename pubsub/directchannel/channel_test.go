package directchannel

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"berty.tech/go-orbit-db/iface"
	"github.com/libp2p/go-libp2p-core/host"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestInitDirectChannelFactory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	mn := mocknet.New(ctx)

	var (
		err           error
		reportedErr   error
		muReportedErr sync.Mutex
	)
	count := 10
	hosts := make([]host.Host, count)
	directChannelsFactories := make([]iface.DirectChannelFactory, count)

	for i := 0; i < count; i++ {
		hosts[i], err = mn.GenPeer()
		require.NoError(t, err)

		directChannelsFactories[i] = InitDirectChannelFactory(hosts[i])
	}

	err = mn.LinkAll()
	require.NoError(t, err)

	err = mn.ConnectAllButSelf()
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	require.GreaterOrEqual(t, count, 2)

	var (
		receivedMessages uint32 = 0
		expectedMessages uint32 = 0
	)

	for i := 0; i < count; i++ {
		for j := i + 1; j < count; j++ {
			wg.Add(1)
			expectedMessages++

			go func(i, j int) {
				initWg := sync.WaitGroup{}
				sentWg := sync.WaitGroup{}
				initWg.Add(2)

				defer func() {
					initWg.Wait()
					sentWg.Wait()
					wg.Done()
				}()

				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				expectedMessage := []byte(fmt.Sprintf("test-%d-%d", i, j))

				ch1, err := directChannelsFactories[j](ctx, hosts[i].ID(), nil)
				if err != nil {
					muReportedErr.Lock()
					reportedErr = err
					muReportedErr.Unlock()
					return
				}
				defer func() { _ = ch1.Close() }()

				ch2, err := directChannelsFactories[i](ctx, hosts[j].ID(), nil)
				if err != nil {
					muReportedErr.Lock()
					reportedErr = err
					muReportedErr.Unlock()
					return
				}

				defer func() { _ = ch2.Close() }()

				var (
					err1, err2 error
				)

				go func() {
					defer initWg.Done()

					err1 = ch1.Connect(ctx)
					if err1 != nil {
						muReportedErr.Lock()
						reportedErr = err1
						muReportedErr.Unlock()
						return
					}
				}()

				go func() {
					defer initWg.Done()

					err2 = ch2.Connect(ctx)
					if err2 != nil {
						muReportedErr.Lock()
						reportedErr = err2
						muReportedErr.Unlock()
						return
					}
				}()

				initWg.Wait()

				if err1 != nil || err2 != nil {
					return
				}

				initWg.Add(1)
				sentWg.Add(2)

				go func() {
					defer cancel()
					defer sentWg.Done()

					sub := ch2.Subscribe(ctx)
					initWg.Done()

					for evt := range sub {
						if e, ok := evt.(*iface.EventPubSubPayload); ok {
							if bytes.Equal(e.Payload, expectedMessage) {
								atomic.AddUint32(&receivedMessages, 1)
								return
							}
						}
					}
				}()

				initWg.Wait()

				err = ch1.Send(ctx, expectedMessage)
				sentWg.Done()

				if err != nil {
					muReportedErr.Lock()
					reportedErr = err
					muReportedErr.Unlock()
					return
				}

				sentWg.Wait()
			}(i, j)
		}
	}

	go func() {
		wg.Wait()
		cancel()
	}()

	<-ctx.Done()
	require.Equal(t, int(expectedMessages), int(atomic.LoadUint32(&receivedMessages)))
	muReportedErr.Lock()
	require.NoError(t, reportedErr)
	muReportedErr.Unlock()
}
