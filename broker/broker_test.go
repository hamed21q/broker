package broker

import (
	db "BaleBroker/db/memory"
	"BaleBroker/pkg"
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var (
	service *BaleBroker
	mainCtx = context.Background()
	letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func TestMain(m *testing.M) {
	rand.Seed(time.Now().Unix())
	tearDownTest()
	m.Run()
}

func tearDownTest() {
	idGenerator := pkg.NewSequentialIdentifier()
	memoryDB := db.NewMemoryDB()
	service = NewBaleBroker(memoryDB, idGenerator)
}

func TestPublishShouldFailOnClosed(t *testing.T) {
	defer tearDownTest()
	msg := createMessage()

	err := service.Close()
	assert.Nil(t, err)

	_, err = service.Publish(mainCtx, "ali", msg)
	assert.Equal(t, ErrUnavailable, err)
}

func TestSubscribeShouldFailOnClosed(t *testing.T) {
	defer tearDownTest()
	err := service.Close()
	assert.Nil(t, err)

	_, err = service.Subscribe(mainCtx, "ali")
	assert.Equal(t, ErrUnavailable, err)
}

func TestFetchShouldFailOnClosed(t *testing.T) {
	defer tearDownTest()
	err := service.Close()
	assert.Nil(t, err)

	_, err = service.Fetch(mainCtx, "ali", rand.Intn(100))
	assert.Equal(t, ErrUnavailable, err)
}

func TestPublishShouldNotFail(t *testing.T) {
	defer tearDownTest()
	msg := createMessage()

	_, err := service.Publish(mainCtx, "ali", msg)

	assert.Equal(t, nil, err)
}

func TestSubscribeShouldNotFail(t *testing.T) {
	defer tearDownTest()
	sub, err := service.Subscribe(mainCtx, "ali")

	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, sub)
}

func TestPublishShouldSendMessageToSubscribedChan(t *testing.T) {
	defer tearDownTest()
	msg := createMessage()

	sub, _ := service.Subscribe(mainCtx, "ali")
	var wg sync.WaitGroup
	wg.Add(1)
	go func(s <-chan pkg.Message) {
		defer wg.Done()
		in := <-s
		equalMessage(t, msg, in)
	}(sub)
	_, _ = service.Publish(mainCtx, "ali", msg)
	wg.Wait()
}

func TestPublishShouldSendMessageToSubscribedChans(t *testing.T) {
	defer tearDownTest()
	msg := createMessage()

	sub1, _ := service.Subscribe(mainCtx, "ali")
	sub2, _ := service.Subscribe(mainCtx, "ali")
	sub3, _ := service.Subscribe(mainCtx, "ali")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		in1 := <-sub1
		in2 := <-sub2
		in3 := <-sub3

		equalMessage(t, msg, in1)
		equalMessage(t, msg, in2)
		equalMessage(t, msg, in3)
	}()
	_, _ = service.Publish(mainCtx, "ali", msg)
	wg.Wait()
}

func TestPublishShouldPreserveOrder(t *testing.T) {
	defer tearDownTest()
	n := 50
	messages := make([]pkg.Message, n)
	sub, _ := service.Subscribe(mainCtx, "ali")
	go func() {
		for i := 0; i < n; i++ {
			msg := <-sub
			assert.Equal(t, messages[i].Body, msg.Body)
		}
	}()
	for i := 0; i < n; i++ {
		messages[i] = createMessage()
		_, _ = service.Publish(mainCtx, "ali", messages[i])
	}
}

func TestPublishShouldNotSendToOtherSubscriptions(t *testing.T) {
	defer tearDownTest()
	msg := createMessage()
	ali, _ := service.Subscribe(mainCtx, "ali")
	maryam, _ := service.Subscribe(mainCtx, "maryam")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case m := <-ali:
			equalMessage(t, msg, m)
		case <-maryam:
			assert.Fail(t, "Wrong message received")
		}
	}()
	_, _ = service.Publish(mainCtx, "ali", msg)
	wg.Wait()
}

func TestNonExpiredMessageShouldBeFetchable(t *testing.T) {
	defer tearDownTest()
	msg := createMessageWithExpire(time.Second * 10)
	id, _ := service.Publish(mainCtx, "ali", msg)
	fMsg, err := service.Fetch(mainCtx, "ali", id)

	require.NoError(t, err)
	equalMessage(t, msg, *fMsg)
}

func TestExpiredMessageShouldNotBeFetchable(t *testing.T) {
	defer tearDownTest()
	msg := createMessageWithExpire(time.Millisecond * 500)
	id, _ := service.Publish(mainCtx, "ali", msg)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	<-ticker.C
	fMsg, err := service.Fetch(mainCtx, "ali", id)
	assert.Equal(t, ErrExpiredID, err)
	assert.Nil(t, nil, fMsg)
}

func TestNewSubscriptionShouldNotGetPreviousMessages(t *testing.T) {
	defer tearDownTest()
	msg := createMessage()
	_, _ = service.Publish(mainCtx, "ali", msg)
	sub, _ := service.Subscribe(mainCtx, "ali")

	select {
	case <-sub:
		assert.Fail(t, "Got previous message")
	default:
	}
}

func TestConcurrentSubscribesOnOneSubjectShouldNotFail(t *testing.T) {
	defer tearDownTest()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var wg sync.WaitGroup

	for {
		select {
		case <-ticker.C:
			wg.Wait()
			return

		default:
			wg.Add(1)
			go func() {
				defer wg.Done()

				_, err := service.Subscribe(mainCtx, "ali")
				assert.Nil(t, err)
			}()
		}
	}
}

func TestConcurrentSubscribesShouldNotFail(t *testing.T) {
	defer tearDownTest()
	ticker := time.NewTicker(2000 * time.Millisecond)
	defer ticker.Stop()
	var wg sync.WaitGroup

	for {
		select {
		case <-ticker.C:
			wg.Wait()
			return

		default:
			wg.Add(1)
			go func() {
				defer wg.Done()

				_, err := service.Subscribe(mainCtx, randomString(4))
				assert.Nil(t, err)
			}()
		}
	}
}

func TestConcurrentPublishOnOneSubjectShouldNotFail(t *testing.T) {
	defer tearDownTest()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var wg sync.WaitGroup

	msg := createMessage()

	for {
		select {
		case <-ticker.C:
			wg.Wait()
			return

		default:
			wg.Add(1)
			go func() {
				defer wg.Done()

				_, err := service.Publish(mainCtx, "ali", msg)
				assert.Nil(t, err)
			}()
		}
	}
}

func TestConcurrentPublishShouldNotFail(t *testing.T) {
	defer tearDownTest()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var wg sync.WaitGroup

	msg := createMessage()

	for {
		select {
		case <-ticker.C:
			wg.Wait()
			return

		default:
			wg.Add(1)
			go func() {
				defer wg.Done()

				_, err := service.Publish(mainCtx, randomString(4), msg)
				assert.Nil(t, err)
			}()
		}
	}
}

func TestDataRace(t *testing.T) {
	duration := 500 * time.Millisecond
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	var wg sync.WaitGroup

	ids := make(chan int, 100000)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ticker.C:
				return

			default:
				id, err := service.Publish(mainCtx, "ali", createMessageWithExpire(duration))
				ids <- id
				assert.Nil(t, err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ticker.C:
				return
			default:
				c, err := service.Subscribe(mainCtx, "ali")
				go func() {
					for {
						<-c
					}
				}()
				assert.Nil(t, err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ticker.C:
				return

			case id := <-ids:
				_, err := service.Fetch(mainCtx, "ali", id)
				assert.Nil(t, err)
			}
		}
	}()

	wg.Wait()
}

func BenchmarkPublish(b *testing.B) {
	defer tearDownTest()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := service.Publish(mainCtx, randomString(2), createMessage())
		assert.Nil(b, err)
	}
}

func BenchmarkSubscribe(b *testing.B) {
	defer tearDownTest()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := service.Subscribe(mainCtx, randomString(2))
		assert.Nil(b, err)
	}
}

func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func createMessage() pkg.Message {
	body := randomString(16)

	return pkg.Message{
		Body:       body,
		Expiration: 0,
	}
}

func createMessageWithExpire(duration time.Duration) pkg.Message {
	body := randomString(16)

	return pkg.Message{
		Body:       body,
		Expiration: duration,
	}
}

func equalMessage(t *testing.T, m1, m2 pkg.Message) {
	assert.Equal(t, m1.Body, m2.Body)
}
