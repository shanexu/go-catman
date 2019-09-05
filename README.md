# leader election

``` go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/shanexu/go-catman"
)

func main() {
	done := make(chan struct{})
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second)
	if err != nil {
		panic(err)
	}

	cm := catman.NewCatMan(conn)
	cm.CMCreate("/leader_election", nil)
	cm.LeaderElector(context.Background(), func() {
		fmt.Println("takeLeaderShip")
		<-done
	}, "/testle", nil)

	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	close(done)
}
```
# queue

``` go
package main

import (
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/shanexu/go-catman"
)

func main() {
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second)
	if err != nil {
		panic(err)
	}

	cm := catman.NewCatMan(conn)
	q := cm.NewDistributedQueue("/queue")

	_, err = q.Offer([]byte("hello"))
	if err != nil {
		panic(err)
	}

	data, err := q.Element()
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s\n", data)

	data, err = q.Peek()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", data)
}
```

# lock
``` go
package main

import (
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/shanexu/go-catman"
)

type lockListener int

func (l lockListener) LockAcquired() {
	fmt.Printf("LockAcquired %d\n", l)
}

func (l lockListener) LockReleased() {
	fmt.Printf("LockReleased %d\n", l)
}

func main() {
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second)
	if err != nil {
		panic(err)
	}

	cm := catman.NewCatMan(conn)
	go func() {
		l := cm.NewLock("/lock", catman.OpenAclUnsafe, lockListener(0))
		l.Lock()
		fmt.Println("locked")
		time.Sleep(time.Second * 10)
		l.Unlock()
	}()

	l := cm.NewLock("/lock", catman.OpenAclUnsafe, lockListener(1))
	l.Lock()
	fmt.Println("locked")
	time.Sleep(time.Second * 10)
	l.Unlock()
}
```
