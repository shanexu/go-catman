# catman
```go
package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"

	"github.com/shanexu/go-catman"
)

func main() {
	cm, err := catman.NewCatMan([]string{"127.0.0.1:2181"}, time.Second)
	if err != nil {
		panic(err)
	}
	defer cm.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	cs, err := cm.CMChildren("/children", catman.WatcherFunc(func(event zk.Event) {
		fmt.Println(event)
		wg.Done()
	}))
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", cs)
	wg.Wait()
}
```

# election

``` go
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shanexu/go-catman"
)

func main() {
	cm, err := catman.NewCatMan([]string{"127.0.0.1:2181"}, time.Second)
	if err != nil {
		panic(err)
	}
	defer cm.Close()
	l := cm.NewLeaderElectionSupport("myhost", "/election")
	l.AddListener(catman.LeaderElectionAwareFunc(func(event catman.ElectionEvent) {
		fmt.Println("ElectionEvent:", event)
	}))
	if err := l.Start(); err != nil {
		panic(err)
	}
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit
	if err := l.Stop(); err != nil {
		panic(err)
	}
}
```
# queue

``` go
package main

import (
	"fmt"
	"time"

	"github.com/shanexu/go-catman"
)

func main() {
	cm, err := catman.NewCatMan([]string{"127.0.0.1:2181"}, time.Second)
	defer cm.Close()
	if err != nil {
		panic(err)
	}
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

	data, err = q.Take()
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
	cm, err := catman.NewCatMan([]string{"127.0.0.1:2181"}, time.Second)
	if err != nil {
		panic(err)
	}
	go func() {
		l := cm.NewLock("/mylock", catman.OpenAclUnsafe, lockListener(0))
		l.Lock()
		fmt.Println("locked")
		time.Sleep(time.Second * 10)
		l.Unlock()
	}()

	l := cm.NewLock("/mylock", catman.OpenAclUnsafe, lockListener(1))
	l.Lock()
	fmt.Println("locked")
	time.Sleep(time.Second * 10)
	l.Unlock()
}
```
