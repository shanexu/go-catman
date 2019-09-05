package catman

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/emirpasic/gods/sets/treeset"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/atomic"
)

func TestCatMan_NewLocker(t *testing.T) {
	type fields struct {
		Conn       *zk.Conn
		defaultACL []zk.ACL
	}
	type args struct {
		dir string
		acl []zk.ACL
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Lock
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := &CatMan{
				Conn:       tt.fields.Conn,
				defaultACL: tt.fields.defaultACL,
			}
			if got := cm.NewLocker(tt.args.dir, tt.args.acl); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewLocker() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLock_Closed(t *testing.T) {
	type fields struct {
		cm         *CatMan
		acl        []zk.ACL
		closed     *atomic.Bool
		retryDelay time.Duration
		retryCount int
		dir        string
		id         string
		idName     *ZNodeName
		data       []byte
		l          sync.Mutex
		callback   LockListener
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Lock{
				cm:         tt.fields.cm,
				acl:        tt.fields.acl,
				closed:     tt.fields.closed,
				retryDelay: tt.fields.retryDelay,
				retryCount: tt.fields.retryCount,
				dir:        tt.fields.dir,
				id:         tt.fields.id,
				idName:     tt.fields.idName,
				data:       tt.fields.data,
				l:          tt.fields.l,
				callback:   tt.fields.callback,
			}
			if got := l.Closed(); got != tt.want {
				t.Errorf("Closed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLock_retryOperation(t *testing.T) {
	type fields struct {
		cm         *CatMan
		acl        []zk.ACL
		closed     *atomic.Bool
		retryDelay time.Duration
		retryCount int
		dir        string
		id         string
		idName     *ZNodeName
		data       []byte
		l          sync.Mutex
		callback   LockListener
	}
	type args struct {
		operation ZooKeeperOperation
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Lock{
				cm:         tt.fields.cm,
				acl:        tt.fields.acl,
				closed:     tt.fields.closed,
				retryDelay: tt.fields.retryDelay,
				retryCount: tt.fields.retryCount,
				dir:        tt.fields.dir,
				id:         tt.fields.id,
				idName:     tt.fields.idName,
				data:       tt.fields.data,
				l:          tt.fields.l,
				callback:   tt.fields.callback,
			}
			got, err := l.retryOperation(tt.args.operation)
			if (err != nil) != tt.wantErr {
				t.Errorf("retryOperation() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("retryOperation() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_largestLessThan(t *testing.T) {
	type args struct {
		sortedNames *treeset.Set
		idNmae      *ZNodeName
	}
	tests := []struct {
		name  string
		args  args
		wantZ *ZNodeName
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotZ := largestLessThan(tt.args.sortedNames, tt.args.idNmae); !reflect.DeepEqual(gotZ, tt.wantZ) {
				t.Errorf("largestLessThan() = %v, want %v", gotZ, tt.wantZ)
			}
		})
	}
}
