package catman

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/emirpasic/gods/sets/treeset"
	"github.com/samuel/go-zookeeper/zk"
)

func TestLock_retryOperation(t *testing.T) {
	type fields struct {
		retryDelay time.Duration
		retryCount int
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
		{
			"case1",
			fields{time.Millisecond, 3},
			args{ZooKeeperOperationFunc(func() (bool, error) {
				return true, nil
			})},
			true,
			false,
		},
		{
			"case2",
			fields{time.Millisecond, 3},
			args{func() ZooKeeperOperation {
				c := 0
				return ZooKeeperOperationFunc(func() (bool, error) {
					c++
					if c == 3 {
						return true, nil
					}
					return false, zk.ErrConnectionClosed
				})
			}()},
			true,
			false,
		},
		{
			"case2",
			fields{time.Millisecond, 2},
			args{func() ZooKeeperOperation {
				c := 0
				return ZooKeeperOperationFunc(func() (bool, error) {
					c++
					if c == 3 {
						return true, nil
					}
					return false, errors.New("Bang!")
				})
			}()},
			false,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Lock{
				retryDelay: tt.fields.retryDelay,
				retryCount: tt.fields.retryCount,
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
		{
			"case1",
			args{
				treeset.NewWith(ZNodeNameComparator, NewZNodeName("hello-0001"), NewZNodeName("hello-0003"), NewZNodeName("hello-0002")),
				NewZNodeName("hello-0001"),
			},
			nil,
		},
		{
			"case2",
			args{
				treeset.NewWith(ZNodeNameComparator, NewZNodeName("hello-0001"), NewZNodeName("hello-0003"), NewZNodeName("hello-0002")),
				NewZNodeName("hello-0002"),
			},
			NewZNodeName("hello-0001"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotZ := largestLessThan(tt.args.sortedNames, tt.args.idNmae); !reflect.DeepEqual(gotZ, tt.wantZ) {
				t.Errorf("largestLessThan() = %v, want %v", gotZ, tt.wantZ)
			}
		})
	}
}
