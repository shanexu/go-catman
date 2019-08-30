package catman

import (
	"reflect"
	"testing"
)

func Test_path2Seq(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    int64
		wantErr bool
	}{
		{"case 1", args{"-123"}, 123, false},
		{"case 2", args{"123"}, 0, true},
		{"case 3", args{"-00001"}, 1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := path2Seq(tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("path2Seq() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("path2Seq() got = %v, want %v", got, tt.want)
			}
		})
	}
}
