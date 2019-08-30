package catman

import (
	"reflect"
	"testing"
)

func Test_findCandidateJ(t *testing.T) {
	type args struct {
		cs   []candidate
		self candidate
	}
	tests := []struct {
		name    string
		args    args
		want    *candidate
		wantErr bool
	}{
		{
			"case no j",
			args{
				[]candidate{{"/testle/_c_5ef0d137074f090569e4f22732f6fd0f-0000000005", 5}},
				candidate{"/testle/_c_5ef0d137074f090569e4f22732f6fd0f-0000000005", 5},
			},
			nil,
			false,
		},
		{
			"case not in",
			args{
				[]candidate{{"/testle/_c_5ef0d137074f090569e4f22732f6fd0f-0000000005", 5}},
				candidate{"/testle/_c_5ef0d137074f090569e4f22732f6fd0f-0000000004", 4},
			},
			nil,
			true,
		},
		{
			"case normal",
			args{
				[]candidate{
					{"/testle/_c_5ef0d137074f090569e4f22732f6fd0f-0000000005", 5},
					{"/testle/_c_5ef0d137074f090569e4f22732f6fd0f-0000000004", 4},
				},
				candidate{"/testle/_c_5ef0d137074f090569e4f22732f6fd0f-0000000005", 5},
			},
			&candidate{"/testle/_c_5ef0d137074f090569e4f22732f6fd0f-0000000004", 4},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := findCandidateJ(tt.args.cs, tt.args.self)
			if (err != nil) != tt.wantErr {
				t.Errorf("findCandidateJ() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("findCandidateJ() got = %v, want %v", got, tt.want)
			}
		})
	}
}
