package catman

import (
	"testing"
)

func test_findCandidateJ(t *testing.T) {
	c, err := findCandidateJ([]candidate{{"_c_5ef0d137074f090569e4f22732f6fd0f-0000000005", 5}}, candidate{"/testle/_c_5ef0d137074f090569e4f22732f6fd0f-0000000005", 5})
	if err != nil {
		t.Fatal(c)
	}
	if c != nil {
		t.Fatal("c is not nil")
	}
}
