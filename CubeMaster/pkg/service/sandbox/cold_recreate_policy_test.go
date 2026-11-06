package sandbox

import "testing"

func TestColdRecreatePolicyIsInternalOnly(t *testing.T) {
	key := "cube.master.recreate.policy"
	if !rejectUserCubeMasterAnnotation(key) { t.Fatal("public Create must not authorize process loss") }
	if _, ok := stripUserCubeMasterLabels(map[string]string{key: "cold-retained-disk-discard-processes-v1"})[key]; ok {
		t.Fatal("public labels must not authorize process loss")
	}
}
