package main

import (
	"reflect"
	"testing"
)

// TestVariedParamObject_emits_schema_v2_shape checks the bench
// harness's varied_param emission. For experiment.kind == "ab" the
// field must be a structured object {kind, name, baseline,
// candidate}; missing any of the four fields → varied_param is null.
func TestVariedParamObject_emits_schema_v2_shape(t *testing.T) {
	t.Run("populated when all four fields are set", func(t *testing.T) {
		got := variedParamObject(args{
			variedParamKind:      "config",
			variedParamName:      "ManifestAppendBatchSize",
			variedParamBaseline:  "1",
			variedParamCandidate: "16",
		})
		want := map[string]any{
			"kind":      "config",
			"name":      "ManifestAppendBatchSize",
			"baseline":  "1",
			"candidate": "16",
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("variedParamObject = %#v, want %#v", got, want)
		}
	})

	cases := []struct {
		name string
		a    args
	}{
		{"missing kind", args{variedParamName: "x", variedParamBaseline: "1", variedParamCandidate: "2"}},
		{"missing name", args{variedParamKind: "config", variedParamBaseline: "1", variedParamCandidate: "2"}},
		{"missing baseline", args{variedParamKind: "config", variedParamName: "x", variedParamCandidate: "2"}},
		{"missing candidate", args{variedParamKind: "config", variedParamName: "x", variedParamBaseline: "1"}},
		{"all empty (non-A/B run)", args{}},
	}
	for _, c := range cases {
		t.Run(c.name+" → nil", func(t *testing.T) {
			if got := variedParamObject(c.a); got != nil {
				t.Errorf("variedParamObject = %#v, want nil", got)
			}
		})
	}
}
