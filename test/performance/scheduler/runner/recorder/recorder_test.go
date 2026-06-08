/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package recorder

import (
	"testing"
	"time"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/test/performance/scheduler/runner/generator"
)

func TestRecordWorkloadStateUsesObjectTimestampsForFirstObservedAdmitted(t *testing.T) {
	creationTime := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	admissionTime := creationTime.Add(90 * time.Second)
	finishTime := creationTime.Add(2 * time.Minute)

	wl := utiltesting.MakeWorkload("wl", "ns").
		UID("uid").
		Creation(creationTime).
		Label(generator.ClassLabel, "large").
		Admission(&kueue.Admission{ClusterQueue: "cq"}).
		AdmittedAt(true, admissionTime).
		FinishedAt(finishTime).
		Obj()

	rec := New(time.Minute)
	rec.running.Store(true)
	rec.RecordWorkloadState(wl)

	select {
	case ev := <-rec.wlEvChan:
		rec.recordWLEvent(ev)
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for workload event")
	}

	got := rec.Store.WL[wl.UID]
	if got == nil {
		t.Fatal("Expected workload state to be recorded")
	}
	if got.TimeToAdmitMs != admissionTime.Sub(creationTime).Milliseconds() {
		t.Errorf("TimeToAdmitMs = %d, want %d", got.TimeToAdmitMs, admissionTime.Sub(creationTime).Milliseconds())
	}
	if got.TimeToFinishedMs != finishTime.Sub(creationTime).Milliseconds() {
		t.Errorf("TimeToFinishedMs = %d, want %d", got.TimeToFinishedMs, finishTime.Sub(creationTime).Milliseconds())
	}
}
