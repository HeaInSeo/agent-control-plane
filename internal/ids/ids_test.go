package ids_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/HeaInSeo/agent-control-plane/internal/ids"
)

func TestMintedIdentifiersValidate(t *testing.T) {
	checks := map[string]error{
		string(ids.NewPacketID()):            ids.NewPacketID().Validate(),
		string(ids.NewTaskID()):              ids.NewTaskID().Validate(),
		string(ids.NewAttemptID()):           ids.NewAttemptID().Validate(),
		string(ids.NewWorkspaceID()):         ids.NewWorkspaceID().Validate(),
		string(ids.NewRepositorySubjectID()): ids.NewRepositorySubjectID().Validate(),
		string(ids.NewPublishAttemptID()):    ids.NewPublishAttemptID().Validate(),
		string(ids.NewEvidenceID()):          ids.NewEvidenceID().Validate(),
		string(ids.NewEventID()):             ids.NewEventID().Validate(),
		string(ids.NewSchedulerOwnerID()):    ids.NewSchedulerOwnerID().Validate(),
	}
	for id, err := range checks {
		if err != nil {
			t.Fatalf("freshly minted %q failed validation: %v", id, err)
		}
	}
}

func TestIdentifiersAreUnique(t *testing.T) {
	seen := make(map[ids.AttemptID]struct{}, 1000)
	for range 1000 {
		id := ids.NewAttemptID()
		if _, dup := seen[id]; dup {
			t.Fatalf("minted a duplicate identifier: %s", string(id))
		}
		seen[id] = struct{}{}
	}
}

// An identifier from one kind must not validate as another. This is the data
// half of the separation; the type system covers the code half.
func TestIdentifierKindsDoNotCrossValidate(t *testing.T) {
	task := ids.NewTaskID()
	attempt := ids.NewAttemptID()
	workspace := ids.NewWorkspaceID()

	if err := ids.TaskID(attempt).Validate(); !errors.Is(err, ids.ErrMalformedID) {
		t.Fatalf("an attempt id validated as a task id: %v", err)
	}
	if err := ids.AttemptID(task).Validate(); !errors.Is(err, ids.ErrMalformedID) {
		t.Fatalf("a task id validated as an attempt id: %v", err)
	}
	if err := ids.WorkspaceID(attempt).Validate(); !errors.Is(err, ids.ErrMalformedID) {
		t.Fatalf("an attempt id validated as a workspace id: %v", err)
	}
	if err := ids.PublishAttemptID(workspace).Validate(); !errors.Is(err, ids.ErrMalformedID) {
		t.Fatalf("a workspace id validated as a publish attempt id: %v", err)
	}
	if err := ids.EvidenceID(task).Validate(); !errors.Is(err, ids.ErrMalformedID) {
		t.Fatalf("a task id validated as an evidence id: %v", err)
	}
}

func TestMalformedIdentifiersAreRejected(t *testing.T) {
	for _, bad := range []string{
		"",
		"task",
		"task_",
		"task_short",
		"task_" + strings.Repeat("f", 31),
		"task_" + strings.Repeat("f", 33),
		"task_" + strings.Repeat("z", 32),
		"TASK_" + strings.Repeat("a", 32),
		" task_" + strings.Repeat("a", 32),
	} {
		if err := ids.TaskID(bad).Validate(); !errors.Is(err, ids.ErrMalformedID) {
			t.Fatalf("%q: want ErrMalformedID, got %v", bad, err)
		}
	}
}

func TestIdentifiersCarryTheirKindAsAPrefix(t *testing.T) {
	prefixes := map[string]string{
		string(ids.NewPacketID()):            "pkt_",
		string(ids.NewTaskID()):              "task_",
		string(ids.NewAttemptID()):           "att_",
		string(ids.NewWorkspaceID()):         "ws_",
		string(ids.NewRepositorySubjectID()): "rsub_",
		string(ids.NewPublishAttemptID()):    "pub_",
		string(ids.NewEvidenceID()):          "evd_",
		string(ids.NewEventID()):             "evt_",
		string(ids.NewSchedulerOwnerID()):    "sched_",
	}
	for id, prefix := range prefixes {
		if !strings.HasPrefix(id, prefix) {
			t.Fatalf("%q does not carry prefix %q", id, prefix)
		}
	}
}
