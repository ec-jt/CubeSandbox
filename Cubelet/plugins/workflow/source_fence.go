package workflow

import "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/sourcefence"

// Initialized by the local cubebox plugin before recovery or service startup.
// Enforcement is independent of the admission configuration flag.
var SourceFences *sourcefence.Store

// EvictionRecords holds the durable local-quiesced records written by LRU
// paused eviction. It is a separate root from SourceFences on purpose: an
// eviction record is proof for Master, not a node-wide deny gate, so it must
// not trip SourceFences.Any() (package cleanup) or SourceFences.Check().
// Initialized next to SourceFences.
var EvictionRecords *sourcefence.Store
