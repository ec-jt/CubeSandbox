package workflow

import "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/sourcefence"

// Initialized by the local cubebox plugin before recovery or service startup.
// Enforcement is independent of the admission configuration flag.
var SourceFences *sourcefence.Store
