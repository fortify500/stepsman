package bl

import "fmt"

var ErrActiveRunsWithSameTitleExists = fmt.Errorf("active runs with the same title detected")
var ErrRecordNotFound = fmt.Errorf("failed to locate record")
