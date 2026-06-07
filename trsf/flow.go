package trsf

var _ FlowController = (*flowController)(nil)

type flowController struct {
	WindowSize int
	SentSize   int
}

func (fc *flowController) SendableSize() int {
	return fc.WindowSize - fc.SentSize
}

func (fc *flowController) RecordSend(size int) {
	fc.SentSize += size
}

func (fc *flowController) UpdateWindow(size int) bool {
	if size > fc.WindowSize {
		fc.WindowSize = size
		return true
	}
	return false
}

func newFlowController(windowSize int) *flowController {
	return &flowController{
		WindowSize: windowSize,
	}
}
