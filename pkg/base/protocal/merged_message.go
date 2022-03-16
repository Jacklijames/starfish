package protocal

// MergedWarpMessage
type MergedWarpMessage struct {
	Msgs   []MessageTypeAware
	MsgIDs []int32
}

// GetTypeCode
func (req MergedWarpMessage) GetTypeCode() int16 {
	return TypeStarfishMerge
}

// MergeResultMessage
type MergeResultMessage struct {
	Msgs []MessageTypeAware
}

// GetTypeCode
func (resp MergeResultMessage) GetTypeCode() int16 {
	return TypeStarfishMergeResult
}
