package state

const LastReason = Reason(len(reasonNames) - 1)

func (m *Machine) Transfers() int {
	return m.ntransfers
}
