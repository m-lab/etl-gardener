package rex

func GetQueueChan(ss *ReprocState) chan<- string {
	return ss.queues
}
