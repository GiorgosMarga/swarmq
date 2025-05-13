package messages

const (
	CreateTopic = iota
	Pub
	Join
	Sub
)

type SubMessage struct {
	TopicKey    string
	PartitionId int
}

type PubMessage struct {
	TopicKey    string
	PartitionId int
	Key         string
	Val         any
}

type CreateTopicMessage struct {
	TopicKey        string
	NumOfPartitions int
}
