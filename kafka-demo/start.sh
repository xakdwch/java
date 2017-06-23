usage()
{
    echo "usage: $0 {Producer|Consumer|TopicMetadata|TopicOffsets|OffsetCommit}"
}

if [[ "$1" != "Producer" && "$1" != "Consumer" && "$1" != "TopicMetadata" && 
      "$1" != "TopicOffsets" && "$1" != "OffsetCommit" ]]; then
    usage
    exit 0
fi

java -Djava.ext.dirs=./target kafkatest.$@
