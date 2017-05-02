usage()
{
    echo "usage: $0 {Producer|Consumer}"
}

if [[ "$1" != "Producer" && "$1" != "Consumer" ]]; then
    usage
    exit 0
fi

java -Djava.ext.dirs=./target kafkatest.$@
