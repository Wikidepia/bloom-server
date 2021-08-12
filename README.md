# Bloom Filter Server

## Setup

1. Install [Go](https://golang.org/doc/install)
2. Install Redis & [RedisBloom](https://oss.redis.com/redisbloom/Quick_Start/)
3. Clone this repository
4. `go build`, to build binary
5. `./main`, to run bloom-server

## Endpoints

1. `/deduplicate/`, to remove duplicate from list of urls. This will return list of unique urls

```bash
curl -s -v -F "file=@urls.txt" localhost:8000/deduplicate/
```

2. `/add/`, to add urls to bloom filter from list of urls. This will return 200OK if successfully added to bloom filter

```bash
curl -s -v -F "file=@urls.txt" localhost:8000/add/
```

3. `/info/`, to check information about duplicate count & bloom filter
