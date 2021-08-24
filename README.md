## Endpoints

1. `/deduplicate/`, to remove duplicate from list of data. This will return list of unique data

```bash
curl -s -v -F "file=@urls.txt" -F "key=clipped" localhost:8000/deduplicate/
```

2. `/add/`, to add urls to bloom filter from list of data. This will return 200OK if successfully added to bloom filter

```bash
curl -s -v -F "file=@urls.txt" -F "key=clipped" localhost:8000/add/
```

3. `/info/`, to check information about duplicate count & bloom filter