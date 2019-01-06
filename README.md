# ngt-cluster

**ngt-cluster** is an experimental implementation of the NGT server that supports multi-node clusters.

## HTTP API

### Insert

```
POST /insert  HTTP/1.1
...

{
  "vector": [...]
}
```

### Delete

```
POST /remove HTTP/1.1
...

{
  "id": 1
}
```

### Seaech

```
POST /search HTTP/1.1

{
  "vector": [...],
  "results": 10,
  "epsilon": 0.1
}
```
