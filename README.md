# kafka-transport

## Install

```
$ pip install kafka-transport
```

## Use

Package contain `init`, `push`, `subscribe`, `fetch` async functions:

```
async init(kafka_host)
async push(topic, value, key)
async subscribe(topic, callback)
async fetch(to, _from, value)
```
