# Redpanda Shadow Linking Demo

This demo shows Redpanda's native **shadow linking** feature — a built-in disaster recovery mechanism that continuously replicates topics, consumer group offsets, and Schema Registry data from a source cluster to a shadow cluster running on Kubernetes.

It also uses [Kroxylicious](https://kroxylicious.io/) to test shadow linking through a Kafka
protocol proxy: a Kroxylicious virtual cluster sits in front of the source cluster and, via a small
custom filter, presents every topic under a `p_` prefix. The ShadowLink reads from that proxy
instead of talking to the source cluster directly, so a healthy shadow — with `p_`-prefixed topics
— is proof that shadowing keeps working end-to-end through the proxy. See
[What Gets Deployed](#what-gets-deployed) and [Testing Through the Kroxylicious Proxy](#testing-through-the-kroxylicious-proxy) below,
and [kroxylicious/topic-prefixer/README.md](kroxylicious/topic-prefixer/README.md) for why this needs a custom filter.

## Prerequisites

- **Kubernetes cluster** with sufficient resources to run two Redpanda single-node clusters
- **kubectl** configured to access your cluster
- **Helm** (v3+) for installing cert-manager and the Redpanda operator
- **Docker** for building the custom Kroxylicious proxy image and (via `kind`) running the cluster
- **rpk** (Redpanda CLI) for interacting with the clusters and running demos

## What Gets Deployed

`setup.sh` installs the following into your Kubernetes cluster:

| Component            | Namespace              | Purpose |
|-----------------------|-------------------------|---|
| cert-manager          | `cert-manager`          | TLS certificate management (required by Redpanda operator) |
| Redpanda Operator     | `rp-operator`           | Kubernetes operator for managing Redpanda clusters |
| Kroxylicious Operator | `kroxylicious-operator` | Kubernetes operator for managing Kroxylicious proxies |
| Source cluster        | `source`                | The primary cluster that data is written to |
| Shadow cluster        | `shadow`                | The shadow cluster that replicates from source |
| Kroxylicious proxy    | `kroxylicious`          | Proxy virtual cluster fronting the source cluster, prefixing topic names with `p_` |
| ShadowLink resource   | `shadow`                | Defines the replication relationship between clusters, reading from the Kroxylicious proxy |

Port forwards are created so you can reach both clusters from localhost:

| Cluster | Kafka | Admin API | Schema Registry |
|---------|---|---|---|
| Source  | `localhost:19094` | `localhost:19644` | `localhost:18081` |
| Shadow  | `localhost:29094` | `localhost:29644` | `localhost:28081` |

Two `rpk` profiles are also created — `source` and `shadow` — so demo scripts can address each cluster independently.

## Setup

```zsh
./setup.sh
```

The script installs cert-manager, the Redpanda operator and the Kroxylicious operator via Helm/manifests, builds the custom Kroxylicious proxy image and loads it into the `kind` cluster, deploys both Redpanda clusters and the Kroxylicious proxy, waits for everything to be ready, applies the ShadowLink resource, creates port forwards, and configures `rpk` profiles.

## Shadow Link Configuration

The ShadowLink (`resources/shadow-link.yaml`) is configured to replicate:

- **All topics** (wildcard filter `*`) with a 5-second sync interval, read from the source cluster through the Kroxylicious proxy virtual cluster rather than the source cluster directly
- **All consumer group offsets** with a 5-second sync interval
- **Schema Registry** via replication of the internal `_schemas` topic

## Testing Through the Kroxylicious Proxy

Because the ShadowLink's `sourceCluster` is the Kroxylicious proxy virtual cluster
(`resources/kroxylicious-proxy.yaml`), and that proxy's `topic-prefixer` filter presents every topic
**and consumer group** with a `p_` prefix, every topic and consumer group discovered and replicated
by the ShadowLink shows up on the shadow cluster with that prefix — even though the real topic/group
on the source cluster is never renamed. This is visible throughout the demos below: e.g. a topic
created as `basic` on the source cluster appears as `p_basic` on the shadow cluster, and a consumer
group created as `consumer-group-foo` appears as `p_consumer-group-foo`.

You can see both sides of this at once:

```shell
# The real name, straight from the source cluster (bypasses the proxy):
rpk --profile source topic list

# The name as replicated to the shadow cluster, via the proxy:
rpk --profile shadow topic list
```

See [kroxylicious/topic-prefixer/README.md](kroxylicious/topic-prefixer/README.md) for why a custom
filter is needed for this rather than Kroxylicious's built-in `MultiTenant` filter, and
[kroxylicious/topic-prefixer/CONSUMER_GROUP_RENAMING.md](kroxylicious/topic-prefixer/CONSUMER_GROUP_RENAMING.md)
for how (and how safely) the same renaming extends to consumer groups.

## Demos

Each demo is in its own directory under `demos/` and consists of numbered scripts to run in order.

### Demo 1 — Basic Topic Shadowing (`demos/1-basic-shadowing/`)

Demonstrates that topics and messages written to the source cluster automatically appear on the shadow cluster.

1. `1-create-topic-on-source.sh` — creates topic `basic` on source
2. `2-produce-record-to-source.sh` — produces "Hello, world" to source
3. `3-list-topics-on-shadow.sh` — shows the topic has replicated to the shadow cluster as `p_basic` (see [Testing Through the Kroxylicious Proxy](#testing-through-the-kroxylicious-proxy))
4. `4-consume-messages-on-both.sh` — consumes from both clusters to confirm the message is present on both

### Demo 2 — Schema Registry Shadowing (`demos/2-schema-shadowing/`)

Demonstrates that Avro schemas registered on the source cluster replicate to the shadow, enabling consumers on the shadow to deserialize messages using the same schema.

1. `1-register-schema.sh` — registers an Avro syslog schema on the source Schema Registry
2. `2-create-topic.sh` — creates topic `syslog` on source
3. `3-produce-records.sh` — produces ~3000 syslog records (rate-limited to 10/s)
4. `4-list-schemas-on-both.sh` — shows the schema is present on both clusters' Schema Registries
5. `5-consume-raw-messages-from-shadow.sh` — shows raw Avro-encoded bytes (with schema ID prefix) from the shadow
6. `6-consume-decoded-messages-from-shadow.sh` — consumes and decodes messages using the replicated schema

### Demo 3 — Consumer Group Failover (`demos/3-failover/`)

Demonstrates disaster recovery: consumer group offsets replicate from source to shadow, so after a failover clients can resume on the shadow cluster from exactly where they left off.

1. `1-create-topic-on-source.sh` — creates topic `foo` on source
2. `2-produce-and-consume-on-source.sh` — produces records 1–5 and consumes them with `consumer-group-foo`
3. `3-show-consumer-groups.sh` — shows consumer group offset state on source, and on shadow as `p_consumer-group-foo`
4. `4-failover.sh` — executes `rpk shadow failover disaster-recovery-link --all --no-confirm`
5. `5-produce-and-consume-on-shadow.sh` — produces records 6–10 to shadow and consumes with the same (now `p_`-prefixed) consumer group
6. `6-show-consumer-group-on-shadow.sh` — confirms consumer group offsets have been migrated and are current
