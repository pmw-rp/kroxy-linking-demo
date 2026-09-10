# topic-prefixer

A custom [Kroxylicious](https://kroxylicious.io/) filter, purpose-built for this demo.

## Why a custom filter?

Kroxylicious ships a `MultiTenant` filter that adds a name prefix to topics, but it's designed for
multi-tenancy: it adds the prefix on the way *into* the backing cluster and strips it on the way
*out* to any client, so every client of the virtual cluster only ever sees the unprefixed name -
the prefix is purely an implementation detail of how the backing cluster stores the data. It also
hides any topic that isn't already prefixed, which would make `MultiTenant` show nothing at all for
topics created directly on the real source cluster (as this demo's other scripts do).

This demo wants the opposite: the real topic (and, as of the `consumer-group-renaming` work, the
real consumer group) on the source cluster keeps its real name, but *every* client of the proxy -
including Redpanda's Shadow Link - sees it renamed. That makes the rename visible end-to-end, all
the way through to the shadow cluster, which is the point of testing Kroxylicious here.
`TopicPrefixerFilter` does exactly that: it renames on responses flowing from the backend towards a
client, and un-renames on requests flowing the other way. Topics starting with `_` (Redpanda's
`_schemas` topic, Kafka's own `__consumer_offsets`, etc.) are left alone when the rename is a prefix
(see [Configuration](#configuration)), so Schema Registry replication keeps working unmodified.

It implements the RPCs Shadow Link needs to discover and replicate topics - `Metadata`,
`DescribeConfigs` (Shadow Link reads a topic's config before creating the shadow-side topic - miss
this one and Source Topic Sync silently gives up on every non-internal topic, since it looks up the
config under the prefixed name Metadata just gave it, which doesn't exist on the real backend),
`ListOffsets` and `Fetch` - plus the RPCs needed to discover and replicate consumer group offset
commits: `FindCoordinator`, `ListGroups`, `DescribeGroups`, `ConsumerGroupDescribe`, `JoinGroup`,
`SyncGroup`, `Heartbeat`, `LeaveGroup`, `OffsetFetch`, `OffsetCommit` and `OffsetDelete` - both the
group id and any topic names embedded in a commit get the same treatment as the data-plane RPCs
above. See [CONSUMER_GROUP_RENAMING.md](CONSUMER_GROUP_RENAMING.md) for why that needed checking
empirically rather than just implementing it, and for the gaps it deliberately doesn't cover.

It's demo-quality code, not a general-purpose substitute for `MultiTenant` - extend
`TopicPrefixerFilter` further if your own testing finds gaps (e.g. producing through the proxy would
also need `CreateTopicsRequestFilter`/`ProduceRequestFilter` and their response counterparts).

## Configuration

Topics and consumer groups are renamed independently, each via its own `RenamingConfig` - either a
fixed `prefix` (added/stripped, and skipped for names starting with `_`), or an explicit before/after
`mapping` (any name not in the table passes through unchanged - this filter never hides a name, only
ever renames the ones you've told it to). Exactly one of `prefix`/`mapping` must be given per target.

```yaml
config:
  topics:
    prefix: "p_"
  groups:
    prefix: "p_"          # can be a different prefix, or use `mapping` instead - independent of topics
```

```yaml
config:
  topics:
    mapping:
      orders: renamed-orders
      payments: renamed-payments
  groups:
    mapping:
      billing-service: renamed-billing-service
```

Strategies can be mixed (e.g. a prefix for topics, an explicit mapping for groups). An empty
`prefix` is a valid way to say "don't rename this one" while still renaming the other.

## How it's built and deployed

`setup.sh` builds this module into a Kroxylicious proxy image (see `Dockerfile`) via a multi-stage
build - a Maven stage compiles the filter, then it's copied onto
`quay.io/kroxylicious/proxy:0.24.0`'s `classpath-plugins` directory, Kroxylicious's (alpha) extension
point for adding filters to the classpath without rebuilding the base image. The image is loaded
directly into the `kind` cluster with `kind load docker-image`, and the Kroxylicious operator is
told to use it instead of the stock image via the `KROXYLICIOUS_IMAGE` env var on its Deployment
(see `resources/kroxylicious-operator.yaml`).

`resources/kroxylicious-proxy.yaml` wires up a `KafkaProtocolFilter` referencing this filter by its
fully-qualified class name (`demo.topicprefixer.TopicPrefixer`) with the config described above, and
attaches it to a `VirtualKafkaCluster` that targets the source Redpanda cluster.

## Building/testing standalone

```shell
docker run --rm -v "$PWD":/build -w /build maven:3.9-eclipse-temurin-21 mvn package
```

Runs the unit tests (using Kroxylicious's `MockFilterContext` test support) and produces
`target/topic-prefixer.jar`.
