# Research: renaming consumer group commits through Kroxylicious

**Question:** can Kroxylicious rename the topics referenced by consumer group offset commits (the
same way it renames topics in the data plane), and can it also rename the consumer group itself?

**Answer: yes to both**, using exactly the same technique already used for topics in
`TopicPrefixerFilter` - Kroxylicious's filter API exposes every relevant field on every relevant
RPC as a plain, mutable string, so a demo-quality prefixer generalises to consumer groups with no
new mechanism, just more RPCs. Both are now implemented on this branch and validated end-to-end
against a live Shadow Link, on top of the working setup described in the main README.

## Why this needed checking at all

The topic-only version of this filter (`main` branch) already produced a *correct* demo: Shadow
Link replicated consumer group offsets from source to shadow with the right topic, the right
offset, and the right lag, even though the filter never touched a single consumer-group RPC. That
was surprising enough to be worth explaining before answering "can we go further" - if Shadow Link
already gets this right without help, does renaming groups risk breaking something that currently
works by accident?

Wire-level tracing (Kroxylicious's `ProtocolLogger` filter, attached temporarily) answered this
empirically rather than by guesswork:

- Consumer Group Shadowing reads **through the proxy** using `LIST_GROUPS`, `FIND_COORDINATOR` and
  `OFFSET_FETCH` - all plain Kafka protocol, no admin API involved.
- Before this branch, the topic-only filter didn't touch any of these, so `OFFSET_FETCH` was
  observed carrying the **real, unprefixed** topic name (`"cgtest"`, not `"p_cgtest"`) back to
  Shadow Link.
- Despite that, the shadow cluster ended up with the offset committed against the correctly
  *shadowed* topic (`p_cgtest`), with accurate lag. Shadow Link must be correlating source and
  shadow topics by topic id internally (topic ids survive the rename; names don't), not by trusting
  the name string it read - so the existing mismatch was cosmetic, not functional.

That finding set the bar for this change: it had to be more than "technically possible", it had to
not regress a path that already worked despite the filter's ignorance of it.

## What's implemented

`TopicPrefixerFilter` (same class as the topic-only version) now also implements:

> **Since this was written:** topic renaming and group renaming were originally the same `prefix`,
> shared via `addPrefix`/`stripPrefix` helpers on the filter itself. They're now independently
> configurable - a `NameRenamer` per target, each either a prefix or an explicit mapping - see
> [README.md#configuration](README.md#configuration). Nothing below changes as a result: whichever
> renaming strategy is chosen for groups is applied through exactly the same RPCs this document
> describes.

| RPC | What's renamed |
|---|---|
| `FindCoordinator` | the group id being looked up - **only** when `keyType == GROUP`; transaction id lookups are left completely untouched (see [Scope](#scope-and-known-gaps)) |
| `ListGroups` | every group id in the response (no filtering - unlike `MultiTenant`, nothing is hidden) |
| `DescribeGroups`, `ConsumerGroupDescribe` | the group id(s) |
| `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup` | the group id (request only - these RPCs don't echo it back) |
| `OffsetFetch`, `OffsetCommit`, `OffsetDelete` | the group id **and** the topic name(s) inside the commit, using the exact same `addPrefix`/`stripPrefix` as `Metadata`/`Fetch`/`ListOffsets`/`DescribeConfigs` - so a topic always gets the same name whether you're looking at it via the data plane or via a consumer group's committed offsets |

`OffsetFetch` in particular has two request/response shapes depending on API version (a deprecated
singular `groupId`/`topics` pair, and a newer `groups` list each with their own `topics`) - both are
rewritten, mirroring how `MultiTenantFilter` (Kroxylicious's own multi-tenancy filter) handles the
same duality.

### The one non-obvious implementation detail: `FindCoordinator`

`FindCoordinator` is used for both consumer group coordinators and transaction coordinators
(`keyType 0` vs `1`), but its *response* doesn't restate which kind a given entry is - only the
request says. Renaming the response unconditionally would incorrectly rename a transactional id
that was never renamed on the way in (this filter doesn't implement the transactional producer
lifecycle at all). The fix is a small in-flight correlation-id set, populated in
`onFindCoordinatorRequest` only for `keyType == GROUP` and consulted (and cleared) in
`onFindCoordinatorResponse` - safe as unsynchronized mutable state because a filter instance is
scoped to one connection and processes that connection's messages sequentially (the same assumption
`MultiTenantFilter` makes with its own lazily-cached tenant prefix field). Covered by
`findCoordinatorRoundTripRenamesGroupLookup` / `findCoordinatorLeavesTransactionLookupUntouched` in
the test suite.

## Empirical validation

Rebuilt the proxy image, reloaded it into `kind`, and re-ran the full source→shadow flow on a live
cluster (not just unit tests):

- A consumer group created directly on the source cluster (`consumer-group-foo`, via the existing
  demo scripts, bypassing the proxy - matching how every other demo topic is created) shows up on
  the shadow cluster as **`p_consumer-group-foo`**, tracking the correctly-shadowed **`p_foo`**
  topic, with correct offsets and lag throughout - both before and after `demos/3-failover`'s
  failover step (`demos/3-failover/3-show-consumer-groups.sh` and
  `6-show-consumer-group-on-shadow.sh`/`5-produce-and-consume-on-shadow.sh` were updated to expect
  the `p_`-prefixed group name; see the diff on this branch).
- The topic data plane (`demos/1-basic-shadowing`) and Schema Registry replication (`_schemas`
  staying unprefixed) were re-checked afterwards and are unaffected - this change is additive, it
  doesn't touch any of the existing Metadata/Fetch/ListOffsets/DescribeConfigs handling.

## Scope and known gaps

Deliberately out of scope, consistent with the topic-only filter's own documented gaps:

- **Transactional producers.** `InitProducerId`, `AddOffsetsToTxn`, `AddPartitionsToTxn`,
  `TxnOffsetCommit`, `EndTxn` aren't implemented, so a transactional id is never renamed - and
  `FindCoordinator` deliberately leaves transaction-coordinator lookups alone to stay consistent
  with that (see above).
- **`DeleteGroups`.** Not implemented - `MultiTenantFilter` itself doesn't implement it either, so
  this isn't a new gap relative to Kroxylicious's own reference filter.
- **The newer KIP-848 consumer group protocol** (`ConsumerGroupHeartbeat`-based joining, as opposed
  to classic `JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`). `MultiTenantFilter` doesn't cover
  this either. Redpanda Shadow Link's own client uses the classic protocol (confirmed by the wire
  trace above), so this doesn't affect the shadowing demo, but a client that prefers KIP-848 for its
  *own* group membership through this proxy would see its group id go unrenamed. Unlike
  `DescribeTopicPartitions` (which this filter suppresses via `ApiVersions` to force clients back
  onto plain `Metadata`), there's no `ApiVersions`-suppression fix on the table for this one without
  also implementing the transactional lifecycle above - `ConsumerGroupHeartbeat` is bundled into the
  same protocol generation as some transaction features clients may depend on. Documented as a gap
  rather than worked around.

## Recommendation

Merge as-is if you want consumer group ids visibly renamed end-to-end (the same "does the rename
survive the whole round trip" proof the topic prefix already gives you) - it's a real feature, not
just a research exercise, and it's validated on a live cluster, not just in unit tests. If you'd
rather keep group ids untouched (e.g. because `p_`-prefixed group ids in monitoring/alerting on the
shadow cluster is more confusing than helpful for your use case), keep `main`'s topic-only filter -
the finding above still stands there: Shadow Link's own topic-id-based correlation means group
*offsets* replicate correctly regardless of whether this filter renames anything at the consumer
group layer.
