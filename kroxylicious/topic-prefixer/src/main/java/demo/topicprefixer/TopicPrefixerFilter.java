package demo.topicprefixer;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.DescribeConfigsRequestData;
import io.kroxylicious.kafka.common.message.DescribeConfigsResponseData;
import io.kroxylicious.kafka.common.message.FetchRequestData;
import io.kroxylicious.kafka.common.message.FetchResponseData;
import io.kroxylicious.kafka.common.message.ListOffsetsRequestData;
import io.kroxylicious.kafka.common.message.ListOffsetsResponseData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.transform.ApiVersionsResponseTransformer;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.DescribeConfigsRequestFilter;
import io.kroxylicious.proxy.filter.DescribeConfigsResponseFilter;
import io.kroxylicious.proxy.filter.FetchRequestFilter;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ListOffsetsRequestFilter;
import io.kroxylicious.proxy.filter.ListOffsetsResponseFilter;
import io.kroxylicious.proxy.filter.MetadataRequestFilter;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import static io.kroxylicious.kafka.transform.ApiVersionsResponseTransformers.removeApiKeys;

/**
 * Presents every topic on the backing cluster to clients of the virtual cluster under a cosmetic
 * {@code prefix}, without ever renaming the real topic. See {@link TopicPrefixer} for the rationale.
 * <p>
 * Topics whose real name starts with {@code _} (Redpanda's Schema Registry {@code _schemas} topic,
 * Kafka's own {@code __consumer_offsets}, etc.) are deliberately left untouched, so infrastructure
 * that relies on well-known topic names keeps working unmodified through the proxy.
 * <p>
 * Only the RPCs that Redpanda Shadow Link needs in order to discover and replicate topics are
 * covered here: {@code Metadata} (topic discovery), {@code DescribeConfigs} (reading the topic's
 * configuration before creating the corresponding shadow topic), {@code ListOffsets} (finding where
 * to start replicating from) and {@code Fetch} (pulling the records). This is demo-quality code, not
 * a general-purpose substitute for the {@code MultiTenant} filter - extend it if your own testing
 * finds gaps (e.g. if you also want to produce through the proxy, you'll need
 * {@code CreateTopicsRequestFilter}/{@code ProduceRequestFilter} and their response counterparts).
 * <p>
 * One RPC is actively suppressed rather than rewritten: {@code DescribeTopicPartitions} (KIP-966) is
 * a newer alternative to {@code Metadata} for topic discovery that this filter doesn't rewrite. Left
 * alone, a client that prefers it (as Redpanda Shadow Link's Kafka client does) would discover topics
 * by their real, unprefixed names, then get confused when the corresponding {@code Fetch} responses
 * come back under the prefixed name. {@code onApiVersionsResponse} removes it from the advertised API
 * list - the same trick Kroxylicious's own {@code MultiTenant} filter uses - forcing such clients back
 * onto {@code Metadata}, which this filter does rewrite.
 */
class TopicPrefixerFilter implements
        ApiVersionsResponseFilter,
        MetadataRequestFilter, MetadataResponseFilter,
        DescribeConfigsRequestFilter, DescribeConfigsResponseFilter,
        ListOffsetsRequestFilter, ListOffsetsResponseFilter,
        FetchRequestFilter, FetchResponseFilter {

    private static final ApiVersionsResponseTransformer API_VERSIONS_RESPONSE_INTERCEPTOR = removeApiKeys(Set.of(ApiKeys.DESCRIBE_TOPIC_PARTITIONS));

    /** Kafka protocol {@code ConfigResource.Type} for a topic resource (as opposed to e.g. a broker). */
    private static final byte RESOURCE_TYPE_TOPIC = 2;

    private final String prefix;

    TopicPrefixerFilter(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header, ApiVersionsResponseData response,
                                                                         FilterContext context) {
        return context.forwardResponse(header, API_VERSIONS_RESPONSE_INTERCEPTOR.transform(response));
    }

    private boolean isPrefixable(String topicName) {
        return topicName != null && !topicName.isEmpty() && !topicName.startsWith("_");
    }

    /** Applied to names flowing from the real backend towards a client: add the cosmetic prefix. */
    private String addPrefix(String topicName) {
        return isPrefixable(topicName) ? prefix + topicName : topicName;
    }

    /** Applied to names flowing from a client towards the real backend: strip the cosmetic prefix. */
    private String stripPrefix(String topicName) {
        if (isPrefixable(topicName) && topicName.startsWith(prefix)) {
            return topicName.substring(prefix.length());
        }
        return topicName;
    }

    @Override
    public CompletionStage<RequestFilterResult> onMetadataRequest(short apiVersion, RequestHeaderData header, MetadataRequestData request,
                                                                    FilterContext context) {
        if (request.topics() != null) {
            // request.topics() == null means "give me all the topics" - nothing to rewrite here.
            request.topics().forEach(topic -> topic.setName(stripPrefix(topic.name())));
        }
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onMetadataResponse(short apiVersion, ResponseHeaderData header, MetadataResponseData response,
                                                                      FilterContext context) {
        response.topics().forEach(topic -> topic.setName(addPrefix(topic.name())));
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<RequestFilterResult> onListOffsetsRequest(short apiVersion, RequestHeaderData header, ListOffsetsRequestData request,
                                                                       FilterContext context) {
        request.topics().forEach(topic -> topic.setName(stripPrefix(topic.name())));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onListOffsetsResponse(short apiVersion, ResponseHeaderData header, ListOffsetsResponseData response,
                                                                        FilterContext context) {
        response.topics().forEach(topic -> topic.setName(addPrefix(topic.name())));
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<RequestFilterResult> onDescribeConfigsRequest(short apiVersion, RequestHeaderData header, DescribeConfigsRequestData request,
                                                                          FilterContext context) {
        request.resources().stream()
                .filter(resource -> resource.resourceType() == RESOURCE_TYPE_TOPIC)
                .forEach(resource -> resource.setResourceName(stripPrefix(resource.resourceName())));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onDescribeConfigsResponse(short apiVersion, ResponseHeaderData header, DescribeConfigsResponseData response,
                                                                            FilterContext context) {
        response.results().stream()
                .filter(result -> result.resourceType() == RESOURCE_TYPE_TOPIC)
                .forEach(result -> result.setResourceName(addPrefix(result.resourceName())));
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<RequestFilterResult> onFetchRequest(short apiVersion, RequestHeaderData header, FetchRequestData request, FilterContext context) {
        request.topics().forEach(topic -> topic.setTopic(stripPrefix(topic.topic())));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onFetchResponse(short apiVersion, ResponseHeaderData header, FetchResponseData response, FilterContext context) {
        response.responses().forEach(topic -> topic.setTopic(addPrefix(topic.topic())));
        return context.forwardResponse(header, response);
    }
}
