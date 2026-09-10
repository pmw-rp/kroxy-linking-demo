package demo.topicprefixer;

import java.time.Duration;
import java.util.List;

import io.kroxylicious.kafka.common.message.DescribeConfigsRequestData;
import io.kroxylicious.kafka.common.message.DescribeConfigsResponseData;
import io.kroxylicious.kafka.common.message.FetchRequestData;
import io.kroxylicious.kafka.common.message.FetchResponseData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.testing.filter.assertj.MockFilterContextAssert;
import io.kroxylicious.testing.filter.context.MockFilterContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopicPrefixerFilterTest {

    private static final String PREFIX = "p_";
    private static final short METADATA_VERSION = 12;
    private static final short FETCH_VERSION = 15;
    private static final short DESCRIBE_CONFIGS_VERSION = 4;

    private TopicPrefixerFilter filter;

    @BeforeEach
    void beforeEach() {
        filter = new TopicPrefixerFilter(PREFIX);
    }

    @Test
    void addsPrefixToOrdinaryTopicInMetadataResponse() {
        var header = new ResponseHeaderData();
        var response = new MetadataResponseData();
        var topic = new MetadataResponseData.MetadataResponseTopic();
        topic.setName("foo");
        response.setTopics(new MetadataResponseData.MetadataResponseTopicCollection(List.of(topic).iterator()));
        var context = MockFilterContext.builder(header, response).build();

        var stage = filter.onMetadataResponse(METADATA_VERSION, header, response, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> MockFilterContextAssert.assertThat(result)
                .isForwardResponse()
                .hasMessageInstanceOfSatisfying(MetadataResponseData.class,
                        forwarded -> assertThat(forwarded.topics().iterator().next().name()).isEqualTo("p_foo")));
    }

    @Test
    void leavesInternalTopicUnprefixedInMetadataResponse() {
        var header = new ResponseHeaderData();
        var response = new MetadataResponseData();
        var topic = new MetadataResponseData.MetadataResponseTopic();
        topic.setName("_schemas");
        response.setTopics(new MetadataResponseData.MetadataResponseTopicCollection(List.of(topic).iterator()));
        var context = MockFilterContext.builder(header, response).build();

        var stage = filter.onMetadataResponse(METADATA_VERSION, header, response, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> MockFilterContextAssert.assertThat(result)
                .isForwardResponse()
                .hasMessageInstanceOfSatisfying(MetadataResponseData.class,
                        forwarded -> assertThat(forwarded.topics().iterator().next().name()).isEqualTo("_schemas")));
    }

    @Test
    void stripsPrefixFromNamedTopicInMetadataRequest() {
        var header = new RequestHeaderData();
        var request = new MetadataRequestData();
        var topic = new MetadataRequestData.MetadataRequestTopic();
        topic.setName("p_foo");
        request.setTopics(List.of(topic));
        var context = MockFilterContext.builder(header, request).build();

        var stage = filter.onMetadataRequest(METADATA_VERSION, header, request, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> MockFilterContextAssert.assertThat(result)
                .isForwardRequest()
                .hasMessageInstanceOfSatisfying(MetadataRequestData.class,
                        forwarded -> assertThat(forwarded.topics().iterator().next().name()).isEqualTo("foo")));
    }

    @Test
    void leavesWildcardMetadataRequestUntouched() {
        var header = new RequestHeaderData();
        var request = new MetadataRequestData();
        request.setTopics(null);
        var context = MockFilterContext.builder(header, request).build();

        var stage = filter.onMetadataRequest(METADATA_VERSION, header, request, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> MockFilterContextAssert.assertThat(result)
                .isForwardRequest()
                .hasMessageInstanceOfSatisfying(MetadataRequestData.class, forwarded -> assertThat(forwarded.topics()).isNull()));
    }

    @Test
    void stripsPrefixFromFetchRequestTopic() {
        var header = new RequestHeaderData();
        var request = new FetchRequestData();
        var topic = new FetchRequestData.FetchTopic();
        topic.setTopic("p_foo");
        request.setTopics(List.of(topic));
        var context = MockFilterContext.builder(header, request).build();

        var stage = filter.onFetchRequest(FETCH_VERSION, header, request, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> MockFilterContextAssert.assertThat(result)
                .isForwardRequest()
                .hasMessageInstanceOfSatisfying(FetchRequestData.class,
                        forwarded -> assertThat(forwarded.topics().get(0).topic()).isEqualTo("foo")));
    }

    @Test
    void addsPrefixToFetchResponseTopic() {
        var header = new ResponseHeaderData();
        var response = new FetchResponseData();
        var topic = new FetchResponseData.FetchableTopicResponse();
        topic.setTopic("foo");
        response.setResponses(List.of(topic));
        var context = MockFilterContext.builder(header, response).build();

        var stage = filter.onFetchResponse(FETCH_VERSION, header, response, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> MockFilterContextAssert.assertThat(result)
                .isForwardResponse()
                .hasMessageInstanceOfSatisfying(FetchResponseData.class,
                        forwarded -> assertThat(forwarded.responses().get(0).topic()).isEqualTo("p_foo")));
    }

    @Test
    void stripsPrefixFromDescribeConfigsTopicResource() {
        var header = new RequestHeaderData();
        var request = new DescribeConfigsRequestData();
        var resource = new DescribeConfigsRequestData.DescribeConfigsResource();
        resource.setResourceType((byte) 2);
        resource.setResourceName("p_foo");
        request.setResources(List.of(resource));
        var context = MockFilterContext.builder(header, request).build();

        var stage = filter.onDescribeConfigsRequest(DESCRIBE_CONFIGS_VERSION, header, request, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> MockFilterContextAssert.assertThat(result)
                .isForwardRequest()
                .hasMessageInstanceOfSatisfying(DescribeConfigsRequestData.class,
                        forwarded -> assertThat(forwarded.resources().get(0).resourceName()).isEqualTo("foo")));
    }

    @Test
    void leavesNonTopicResourceUntouchedInDescribeConfigsRequest() {
        var header = new RequestHeaderData();
        var request = new DescribeConfigsRequestData();
        var resource = new DescribeConfigsRequestData.DescribeConfigsResource();
        resource.setResourceType((byte) 4); // BROKER, not TOPIC
        resource.setResourceName("0");
        request.setResources(List.of(resource));
        var context = MockFilterContext.builder(header, request).build();

        var stage = filter.onDescribeConfigsRequest(DESCRIBE_CONFIGS_VERSION, header, request, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> MockFilterContextAssert.assertThat(result)
                .isForwardRequest()
                .hasMessageInstanceOfSatisfying(DescribeConfigsRequestData.class,
                        forwarded -> assertThat(forwarded.resources().get(0).resourceName()).isEqualTo("0")));
    }

    @Test
    void addsPrefixToDescribeConfigsTopicResult() {
        var header = new ResponseHeaderData();
        var response = new DescribeConfigsResponseData();
        var result = new DescribeConfigsResponseData.DescribeConfigsResult();
        result.setResourceType((byte) 2);
        result.setResourceName("foo");
        response.setResults(List.of(result));
        var context = MockFilterContext.builder(header, response).build();

        var stage = filter.onDescribeConfigsResponse(DESCRIBE_CONFIGS_VERSION, header, response, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(r -> MockFilterContextAssert.assertThat(r)
                .isForwardResponse()
                .hasMessageInstanceOfSatisfying(DescribeConfigsResponseData.class,
                        forwarded -> assertThat(forwarded.results().get(0).resourceName()).isEqualTo("p_foo")));
    }
}
