package demo.topicprefixer;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for the {@link TopicPrefixer} filter.
 */
public class TopicPrefixerConfig {

    private final String prefix;

    public TopicPrefixerConfig(@JsonProperty(required = true) String prefix) {
        this.prefix = prefix;
    }

    public String prefix() {
        return prefix;
    }
}
