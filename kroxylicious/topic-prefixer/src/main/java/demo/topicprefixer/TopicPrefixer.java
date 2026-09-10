package demo.topicprefixer;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;

/**
 * Filter factory for {@link TopicPrefixerFilter}.
 * <p>
 * This is a one-way, demo-only counterpart to Kroxylicious's built-in {@code MultiTenant} filter.
 * {@code MultiTenant} adds a prefix on the way IN (client -&gt; backend) and strips it on the way OUT
 * (backend -&gt; client), so any client of the virtual cluster only ever sees the unprefixed name -
 * the prefix is purely an implementation detail of how the backend cluster stores the data.
 * <p>
 * This filter does the opposite: it adds the prefix on the way OUT (backend -&gt; client) and strips
 * it on the way IN (client -&gt; backend), so the prefix is entirely cosmetic and is visible to every
 * client of the virtual cluster - including Redpanda's Shadow Link, which is the point of this demo.
 * The real topic on the backing cluster is never renamed.
 */
@Plugin(configType = TopicPrefixerConfig.class)
public class TopicPrefixer implements FilterFactory<TopicPrefixerConfig, TopicPrefixerConfig> {

    @Override
    public TopicPrefixerConfig initialize(FilterFactoryContext context, TopicPrefixerConfig config) throws PluginConfigurationException {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public TopicPrefixerFilter createFilter(FilterFactoryContext context, TopicPrefixerConfig configuration) {
        return new TopicPrefixerFilter(configuration.prefix());
    }
}
