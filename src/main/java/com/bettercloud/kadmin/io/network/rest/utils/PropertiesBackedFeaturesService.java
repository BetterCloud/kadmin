package com.bettercloud.kadmin.io.network.rest.utils;

import com.bettercloud.kadmin.api.services.FeaturesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Created by davidesposito on 7/25/16.
 */
@Service
public class PropertiesBackedFeaturesService implements FeaturesService {

    private final Environment env;

    @Autowired
    public PropertiesBackedFeaturesService(Environment env) {
        this.env = env;
    }

    @Override
    public boolean customUrlsEnabled() {
        return env.getProperty("ff.customKafkaUrl.enabled", Boolean.class, false);
    }

    @Override
    public boolean producersEnabeled() {
        return env.getProperty("ff.producer.enabled", Boolean.class, false);
    }

    @Override
    public String getCustomUrl(String url) {
        return Optional.of(url)
                .filter(u -> customUrlsEnabled())
                .orElse(null);
    }
}
