package com.bettercloud.kadmin.api.services;

import java.util.Optional;

/**
 * Created by davidesposito on 7/25/16.
 */
public interface FeaturesService {

    boolean customUrlsEnabled();

    boolean producersEnabeled();

    String getCustomUrl(String url);
}
