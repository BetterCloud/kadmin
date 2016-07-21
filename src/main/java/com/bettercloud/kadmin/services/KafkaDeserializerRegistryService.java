package com.bettercloud.kadmin.services;

import com.bettercloud.kadmin.api.models.DeserializerInfoModel;
import com.bettercloud.kadmin.api.models.SerializerInfoModel;
import com.bettercloud.kadmin.api.services.DeserializerRegistryService;
import com.bettercloud.kadmin.api.services.SerializerRegistryService;

/**
 * Created by davidesposito on 7/21/16.
 */
public class KafkaDeserializerRegistryService extends SimpleRegistryService<DeserializerInfoModel> implements DeserializerRegistryService {
}
