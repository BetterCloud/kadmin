package com.bettercloud.kadmin.services;

import com.bettercloud.kadmin.api.models.Model;
import com.bettercloud.kadmin.api.models.SerializerInfoModel;
import com.bettercloud.kadmin.api.services.RegistryService;
import com.bettercloud.kadmin.api.services.SerializerRegistryService;
import com.bettercloud.util.Page;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/21/16.
 */
public class KafkaSerializerRegistryService extends SimpleRegistryService<SerializerInfoModel> implements SerializerRegistryService {
}
