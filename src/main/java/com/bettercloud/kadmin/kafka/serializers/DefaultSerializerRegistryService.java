package com.bettercloud.kadmin.kafka.serializers;

import com.bettercloud.kadmin.api.models.SerializerInfoModel;
import com.bettercloud.kadmin.api.services.SerializerRegistryService;
import com.bettercloud.util.Page;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/21/16.
 */
public class DefaultSerializerRegistryService implements SerializerRegistryService {

    private final Map<String, SerializerInfoModel> infoMap = Maps.newConcurrentMap();

    @Override
    public void register(SerializerInfoModel model) {
        if (model != null && model.getId() != null) {
            infoMap.put(model.getId(), model);
        }
    }

    @Override
    public Page<SerializerInfoModel> findAll() {
        Page<SerializerInfoModel> page = new Page<>();
        List<SerializerInfoModel> content = infoMap.values().stream().collect(Collectors.toList());
        page.setContent(content);
        page.setPage(0);
        page.setSize(content.size());
        page.setTotalElements(content.size());
        return page;
    }

    @Override
    public SerializerInfoModel findById(String id) {
        return infoMap.get(id);
    }

    @Override
    public SerializerInfoModel remove(String id) {
        return infoMap.remove(id);
    }
}
