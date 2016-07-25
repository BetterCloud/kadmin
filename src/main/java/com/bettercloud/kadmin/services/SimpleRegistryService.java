package com.bettercloud.kadmin.services;

import com.bettercloud.kadmin.api.models.Model;
import com.bettercloud.kadmin.api.models.SerializerInfoModel;
import com.bettercloud.kadmin.api.services.RegistryService;
import com.bettercloud.kadmin.api.services.SerializerRegistryService;
import com.bettercloud.util.Page;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/21/16.
 */
public class SimpleRegistryService<ModelT extends Model> implements RegistryService<ModelT> {

    private final Map<String, ModelT> infoMap = Collections.synchronizedMap(Maps.newLinkedHashMap());

    @Override
    public void register(ModelT model) {
        if (model != null && model.getId() != null) {
            infoMap.put(model.getId(), model);
        }
    }

    @Override
    public Page<ModelT> findAll() {
        Page<ModelT> page = new Page<>();
        List<ModelT> content = infoMap.values().stream().collect(Collectors.toList());
        page.setContent(content);
        page.setPage(0);
        page.setSize(content.size());
        page.setTotalElements(content.size());
        return page;
    }

    @Override
    public ModelT findById(String id) {
        return infoMap.get(id);
    }

    @Override
    public ModelT remove(String id) {
        return infoMap.remove(id);
    }
}
