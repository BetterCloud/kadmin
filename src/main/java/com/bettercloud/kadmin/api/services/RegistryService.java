package com.bettercloud.kadmin.api.services;

import com.bettercloud.kadmin.api.models.DeserializerInfoModel;
import com.bettercloud.util.Page;

/**
 * Created by davidesposito on 7/21/16.
 */
public interface RegistryService<ModelT> {

    void register(ModelT model);

    Page<ModelT> findAll();

    ModelT findById(String id);

    ModelT remove(String id);
}
