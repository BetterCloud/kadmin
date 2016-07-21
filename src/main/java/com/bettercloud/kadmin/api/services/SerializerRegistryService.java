package com.bettercloud.kadmin.api.services;

import com.bettercloud.kadmin.api.models.SerializerInfoModel;
import com.bettercloud.util.Page;

/**
 * Created by davidesposito on 7/21/16.
 */
public interface SerializerRegistryService {

    void register(SerializerInfoModel model);

    Page<SerializerInfoModel> findAll();

    SerializerInfoModel findById(String id);

    SerializerInfoModel remove(String id);
}
