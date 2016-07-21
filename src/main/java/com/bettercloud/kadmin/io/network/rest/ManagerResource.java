package com.bettercloud.kadmin.io.network.rest;

import com.bettercloud.kadmin.api.models.DeserializerInfoModel;
import com.bettercloud.kadmin.api.models.SerializerInfoModel;
import com.bettercloud.kadmin.api.services.DeserializerRegistryService;
import com.bettercloud.kadmin.api.services.SerializerRegistryService;
import com.bettercloud.util.Page;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by davidesposito on 7/21/16.
 */
@RestController
@RequestMapping("/api/manager")
public class ManagerResource {

    private final SerializerRegistryService serializerRegistryService;
    private final DeserializerRegistryService deserializerRegistryService;

    @Autowired
    public ManagerResource(SerializerRegistryService serializerRegistryService,
                           DeserializerRegistryService deserializerRegistryService) {
        this.serializerRegistryService = serializerRegistryService;
        this.deserializerRegistryService = deserializerRegistryService;
    }

    @RequestMapping(
            path = "/serializers",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Page<SerializerInfoModel>> serializers() {
        return ResponseEntity.ok(serializerRegistryService.findAll());
    }

    @RequestMapping(
            path = "/deserializers",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Page<DeserializerInfoModel>> deserializers() {
        return ResponseEntity.ok(deserializerRegistryService.findAll());
    }
}
