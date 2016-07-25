package com.bettercloud.kadmin.io.ui;

import com.bettercloud.kadmin.api.models.DeserializerInfoModel;
import com.bettercloud.kadmin.api.services.DeserializerRegistryService;
import com.bettercloud.kadmin.api.services.FeaturesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Created by davidesposito on 7/15/16.
 */
@Controller
public class KadminUiController {

    private final Environment env;
    private final DeserializerRegistryService deserializerRegistryService;
    private final FeaturesService featuresService;

    @Autowired
    public KadminUiController(Environment env, DeserializerRegistryService deserializerRegistryService,
                              FeaturesService featuresService) {
        this.env = env;
        this.deserializerRegistryService = deserializerRegistryService;
        this.featuresService = featuresService;
    }

    @RequestMapping("/")
    public String home(Model model) {
        setEnvProps(model);
        return "index";
    }

    @RequestMapping("/consumer")
    public String consumer(Model model) {
        setEnvProps(model);
        return "consumer";
    }

    @RequestMapping("/consumer/topic/{topic}/{deserializerId}")
    public String consumer(Model model,
                           @PathVariable("topic") String topic,
                           @PathVariable("deserializerId") String deserializerId) {
        setEnvProps(model);
        DeserializerInfoModel info = deserializerRegistryService.findById(deserializerId);
        if (info != null) {
            model.addAttribute("deserializerId", deserializerId);
            model.addAttribute("deserializerName", info.getName());
        }
        model.addAttribute("defaultTopicName", topic);
        return "consumer";
    }

    @RequestMapping("/producer")
    public String producer(Model model) {
        setEnvProps(model);
        return featuresService.producersEnabeled() ? "producer" : "index";
    }

    @RequestMapping("/basicproducer")
    public String basicProducer(Model model) {
        setEnvProps(model);
        return featuresService.producersEnabeled() ? "basicproducer" : "index";
    }

    @RequestMapping("/manager")
    public String manager(Model model) {
        setEnvProps(model);
        return "manager";
    }

    private void setEnvProps(Model model) {
        model.addAttribute("contextPath", env.getProperty("server.contextPath", ""));
        model.addAttribute("producerEnabled", featuresService.producersEnabeled());
        model.addAttribute("customKafkaUrlEnabled", featuresService.customUrlsEnabled());
    }
}
