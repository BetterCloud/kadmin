package com.bettercloud.kadmin.io.ui;

import com.bettercloud.kadmin.api.models.DeserializerInfoModel;
import com.bettercloud.kadmin.api.services.DeserializerRegistryService;
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

    @Autowired
    public KadminUiController(Environment env, DeserializerRegistryService deserializerRegistryService) {
        this.env = env;
        this.deserializerRegistryService = deserializerRegistryService;
    }

    @RequestMapping("/")
    public String home(Model model) {
        return "index";
    }

    @RequestMapping("/consumer")
    public String consumer(Model model) {
        model.addAttribute("contextPath", env.getProperty("server.contextPath", ""));
        return "consumer";
    }

    @RequestMapping("/consumer/topic/{topic}/{deserializerId}")
    public String consumer(Model model,
                           @PathVariable("topic") String topic,
                           @PathVariable("topic") String deserializerId) {
        model.addAttribute("contextPath", env.getProperty("server.contextPath", ""));
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
        model.addAttribute("contextPath", env.getProperty("server.contextPath", ""));
        return "producer";
    }

    @RequestMapping("/basicproducer")
    public String basicProducer(Model model) {
        model.addAttribute("contextPath", env.getProperty("server.contextPath", ""));
        return "basicproducer";
    }

    @RequestMapping("/manager")
    public String manager(Model model) {
        model.addAttribute("contextPath", env.getProperty("server.contextPath", ""));
        return "manager";
    }
}
