package com.bettercloud.kadmin.io.ui;

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

    @Autowired
    public KadminUiController(Environment env) {
        this.env = env;
    }

    @RequestMapping("/")
    public String home(Model model) {
        return "index";
    }

    @RequestMapping("/consumer")
    public String consumer(Model model) {
        model.addAttribute("contextPath", env.getProperty("server.contextPath", ""));
        model.addAttribute("defaultTopicName", "");
        return "consumer";
    }

    @RequestMapping("/consumer/topic/{topic}")
    public String consumer(Model model,
                @PathVariable("topic") String topic) {
        model.addAttribute("contextPath", env.getProperty("server.contextPath", ""));
        model.addAttribute("defaultTopicName", topic);
        return "consumer";
    }

    @RequestMapping("/producer")
    public String producer(Model model) {
        model.addAttribute("contextPath", env.getProperty("server.contextPath", ""));
        return "producer";
    }

    @RequestMapping("/manager")
    public String manager(Model model) {
        model.addAttribute("contextPath", env.getProperty("server.contextPath", ""));
        return "manager";
    }
}
