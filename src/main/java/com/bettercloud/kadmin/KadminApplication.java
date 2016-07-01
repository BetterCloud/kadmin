package com.bettercloud.kadmin;

import com.bettercloud.kadmin.testing.TestContext;
import com.bettercloud.kadmin.testing.TestListener;
import com.bettercloud.kadmin.testing.TestingRunnerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KadminApplication implements CommandLineRunner {

	@Autowired private TestingRunnerService testingService;
	@Autowired private TestListener testListener;
	@Autowired private TestContext testContext;

	public static void main(String[] args) {
		SpringApplication.run(KadminApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
//		testingService.run();
		testContext.run2943();
	}
}
