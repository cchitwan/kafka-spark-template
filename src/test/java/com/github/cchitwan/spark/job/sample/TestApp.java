package com.github.cchitwan.spark.job.sample;

import com.github.cchitwan.template.app.TemplateApp;
import com.github.cchitwan.template.processor.IProcessor;

/**
 * @author chanchal.chitwan on 06/04/17.
 *
 */
public class TestApp extends TemplateApp<TestConfig, TestProcessor>{

    TestApp(){}
    public static void main(String[] args) throws Exception {
        TestApp testApp = new TestApp();
        testApp.startJob(TestConfig.class, args);
    }


    @Override
    public IProcessor registerProcessor(TestConfig config){

        System.out.println("Registering Test processor");
        return new TestProcessor(config);
    }

}
