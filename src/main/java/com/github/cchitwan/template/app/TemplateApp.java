package com.github.cchitwan.template.app;

import com.github.cchitwan.template.processor.IProcessor;
import com.github.vivekkothari.YamlParser;
import com.github.cchitwan.template.config.IConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author chanchal.chitwan on 06/04/17.
 * This library helps to give a starting point for a spark job to get single JavaDStream<T>
 * from kafka topic event. You just need to define how you wanna process it at each RDD/Partion level.
 *
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public  class TemplateApp<T extends IConfig, R  extends IProcessor> {
    private T config;
    private R processor;
    private Options options;
    private String ymlPath;

    private void init(Class<T> t, String...args) throws IOException, ParseException {
        options = new Options();
        /**
        * calling your customized options
        */
        customOptions(options);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd;

        cmd = parser.parse(options, args);
        /**
        * calling your customized check options
        */
        checkOptions(cmd);

        config = YamlParser.loadWithEnvironmentResolution(new FileInputStream(ymlPath), t);
    }
    private void help(Options options) {
        HelpFormatter f = new HelpFormatter();
        f.printHelp("Options Tip", options);
    }

    /**
     * Override this to add more options
     * @param options
     */
    protected void customOptions(Options options){
        options.addOption("config", true, "config yml file");
    }

    /**
     * Override this to check more options values
     * @param cmd
     */
    protected void checkOptions(CommandLine cmd){
        if (cmd.hasOption("h")) {
            help(options);
            System.exit(1);
        }
        ymlPath = cmd.getOptionValue("config");

        if (ymlPath == null || ymlPath.isEmpty()) {
            throw new RuntimeException("config yml file is mandatory");
        }
    }

    /**
     * This is entry point of App
     * @param t your app config class
     * @param args
     * @throws Exception
     */
    public void startJob(Class<T> t, String...args) throws Exception {
        init(t, args);
        if(config.isEnableConfigLogging()) {
            System.out.println(config.toString() + " \nparent config" + config.getParentConfig());
        }
        this.setProcessor((R)this.registerProcessor((T)config));
        processor.startJob();
    }

    /**
     * Override this to set your customed processor
     * @param config
     * @return
     */
    protected IProcessor registerProcessor(T config) throws Exception {
//        return (R) new AutoDebitDelinkJob(config);
        System.out.println("Default processor is null. Set your own processor");
        return null;
    }


}
