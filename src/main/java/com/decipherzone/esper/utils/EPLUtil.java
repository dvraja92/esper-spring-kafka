package com.decipherzone.esper.utils;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.DeploymentOptions;
import com.espertech.esper.runtime.client.EPDeployException;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class EPLUtil {
    private static final Logger log = LoggerFactory.getLogger(EPLUtil.class);

    public static EPCompiled compileEPL(Configuration configuration, String statement, String name) {
        log.info("Compiling EPL from statement param ({}:{})", name, statement);
        String finalStatement;
        if (name != null) {
            finalStatement = "@name(" + name + ") " + statement;
        } else {
            finalStatement = statement;
        }

        EPCompiled compiled;
        try {
            compiled = EPCompilerProvider.getCompiler().compile(finalStatement, new CompilerArguments(configuration));
        } catch (EPCompileException ex) {
            throw new RuntimeException(ex);
        }

        return compiled;
    }

    public static EPDeployment deploy(EPRuntime runtime, EPCompiled compiled) {
        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(compiled, new DeploymentOptions().setDeploymentId(UUID.randomUUID().toString()));
        } catch (EPDeployException ex) {
            throw new RuntimeException(ex);
        }
        return deployment;
    }
}
