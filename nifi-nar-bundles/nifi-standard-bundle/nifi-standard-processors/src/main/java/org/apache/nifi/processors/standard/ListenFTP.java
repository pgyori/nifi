/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.standard;

import org.apache.ftpserver.ftplet.FtpException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.ftp.NifiFtpServer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"ingest", "ftp", "listen"})
@CapabilityDescription("Starts an FTP Server and listens on a given port to transform incoming files into FlowFiles. "
        + "The URI of the Service will be http://{hostname}:{port}. The default port is 2221.")
public class ListenFTP extends AbstractSessionFactoryProcessor {

    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship for successfully received files")
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("listening-port")
            .displayName("Listening Port")
            .description("The Port to listen on for incoming connections")
            .required(true)
            .defaultValue("2221")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("username")
            .displayName("Username")
            .description("The name of the user that is allowed to log in to the FTP server. " +
                    "If a username is provided, a password must also be provided. " +
                    "If no username is specified, anonymous connections will be permitted.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Password")
            .description("If a Username is specified, then a password must also be specified. " +
                    "The password provided by the client trying to log in to the FTP server will be checked against this password.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    private volatile NifiFtpServer ftpServer;
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;
    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference = new AtomicReference<>();

    @Override
    protected void init(ProcessorInitializationContext context) {
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(RELATIONSHIP_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();
        propertyDescriptors.add(PORT);
        propertyDescriptors.add(USERNAME);
        propertyDescriptors.add(PASSWORD);
        properties = Collections.unmodifiableList(propertyDescriptors);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void startFtpServer(ProcessContext context) throws FtpException {
        String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        int port = Integer.parseInt(context.getProperty(PORT).evaluateAttributeExpressions().getValue());
        ftpServer = new NifiFtpServer(sessionFactoryReference, username, password, port);//TODO: create custom validator for password!=null (also check if it comes from var registry)

        try {
            ftpServer.start();
        } catch (FtpException ftpException) {
            stopFtpServer();
            throw ftpException;
        }
    }

    @OnStopped
    public void stopFtpServer() {
        if (ftpServer != null && !ftpServer.isStopped()) {
            ftpServer.stop();
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        sessionFactoryReference.compareAndSet(null, sessionFactory);
        context.yield();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

}
