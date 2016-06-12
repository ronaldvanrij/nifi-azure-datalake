package Processors;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;

import com.microsoft.azure.CloudException;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.datalake.store.*;
import com.microsoft.azure.management.datalake.store.models.*;
import com.microsoft.rest.credentials.ServiceClientCredentials;

import java.io.*;
import java.nio.charset.Charset;

import org.apache.nifi.annotation.lifecycle.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.expression.AttributeExpression;


/**
 * Created by tobycoleman on 12/06/2016.
 */

@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"datalake", "open energi", "adl", "azure", "put"})
@CapabilityDescription("Store flowfiles in Azure Datalake")
public class PutAzureDatalake extends AbstractProcessor {
    /*
        Azure Java SDK:
        https://azure.microsoft.com/en-gb/documentation/articles/data-lake-store-get-started-java-sdk/
     */

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .name("Account name")
            .description("Azure account name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RESOURCE_GROUP = new PropertyDescriptor.Builder()
            .name("Resource group name")
            .description("Azure resource group name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TENANT_ID = new PropertyDescriptor.Builder()
            .name("Tenant ID")
            .description("Azure tenant ID")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SUBSCRIPTION_ID = new PropertyDescriptor.Builder()
            .name("Subscription ID")
            .description("Azure subscription ID")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder()
            .name("Client ID")
            .description("Azure client ID")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_SECRET = new PropertyDescriptor.Builder()
            .name("Client secret")
            .description("Azure client secret")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AZURE_REGION = new PropertyDescriptor.Builder()
            .name("Azure region")
            .description("Azure region, e.g. North Europe, US East 2, etc.")
            .required(true)
            .sensitive(true)
            .defaultValue("North Europe")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully transferred to ADL")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to put file to ADL")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ACCOUNT_NAME);
        properties.add(RESOURCE_GROUP);
        properties.add(TENANT_ID);
        properties.add(SUBSCRIPTION_ID);
        properties.add(CLIENT_ID);
        properties.add(CLIENT_SECRET);
        properties.add(AZURE_REGION);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ProcessorLog log = this.getLogger();

    }

    @OnRemoved
    @OnShutdown
    @OnUnscheduled
    @OnStopped
    public void onClose() {

    }

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

}