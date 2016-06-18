package Processors;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.microsoft.azure.CloudException;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.datalake.store.*;
import com.microsoft.azure.management.datalake.store.models.*;
import com.microsoft.rest.credentials.ServiceClientCredentials;

import java.io.*;

import org.apache.nifi.annotation.lifecycle.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;


/**
 * Created by tobycoleman on 12/06/2016.
 */

@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"datalake", "open energi", "adl", "azure", "put"})
@CapabilityDescription("Store flowfiles in Azure Data Lake")
@ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the ADL object")
public class PutAzureDataLake extends AbstractProcessor {
    /*
        Azure Java SDK:
        https://azure.microsoft.com/en-gb/documentation/articles/data-lake-store-get-started-java-sdk/
     */

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    // Azure objects
    private DataLakeStoreAccountManagementClient adlsClient;
    private DataLakeStoreFileSystemManagementClient adlsFileSystemClient;
    private String adlsAccountName;

    public static final String REPLACE_RESOLUTION = "replace";
    public static final String APPEND_RESOLUTION = "append";
    public static final String FAIL_RESOLUTION = "fail";

    private Boolean needRefresh = true;

    public static final PropertyDescriptor PATH_NAME = new PropertyDescriptor.Builder()
            .name("Path")
            .description("Path for file in Azure Data Lake, e.g. /test/")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OVERWRITE = new PropertyDescriptor.Builder()
            .name("Overwrite policy")
            .description("Specifies what to do if the file already exists on Data Lake")
            .required(true)
            .defaultValue(APPEND_RESOLUTION)
            .allowableValues(REPLACE_RESOLUTION, APPEND_RESOLUTION, FAIL_RESOLUTION)
            .build();

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .name("Account name")
            .description("Azure account name")
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
        properties.add(PATH_NAME);
        properties.add(OVERWRITE);
        properties.add(ACCOUNT_NAME);
        properties.add(TENANT_ID);
        properties.add(SUBSCRIPTION_ID);
        properties.add(CLIENT_ID);
        properties.add(CLIENT_SECRET);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // Process Azure credentials
        final String clientId = context.getProperty(CLIENT_ID).getValue();
        final String tenantId = context.getProperty(TENANT_ID).getValue();
        final String clientSecret = context.getProperty(CLIENT_SECRET).getValue();
        final String subscriptionId = context.getProperty(SUBSCRIPTION_ID).getValue();
        final String accountName = context.getProperty(ACCOUNT_NAME).getValue();

        AzureSetup(clientId, tenantId, clientSecret, subscriptionId, accountName);
    }

    private void AzureSetup(String clientId, String tenantId, String clientSecret,
                            String subscriptionId, String accountName) {
        final ProcessorLog log = this.getLogger();

        log.debug("Setting up ADL credentials");
        ApplicationTokenCredentials credentials = new ApplicationTokenCredentials(clientId,
                tenantId, clientSecret, null);
        adlsAccountName = accountName;
        adlsClient = new DataLakeStoreAccountManagementClientImpl(credentials);
        adlsFileSystemClient = new DataLakeStoreFileSystemManagementClientImpl(credentials);
        adlsClient.setSubscriptionId(subscriptionId);
        needRefresh = false;
    }

    private void AzureCreateFile(String path, String contents, boolean force) throws IOException, CloudException {
        byte[] bytesContents = contents.getBytes();

        adlsFileSystemClient.getFileSystemOperations().create(adlsAccountName, path, bytesContents, force);
    }

    private void AzureAppendFile(String path, String contents) throws IOException, CloudException {
        byte[] bytesContents = contents.getBytes();

        adlsFileSystemClient.getFileSystemOperations().concurrentAppend(adlsAccountName,
                path, bytesContents, AppendModeType.AUTOCREATE);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ProcessorLog log = this.getLogger();

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        // Update credentials if required
        if (needRefresh) {
            final String clientId = context.getProperty(CLIENT_ID).getValue();
            final String tenantId = context.getProperty(TENANT_ID).getValue();
            final String clientSecret = context.getProperty(CLIENT_SECRET).getValue();
            final String subscriptionId = context.getProperty(SUBSCRIPTION_ID).getValue();
            final String accountName = context.getProperty(ACCOUNT_NAME).getValue();

            AzureSetup(clientId, tenantId, clientSecret, subscriptionId, accountName);
        }
        final String overwritePolicy = context.getProperty(OVERWRITE).getValue();
        final String filename = flowFile.getAttributes().get(CoreAttributes.FILENAME.key());
        final String path = context.getProperty(PATH_NAME).evaluateAttributeExpressions().getValue();
        String fullpath;
        // Combine into full path
        if (path.endsWith("/"))
            fullpath = path.concat(filename);
        else
            fullpath = path.concat("/").concat(filename);

        log.debug("Attempting to send Flowfile to ADL path: {}",
                new Object[] {filename});
        final long startNanos = System.nanoTime();
        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream rawIn) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn)) {
                        // Read contents of Flowfile into string
                        byte[] contents = new byte[1024];
                        int bytesRead = 0;
                        String s = "";
                        while((bytesRead = in.read(contents)) != -1) {
                            s = s.concat(new String(contents, 0, bytesRead));
                        }
                        try {
                            log.debug("Saving file to ADL");
                            // Send file to ADL
                            switch (overwritePolicy) {
                                case REPLACE_RESOLUTION:
                                    AzureCreateFile(fullpath, s, true);
                                    break;
                                case APPEND_RESOLUTION:
                                    AzureAppendFile(fullpath, s);
                                    break;
                                case FAIL_RESOLUTION:
                                    AzureCreateFile(fullpath, s, false);
                                    break;
                                default:
                                    break;
                            }
                        } catch (IOException | CloudException ae) {
                            log.error("Failed to upload file to ADL");
                            throw new IOException(ae);
                        }
                    }
                }
            });
            // Transfer flowfile
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            log.info("Successfully put {} to Azure Datalake in {} milliseconds",
                    new Object[] {fullpath, millis});
            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, filename, millis);
        }  catch (final ProcessException e) {
            log.error("Failed to put {} to ADL owing to {}",
                    new Object[]{flowFile, e});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }

    }

    @OnRemoved
    @OnShutdown
    @OnUnscheduled
    @OnStopped
    public void onClose() {

    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        needRefresh = true;
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