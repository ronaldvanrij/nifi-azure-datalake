package Processors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AzureADAuthenticator;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;


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

    public static final String OVERWRITE_RESOLUTION = "OVERWRITE";
    public static final String FAIL_RESOLUTION = "FAIL";
    
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
            .defaultValue(OVERWRITE_RESOLUTION)
            .allowableValues(OVERWRITE_RESOLUTION, FAIL_RESOLUTION)
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
    
    public static final PropertyDescriptor OCTALPERMISSIONS = new PropertyDescriptor.Builder()
            .name("File permissions")
            .description("Permissions the created file gets, e.g. 744")
            .required(true)
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

    private String clientId;
    private String clientSecret;
    private String accountName;
    private String tenantId;
    private String octalPermissions;

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
        refreshParameters(context);
    }

    private void refreshParameters(ProcessContext context) {
        this.clientId = context.getProperty(CLIENT_ID).getValue();
        this.clientSecret = context.getProperty(CLIENT_SECRET).getValue();
        this.accountName = context.getProperty(ACCOUNT_NAME).getValue() + ".azuredatalakestore.net";
        this.tenantId = context.getProperty(TENANT_ID).getValue();
        this.octalPermissions = context.getProperty(OCTALPERMISSIONS).getValue();
    }

    /**
     * Obtain OAuth2 token and use token to create client object.
     *
     * @return Authorized client with access to the FQDN
     */
    private ADLStoreClient getClient() throws java.io.IOException {
        ADLStoreClient client;
        try {
            String endpoint = "https://login.microsoftonline.com/"+this.tenantId+"/oauth2/token";
            AzureADToken token = AzureADAuthenticator.getTokenUsingClientCreds(endpoint, this.clientId, this.clientSecret);
            client = ADLStoreClient.createClient(this.accountName, token);
        } catch (java.io.IOException ex) {
            getLogger().error("Acquiring Azure AD token and instantiating ADLS client failed with exception: {}", new Object[] {ex.getMessage()});
            throw ex;
        }
        return client;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        // Update credentials if required
        if (needRefresh) {
            refreshParameters(context);
        }
        final IfExists overwritePolicy = IfExists.valueOf(context.getProperty(OVERWRITE).getValue());
        final String filename = flowFile.getAttributes().get(CoreAttributes.FILENAME.key());
        final String path = context.getProperty(PATH_NAME).evaluateAttributeExpressions(flowFile).getValue();
        String fullpath;
        // Combine into full path
        if (path.endsWith("/"))
            fullpath = path.concat(filename);
        else
            fullpath = path.concat("/").concat(filename);

        getLogger().debug("Attempting to send Flowfile to ADL path: {}", new Object[] {filename});
        final long startNanos = System.nanoTime();

        final ComponentLog logger = getLogger();

        try {

            ADLStoreClient client = getClient();

            session.read(flowFile, new InputStreamCallback() {
                
                @Override
                public void process(final InputStream rawIn) throws IOException {
                    try (OutputStream stream = client.createFile(fullpath, overwritePolicy, octalPermissions, true)) {
                        IOUtils.copy(rawIn, stream);
                    } catch (ADLException ex) {
                        logger.error("Writing to ADLS stream {} failed with ADLS exception: {}", new Object[]{fullpath, ex.getMessage()});
                    } catch (IOException ex) {
                        logger.error("Creating ADLS stream {} failed with IO exception: {}", new Object[]{fullpath, ex.getMessage()});
                    }
                }
            });
            
            // Transfer flowfile
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            getLogger().info("Successfully put {} to Azure Datalake in {} milliseconds", new Object[] {fullpath, millis});
            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, filename, millis);
        }  catch (final ProcessException e) {
            getLogger().error("Failed to put {} to ADL owing to {}", new Object[]{flowFile, e});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        } catch (IOException e) {
            getLogger().error("Failed to get a ADLS client: {}", new Object[]{e});
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