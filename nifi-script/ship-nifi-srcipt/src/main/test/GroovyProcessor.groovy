import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.sql.*
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.nio.charset.StandardCharsets
import org.apache.commons.io.IOUtils
import org.apache.nifi.annotation.behavior.EventDriven
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.dbcp.DBCPService
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.util.StandardValidators

@EventDriven
@CapabilityDescription("Execute a series of JDBC queries adding the results to each JSON presented in the FlowFile")
class GroovyProcessor implements Processor {

    def log
    private Map<String, PropertyDescriptor> descriptorMap = new HashMap<>()
    final static Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description('FlowFiles that were successfully processed and had any data enriched are routed here')
            .build()

    final static Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description('FlowFiles that were not successfully processed are routed here')
            .build()

    Set<Relationship> getRelationships() { [REL_FAILURE, REL_SUCCESS] as Set }

    final static PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("dbcp-connection-pool-services")
            .displayName("Database Connection Pool Services")
            .description("The Controller Service that is used to obtain a connection to the database.")
            .required(true)
            .identifiesControllerService(DBCPService)
            .build()

    @Override
    List<PropertyDescriptor> getPropertyDescriptors() {
        List<PropertyDescriptor> descriptorList = new ArrayList<>();
        for (String name : descriptorMap.keySet()) {
            descriptorList.add(descriptorMap.get(name))
        }
        Collections.unmodifiableList(descriptorList) as List<PropertyDescriptor>
    }

    void initialize(ProcessorInitializationContext context) { log = context.logger }

    public static GroovyRowResult toRowResult(ResultSet rs) throws SQLException {
        ResultSetMetaData metadata = rs.getMetaData();
        Map<String, Object> lhm = new LinkedHashMap<String, Object>(metadata.getColumnCount(), 1);
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            lhm.put(metadata.getColumnLabel(i), rs.getObject(i));
        }
        return new GroovyRowResult(lhm);
    }
    /**
     * A method that executes only once when initialized
     *
     * @param context
     */
    @OnScheduled
    public void OnScheduled(final ProcessContext context) {
        Map<PropertyDescriptor, String> map = context.getProperties();
        for (PropertyDescriptor descriptor : map.keySet()) {
            descriptorMap.put(descriptor.getName(), descriptor)
        }
    }

    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        def session = sessionFactory.createSession()
        def flowFile = session.get()
        if (!flowFile) return
        try {
            def fromAttributeValue = flowFile.getAttribute("for-attributes")
            GroovyClassLoader loader = new GroovyClassLoader();
            Class aClass = loader.parseClass(new File("/home/sdari/script/AmsInnerKeyVo.groovy"));

            GroovyObject instance = (GroovyObject) aClass.newInstance();
            String a="1"
            String key="SCRIPT_PROCESSOR_ID"
            if(descriptorMap.containsKey(key)){
                a=descriptorMap.get(key).getDefaultValue()
            }
            flowFile = session.putAttribute(flowFile, "from-attribute-Liumouren", a)
            flowFile = session.putAttribute(flowFile, "message",     instance.ge_fo1_total)
            session.transfer(flowFile, REL_SUCCESS)
        } catch (final Throwable t) {
            log.error('{} failed to process due to {}', [this, t] as Object[])
            session.transfer(flowFile, REL_FAILURE)
        } finally {
            session.commit()
        }
    }


    @Override
    Collection<ValidationResult> validate(ValidationContext context) { null }

    @Override
    PropertyDescriptor getPropertyDescriptor(String name) {
        switch (name) {
            default: return null
        }
    }

    @Override
    void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {}

    @Override
    String getIdentifier() { null }

}

processor = new GroovyProcessor()


