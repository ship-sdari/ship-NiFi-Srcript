import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.nifi.components.ValidationContext
import org.apache.nifi.components.ValidationResult
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSessionFactory
import org.apache.nifi.processor.Processor
import org.apache.nifi.processor.ProcessorInitializationContext

import java.lang.String
import java.sql.*
import groovy.sql.*
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.ProcessSession
import org.python.antlr.ast.Set
import java.util.Set
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
    private static Map<String, PropertyDescriptor> descriptorMap = new HashMap<>()
    private static Map<String, Relationship> relationshipMap = new HashMap<>()

    public static final Relationship MY_SUCCESS = new Relationship.Builder()
            .name("my_success")
            .description("FlowFiles that were successfully processed")
            .build();
    public static String id = "1"
    private static String t = "12"
    private DBCPService dbcpService = null
    private static String t2 = "13"

    Set<Relationship> getRelationships() {
        Set<Relationship> set = new HashSet<Relationship>()
        set.add(MY_SUCCESS)
        for (String relation : relationshipMap.keySet()) {
            Relationship relationship = relationshipMap.get(relation)
            set.add(relationship);
        }
        return Collections.unmodifiableSet(set) as Set<Relationship>;
    }

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
        descriptorList.add(DBCP_SERVICE);
        for (String name : descriptorMap.keySet()) {
            descriptorList.add(descriptorMap.get(name))
        }
        Collections.unmodifiableList(descriptorList) as List<PropertyDescriptor>
    }

    public void scriptByInitId(pid, service) throws Exception {
        t2 = t2 + 'scriptByInitId ->15'
        id = pid
        dbcpService = service
        final Relationship REL_SUCCESS = new Relationship.Builder()
                .name("success")
                .description("FlowFiles that were successfully processed")
                .build();
        final Relationship REL_FAILURE = new Relationship.Builder()
                .name("failure")
                .description("FlowFiles that were successfully processed")
                .build();
        relationshipMap.put("failure", REL_FAILURE);
        relationshipMap.put("success", REL_SUCCESS);
    }

    public void setLogger(final ComponentLog logger) throws Exception {
        t2 = t2 + 'setLogger-> 16'
        log = logger
        log.info("进去方法setLogger")
    }

    void initialize(ProcessorInitializationContext context) {

    }

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
    public void onScheduled(final ProcessContext context) {
        Map<PropertyDescriptor, String> map = context.getProperties()
        Set<Relationship> relationshipSet = context.getAvailableRelationships()
        t = "13";
        for (PropertyDescriptor descriptor : map.keySet()) {
            descriptorMap.put(descriptor.getName(), descriptor)
        }
        relationshipMap.put(MY_SUCCESS.getName(), MY_SUCCESS);
        for (Relationship relationship : relationshipSet) {
            relationshipMap.put(relationship.getName(), relationship)
        }
    }

    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession()
        FlowFile flowFile = session.get()
        if (!flowFile) return
        try {
            def fromAttributeValue = flowFile.getAttribute("for-attributes")
            GroovyClassLoader loader = new GroovyClassLoader();
            Class aClass = loader.parseClass(new File("/home/sdari/script/AmsInnerKeyVo.groovy"));

            GroovyObject instance = (GroovyObject) aClass.newInstance();
            String a = "1"
            String a2 = "3"
            String key = "SCRIPT_PROCESSOR_ID"
            if (descriptorMap.containsKey(key)) {
                a = descriptorMap.get(key).getName()
            }
            String key2 = "dbcp-connection-pool-services"
            if (descriptorMap.containsKey(key2)) {
                a2 = descriptorMap.get(key2).getName()
            }
            flowFile = session.putAttribute(flowFile, "from-attribute-Liumouren", a)
            flowFile = session.putAttribute(flowFile, "message", t)
            flowFile = session.putAttribute(flowFile, "my", a2)
            flowFile = session.putAttribute(flowFile, "my2", t2)
            if (relationshipMap.containsKey("my_success")) {
                Relationship relationship = relationshipMap.get("my_success")
                session.transfer(session.clone(flowFile), relationship)
            }
            if (relationshipMap.containsKey("success")) {
                Relationship relationship = relationshipMap.get("success")
                session.transfer(session.clone(flowFile), relationship)
            }
            if (relationshipMap.containsKey("failure")) {
                Relationship relationship = relationshipMap.get("failure")
                session.transfer(session.clone(flowFile), relationship)
            }
            if (dbcpService != null) {
                flowFile = session.putAttribute(flowFile, "get_connection", "ok")
            }
            flowFile = session.putAttribute(flowFile, "get_id", id)
            session.transfer(flowFile, MY_SUCCESS)
        } catch (final Throwable t) {
            log.error('{} failed to process due to {}', [this, t] as Object[])
//            Relationship relationship = relationshipMap.get("failure")
            session.transfer(flowFile, MY_SUCCESS)
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


