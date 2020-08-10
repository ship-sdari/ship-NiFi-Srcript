import groovy.sql.*
import org.apache.nifi.components.ValidationContext
import org.apache.nifi.components.ValidationResult
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSessionFactory
import org.apache.nifi.processor.Processor
import org.apache.nifi.processor.ProcessorInitializationContext

import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException

import org.apache.nifi.annotation.behavior.EventDriven
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.dbcp.DBCPService
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.exception.ProcessException

@EventDriven
@CapabilityDescription("Execute a series of JDBC queries adding the results to each JSON presented in the FlowFile")
 class analysisDataBySid implements Processor {

    def log

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
        Collections.unmodifiableList([DBCP_SERVICE]) as List<PropertyDescriptor>
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

    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        def session = sessionFactory.createSession()
        def flowFile = session.get()
        if (!flowFile) return

        def dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService)
        def conn = dbcpService.getConnection()
        try {
            def fromAttributeValue = flowFile.getAttribute("for-attributes")
            flowFile = session.putAttribute(flowFile, "from-attribute-Liumouren", fromAttributeValue)
            flowFile = session.putAttribute(flowFile, "message", "ceshi")
            session.transfer(flowFile, REL_SUCCESS)
        } catch (final Throwable t) {
            log.error('{} failed to process due to {}', [this, t] as Object[])
            session.transfer(flowFile, REL_FAILURE)
        } finally {
            session.commit()
            conn.close()
        }
    }


    @Override
    Collection<ValidationResult> validate(ValidationContext context) { null }

    @Override
    PropertyDescriptor getPropertyDescriptor(String name) {
        switch (name) {
            case 'JSON Lookup attribute': return LOOKUP_ATTR
            case 'Database Connection Pool Services': return DBCP_SERVICE
            default: return null
        }
    }

    @Override
    void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {}

    @Override
    String getIdentifier() { null }

}

processor = new analysisDataBySid()

