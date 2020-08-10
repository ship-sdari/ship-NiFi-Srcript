package com.sdari.publicUtils

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.Relationship

import java.util.concurrent.atomic.AtomicBoolean

/**
 * This class contains variables and methods common to processors.
 */

class ProcessorComponentHelper {

    final AtomicBoolean isInitialized = new AtomicBoolean(false)

    private List<PropertyDescriptor> descriptors
    private Map<String, Relationship> relationships

    List<PropertyDescriptor> getDescriptors() {
        return descriptors
    }

    void setDescriptors(List<PropertyDescriptor> descriptors) {
        this.descriptors = descriptors
    }

    Map<String, Relationship> getRelationships() {
        return relationships
    }

    void setRelationships(Map<String, Relationship> relationships) {
        this.relationships = relationships
    }

    void createDescriptors() {
        descriptors = []

        // descriptors.add(routes_manager_utils.SCRIPT_FILE)
        // descriptors.add(routes_manager_utils.SCRIPT_BODY)
        // descriptors.add(routes_manager_utils.MODULES)

        this.isInitialized.set(true)
    }

    void createRelationships(List<String> names) {
        relationships = [:]
        relationships.putAll(RoutesManagerUtils.createRelationshipList(names))

        this.isInitialized.set(true)
    }
}
