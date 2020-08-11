package com.sdari.publicUtils

import com.sdari.dto.manager.NifiProcessorAttributesDTO

/**
 * Utility methods and constants used by the scripting components.
 */
class AttributesManagerUtils {

    static createAttributesMap(List<NifiProcessorAttributesDTO> attributeRows) {
        def attributes = [:]
        attributeRows.each {
            switch ((it.getProperty('attribute_type') as String).toLowerCase()) {
                case 'long':
                    attributes.put(it.getProperty('attribute_name'), it.getProperty('attribute_value') as Long)
                    break
                case 'int':
                    attributes.put(it.getProperty('attribute_name'), it.getProperty('attribute_value') as Integer)
                    break
                case 'string':
                    attributes.put(it.getProperty('attribute_name'), it.getProperty('attribute_value') as String)
                    break
                case 'bigdicimal':
                    attributes.put(it.getProperty('attribute_name'), it.getProperty('attribute_value') as BigDecimal)
                    break
                case 'float':
                    attributes.put(it.getProperty('attribute_name'), it.getProperty('attribute_value') as Float)
                    break
                case 'double':
                    attributes.put(it.getProperty('attribute_name'), it.getProperty('attribute_value') as Double)
                    break
                default:
                    attributes.put(it.getProperty('attribute_name'), it.getProperty('attribute_value') as String)
            }
        }
        attributes
    }

}
