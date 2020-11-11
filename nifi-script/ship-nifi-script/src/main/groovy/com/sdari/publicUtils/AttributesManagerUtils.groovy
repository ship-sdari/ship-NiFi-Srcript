package com.sdari.publicUtils

import groovy.sql.Sql

/**
 * Utility methods and constants used by the scripting components.
 */
class AttributesManagerUtils {

    static createAttributesMap(List<GroovyObject> attributeRows) throws Exception {
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
                case 'boolean':
                    attributes.put(it.getProperty('attribute_name'), it.getProperty('attribute_value') as Boolean)
                    break
                default:
                    attributes.put(it.getProperty('attribute_name'), it.getProperty('attribute_value') as String)
            }
        }
        attributes
    }

    static void loadSql(Map attributes, Map<Integer, GroovyObject> connectionDto) throws Exception {
        def mysqlPool = [:]
        for (String name in attributes.keySet()) {
            if (name.toLowerCase().startsWith("mysql.connection")) {
                Integer id = attributes.get(name) as Integer
                String url = connectionDto.get(id).getProperty('url')
                String username = connectionDto.get(id).getProperty('username')
                String password = connectionDto.get(id).getProperty('password')
                String driver = connectionDto.get(id).getProperty('driver')
                Sql conn = getCon(url, username, password, driver)
                mysqlPool.put(name, conn)
            }
        }
    }

    static void releaseSql(Map pool) throws Exception {
        for (con in pool.values()) {
            releaseCon(con as Sql)
        }
    }

    static Sql getCon(final String url, final String userName, final String password, final String driver) throws Exception {
        // Creating a connection to the database
        return Sql.newInstance(url, userName, password, driver)

    }

    static void releaseCon(Sql sql) {
        sql?.close()
    }
}
