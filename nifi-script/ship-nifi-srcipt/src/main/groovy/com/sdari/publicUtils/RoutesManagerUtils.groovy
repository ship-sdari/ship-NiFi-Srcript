package com.sdari.publicUtils

import org.apache.nifi.processor.Relationship

/**
 * Utility methods and constants used by the scripting components.
 */
class RoutesManagerUtils {

    private static Map<String, Relationship> relationshipMap
    /** A relationship indicating flow files were processed successfully */
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name('success')
            .description('FlowFiles that were successfully processed')
            .build()

    /** A relationship indicating an error while processing flow files */
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name('failure')
            .description('FlowFiles that failed to be processed')
            .build()
    static {
        relationshipMap = [:]
        relationshipMap.put(REL_SUCCESS.getName() as String, (REL_SUCCESS))
        relationshipMap.put(REL_FAILURE.getName() as String, (REL_FAILURE))
    }

    static Map<String, Relationship> createRelationshipList(List<String> names) {
        def relationships = [:]
        for (name in names) {
            if (relationshipMap?.get(name) == null) {
                println("路由关系仓库中没有当前名称的路由关系，请定义：" + name)
                continue
            }
            relationships.put(name, relationshipMap?.get(name))
        }
        relationships as Map<String, Relationship>
    }

}
