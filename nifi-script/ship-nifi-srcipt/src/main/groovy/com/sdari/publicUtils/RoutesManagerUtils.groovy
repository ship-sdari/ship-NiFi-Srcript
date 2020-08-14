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

    /** A relationship indicating an error while processing flow files */
    static final Relationship REL_MYSQL = new Relationship.Builder()
            .name('MySQL')
            .description('FlowFiles that success to mysql route')
            .build()

    /** A relationship indicating an error while processing flow files */
    static final Relationship REL_SEND_TO_SHORE = new Relationship.Builder()
            .name('sendToShore')
            .description('FlowFiles that success to sendToShore route')
            .build()

    /** A relationship indicating an error while processing flow files */
    static final Relationship REL_SEND_TO_DIST = new Relationship.Builder()
            .name('sendToDist')
            .description('FlowFiles that success to sendToDist route')
            .build()
    static {
        relationshipMap = [:]
        relationshipMap.put(REL_SUCCESS.getName() as String, (REL_SUCCESS))
        relationshipMap.put(REL_FAILURE.getName() as String, (REL_FAILURE))
        relationshipMap.put(REL_MYSQL.getName() as String, (REL_MYSQL))
        relationshipMap.put(REL_SEND_TO_SHORE.getName() as String, (REL_SEND_TO_SHORE))
        relationshipMap.put(REL_SEND_TO_DIST.getName() as String, (REL_SEND_TO_DIST))
    }

    static Map<String, Relationship> createRelationshipMap(List<String> names) throws Exception{
        def relationships = [:]
        for (name in names) {
            if (relationshipMap?.get(name) == null) {
                println("路由关系仓库中没有当前名称的路由关系，请定义：" + name)
                continue
            }
            relationships.put(name, relationshipMap?.get(name))
        }
        //单独添加失败路由
        relationships.put(REL_FAILURE.getName() as String, REL_FAILURE)
        relationships as Map<String, Relationship>
    }

}
