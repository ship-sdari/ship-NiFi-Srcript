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

    /** A relationship indicating send_to_shore while processing flow files */
    static final Relationship REL_SEND_TO_SHORE = new Relationship.Builder()
            .name('send_to_shore')
            .description('FlowFiles that success to sendToShore route')
            .build()

    /** A relationship indicating send_to_dist while processing flow files */
    static final Relationship REL_SEND_TO_DIST = new Relationship.Builder()
            .name('send_to_dist')
            .description('FlowFiles that success to sendToDist route')
            .build()
    /** A relationship indicating mysql while processing flow files */
    static final Relationship REL_MYSQL = new Relationship.Builder()
            .name('mysql')
            .description('FlowFiles that success to mysql route')
            .build()
    /** A relationship indicating hbase while processing flow files */
    static final Relationship REL_HBASE = new Relationship.Builder()
            .name('hbase')
            .description('FlowFiles that success to mysql route')
            .build()
    /** A relationship indicating es while processing flow files */
    static final Relationship REL_ES = new Relationship.Builder()
            .name('es')
            .description('FlowFiles that success to sendToDist route')
            .build()
    /** A relationship indicating hive while processing flow files */
    static final Relationship REL_HIVE = new Relationship.Builder()
            .name('hive')
            .description('FlowFiles that success to sendToDist route')
            .build()
    /** A relationship indicating realtime_alarm while processing flow files */
    static final Relationship REL_ALARM_REALTIME = new Relationship.Builder()
            .name('realtime_alarm')
            .description('FlowFiles that success to sendToDist route')
            .build()
    /** A relationship indicating window_alarm while processing flow files */
    static final Relationship REL_ALARM_WINDOW = new Relationship.Builder()
            .name('window_alarm')
            .description('FlowFiles that success to sendToDist route')
            .build()
    /** A relationship indicating window_alarm while processing flow files */
    static final Relationship REL_MERGED = new Relationship.Builder()
            .name('merged')
            .description('FlowFiles that success to merged route')
            .build()
    static {
        relationshipMap = [:]
        relationshipMap.put(REL_SUCCESS.getName() as String, (REL_SUCCESS))
        relationshipMap.put(REL_FAILURE.getName() as String, (REL_FAILURE))

        relationshipMap.put(REL_SEND_TO_SHORE.getName() as String, (REL_SEND_TO_SHORE))
        relationshipMap.put(REL_SEND_TO_DIST.getName() as String, (REL_SEND_TO_DIST))

        relationshipMap.put(REL_MYSQL.getName() as String, (REL_MYSQL))
        relationshipMap.put(REL_ES.getName() as String, (REL_ES))
        relationshipMap.put(REL_HIVE.getName() as String, (REL_HIVE))
        relationshipMap.put(REL_HBASE.getName() as String, (REL_HBASE))

        relationshipMap.put(REL_ALARM_REALTIME.getName() as String, (REL_ALARM_REALTIME))
        relationshipMap.put(REL_ALARM_WINDOW.getName() as String, (REL_ALARM_WINDOW))

        relationshipMap.put(REL_MERGED.getName() as String, (REL_MERGED))
    }

    static Map<String, Relationship> createRelationshipMap(List<String> names) throws Exception {
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
