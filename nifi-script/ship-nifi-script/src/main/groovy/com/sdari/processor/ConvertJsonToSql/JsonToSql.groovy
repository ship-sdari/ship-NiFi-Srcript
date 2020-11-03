package com.sdari.processor.ConvertJsonToSql

import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.StringUtils
import org.apache.nifi.logging.ComponentLog

import java.text.MessageFormat


/**
 * @author wanghuaizhi@sdari.mail.com
 * @date 2020/8/20 11:23
 * 将数据拆分路由到MySQL路由
 */
class JsonToSql {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    //新增
    final static String ADD = '0'
    //先删除后新增
    final static String DELETE_ADD = '1'
    //更新
    final static String UPDATE = '2'
    //删除
    final static String DELETE = '3'

    //数据处理使用参数
    final static String SID = 'sid'
    final static String OPTION = 'option'
    final static String STATUS = 'status'
    final static String databaseName = 'database.name'
    final static String TABLE_NAME_OUT = 'table.name'
    private final static String databasesMain = 'main.name'//主库
    private final static String databasesFrom = 'from.name'//从库
    private final static String formatSqlInsert = "INSERT INTO `{0}` ({1}) VALUES ({2});"
    private final static String formatSqlUpdate = 'update `{0}` set {1} where sid ={2} and id={3};'
    private final static String formatSqlDelete = 'delete from `{0}` where id={1};'

    JsonToSql(final ComponentLog logger, final int pid, final String pName, final int rid) {
        log = logger
        processorId = pid
        processorName = pName
        routeId = rid
        currentClassName = this.class.canonicalName
        log.info "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 初始化成功！"
    }

    static def calculation(params) {
        if (null == params) return null
        def returnMap = [:]
        def dataListReturn = []
        def attributesListReturn = []
        final List<JSONObject> dataList = (params as HashMap).get('data') as ArrayList
        final List<JSONObject> attributesList = ((params as HashMap).get('attributes') as ArrayList)
        final Map<String, Map<String, JSONObject>> rules = ((params as HashMap).get('rules') as Map<String, Map<String, JSONObject>>)
        final Map processorConf = ((params as HashMap).get('parameters') as HashMap)
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            final JSONObject JsonData = (dataList.get(i) as JSONObject)
            final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
            final String option = jsonAttributesFormer.get(OPTION)
            final String sid = jsonAttributesFormer.get(SID)
            final String tableName = jsonAttributesFormer.get(TABLE_NAME_OUT)
            final String status = jsonAttributesFormer.get(STATUS)
            def json = []
            JSONObject jsonAttributesFormers = jsonAttributesFormer.clone() as JSONObject
            switch (status) {
                case '1':
                    jsonAttributesFormers.put(databaseName, processorConf.get(databasesMain))
                    break
                case '0':
                    jsonAttributesFormers.put(databaseName, processorConf.get(databasesFrom) + sid)
                    break
                default:
                    if (!jsonAttributesFormers.containsKey(databaseName)) {
                        log.error "option error status=>[${status}] value=>[${option}] data:[${JsonData}]"
                    }
            }

            switch (option) {
                case ADD:
                    json.add(dataByInsert(JsonData, tableName))
                    break
                case DELETE_ADD:
                    json.add(dataByDelete(JsonData, tableName))
                    json.add(dataByInsert(JsonData, tableName))
                    break
                case UPDATE:
                    json.add(dataByUpdate(JsonData, tableName, sid))
                    break
                case DELETE:
                    json.add(dataByDelete(JsonData, tableName))
                    break
                default:
                    log.error "option error value=>[${option}] data:[${JsonData}]"
                    continue
            }
            attributesListReturn.add(jsonAttributesFormers)
            //单条数据处理结束，放入返回仓库
            dataListReturn.add(json)
        }
        //全部数据处理完毕，放入返回数据后返回
        returnMap.put('rules', rules)
        returnMap.put('attributes', attributesListReturn)
        returnMap.put('parameters', processorConf)
        returnMap.put('data', dataListReturn)
        return returnMap
    }
    /**
     *
     * 插入
     * @param data json
     * @param tableName 表名
     */
    static String dataByInsert(JSONObject data, String tableName) {
        String formatSql = formatSqlInsert
        String[] columns = new String[data.keySet().size()];
        String[] values = new String[data.keySet().size()];
        int i = -1;
        for (String column : data.keySet()) {
            i++
            Object value = data.get(column)
            if (column.toLowerCase().contains("time") && null != value) {//时间字段
                Long valueByLong = value as Long
                columns[i] = "`".concat(column).concat("`");
                int size = String.valueOf(valueByLong).size()
                if (size == 13) {
                    values[i] = "FROM_UNIXTIME(".concat(String.valueOf(valueByLong / 1000)).concat(")");
                } else {
                    values[i] = "FROM_UNIXTIME(".concat(String.valueOf(valueByLong)).concat(")");
                }
                continue;
            }
            if (null == value) {
                columns[i] = "`".concat(column).concat("`");
                values[i] = "null";
                continue;
            }
            if (value instanceof String) {
                value = "'".concat(String.valueOf(value)).concat("'");
            }
            columns[i] = "`".concat(column).concat("`");
            values[i] = String.valueOf(value);
        }
        String key = StringUtils.join(columns, ",");
        String value = StringUtils.join(values, ",");
        return MessageFormat.format(formatSql, tableName, key, value)
    }
    /**
     *
     * 更新
     * @param data json
     * @param tableName 表名
     * @param sid 船号
     */
    static String dataByUpdate(JSONObject data, String tableName, String sid) {
        String formatSql = formatSqlUpdate
        List<String> setPart = new ArrayList<>();
        String[] columns = new String[data.keySet().size() - 1];
        String[] values = new String[data.keySet().size() - 1];
        int i = -1;
        for (String column : data.keySet()) {
            i++;
            Object value = data.get(column);
            if (column.toLowerCase().contains("time") && null != value) {//时间字段
                Long valueByLong = value as Long
                columns[i] = "`".concat(column).concat("`");
                values[i] = "FROM_UNIXTIME(".concat(String.valueOf(valueByLong)).concat(")");
                setPart.add("`".concat(column).concat("`=").concat("FROM_UNIXTIME(").concat(String.valueOf(valueByLong)).concat(")"));
                continue;
            }
            if (null == value) {
                columns[i] = "`".concat(column).concat("`");
                values[i] = "null";
                setPart.add("`".concat(column).concat("`=").concat("null").concat(""));
                continue;
            }
            if (value instanceof String) {
                value = "'".concat(String.valueOf(value)).concat("'");
            }
            columns[i] = "`".concat(column).concat("`");
            values[i] = String.valueOf(value);
            setPart.add("`".concat(column).concat("`=").concat(String.valueOf(value)));
        }
        String key = StringUtils.join(columns, ",");
        String value = StringUtils.join(values, ",");

        return MessageFormat.format(formatSql, tableName, key, value, StringUtils.join(setPart, ","), sid, data.get('id'));
    }
    /**
     * 删除
     * @param data json
     * @param tableName 表名
     */
    static String dataByDelete(JSONObject data, String tableName) {
        String formatSql = formatSqlDelete
        String value = data.get("id")
        return MessageFormat.format(formatSql, tableName, value)
    }
}
