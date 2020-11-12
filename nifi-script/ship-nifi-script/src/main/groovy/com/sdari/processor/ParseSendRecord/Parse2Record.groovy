package com.sdari.processor.ParseSendRecord

import com.alibaba.fastjson.JSONObject
import groovy.sql.Sql
import org.apache.nifi.logging.ComponentLog

import java.time.Instant

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 将属性转化为回传记录并提交
 */
class Parse2Record {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static GroovyObject helper

    Parse2Record(final ComponentLog logger, final int pid, final String pName, final int rid, GroovyObject pch) {
        log = logger
        processorId = pid
        processorName = pName
        routeId = rid
        currentClassName = this.class.canonicalName
        helper = pch
        log.info "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 初始化成功！"
    }

    static def calculation(params) {
        if (null == params) return null
        def returnMap = [:]
        def dataListReturn = []
        def attributesListReturn = []
        final List<JSONObject> dataList = (params as HashMap)?.get('data') as ArrayList
        final List<JSONObject> attributesList = ((params as HashMap)?.get('attributes') as ArrayList)
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            try {
                //详细处理流程
                final JSONObject jsonDataFormer = (dataList.get(i) as JSONObject)
                final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
                def execute = {
                    def commit
                    Sql sql = ((helper?.invokeMethod('getMysqlPool',null) as Map).get('mysql.connection.doss_i') as Sql)
                    if (null == sql) throw new Exception("为获取到可用MySQL连接！")
                    String sid = jsonDataFormer.getString('sid')
                    String filename = jsonDataFormer.getString('filename')
                    String dataType = jsonDataFormer.getString('data.type')
                    String isSuccess = jsonDataFormer.getString('is.success')
                    long now = Instant.now().getEpochSecond()
                    BigDecimal sizeLater = jsonAttributesFormer.getBigDecimal('file.size')
                    BigDecimal sizeBefore = jsonAttributesFormer.getBigDecimal('size.before')
                    String compressibility = sizeLater?.divide(sizeBefore, 6, BigDecimal.ROUND_DOWN)?.multiply(BigDecimal.TEN)?.multiply(BigDecimal.TEN) + '%'
                    final String select = "SELECT COUNT(1) FROM `send_return_record` WHERE `sid` = ${sid} AND `filename` = '${filename}' AND `data_type` = '${dataType}';"
                    def count = 0
                    sql.eachRow(select){ row ->
                        count = row[0] as int
                    }
                    if (count == 0){
                        commit = "INSERT INTO `send_return_record` (`sid`,`ship_collect_protocol`,`ship_collect_freq`,`shore_group`,`shore_ip`,`shore_port`,`shore_protocol`,`shore_freq`,`compress_type`,`compressibility`,`filename`,`filesize`,`data_type`,`status`,`create_time`) " +
                                "VALUES (${sid},'${jsonDataFormer.getString('ship.collect.protocol')}',${jsonDataFormer.getDouble('ship.collect.freq')},'${jsonDataFormer.getString('shore.group')}','${jsonDataFormer.getString('shore.ip')}',${jsonDataFormer.getInteger('shore.port')},'${jsonDataFormer.getString('shore.protocol')}',${jsonDataFormer.getDouble('shore.freq')},'${jsonDataFormer.getString('compress.type')}','${compressibility}','${jsonDataFormer.getString('filename')}',${sizeLater},'${dataType}',${isSuccess},FROM_UNIXTIME(${now}));"
                        sql?.executeInsert(commit)
                    }else {
                        commit = "UPDATE `send_return_record` SET `status` = ${isSuccess} WHERE `sid` = ${sid} AND `filename` = '${filename}' AND `data_type` = '${dataType}';"
                        sql?.executeUpdate(commit)
                    }
                }
                execute.call()
                //单条数据处理结束，放入返回仓库
                dataListReturn.add(jsonDataFormer)
                attributesListReturn.add(jsonAttributesFormer)
            } catch (Exception e) {
                log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 处理单条数据时异常", e
            }
        }
        //全部数据处理完毕，放入返回数据后返回
        returnMap.put('attributes', attributesListReturn)
        returnMap.put('data', dataListReturn)
        return returnMap
    }

    static Sql getCon(final String url, final String userName, final String password, final String driver) throws Exception{
        // Creating a connection to the database
        return Sql.newInstance(url, userName, password, driver)

    }
}
