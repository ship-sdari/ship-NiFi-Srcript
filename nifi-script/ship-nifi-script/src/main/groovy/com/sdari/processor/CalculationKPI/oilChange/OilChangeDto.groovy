package com.sdari.processor.CalculationKPI.oilChange

import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.SerializerFeature
import groovy.sql.Sql
import lombok.Data

import java.text.MessageFormat

/**
 *
 * @type: （单桨单桨）
 * @kpiName: 换油检测
 */
class OilChangeDto {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static GroovyObject helper
    private static Sql con
    static final String conName = "con.name"
    //指标名称
    private static host_use_oil = 'host_use_oil'
    private static aux_use_oil = 'aux_use_oil'
    private static boiler_oil_type = 'boiler_oil_type'
    //计算相关参数
    final static String SID = 'sid'
    final static String COLTIME = 'coltime'

    OilChangeDto(final def logger, final int pid, final String pName, final int rid, GroovyObject pch) {
        log = logger
        processorId = pid
        processorName = pName
        routeId = rid
        helper = pch
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
        final Map processorConf = ((params as HashMap).get('parameters') as HashMap)
        final Map shipConf = ((params as HashMap).get('shipConf') as HashMap)
        //加载连接
        if (null == con) {
            sqlINit()
        }
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            final JSONObject JsonData = (dataList.get(i) as JSONObject)
            final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)

            String sid = jsonAttributesFormer.get(SID)
            //   String coltime = String.valueOf(Instant.now())
            String coltime = jsonAttributesFormer.get(COLTIME)
            JSONObject json = calculationKpi((shipConf.get(sid) as Map<String, String>), JsonData, coltime, sid)

            //单条数据处理结束，放入返回
            dataListReturn.add(json)
            attributesListReturn.add(jsonAttributesFormer)
        }
        //全部数据处理完毕，放入返回数据后返回
        returnMap.put('shipConf', shipConf)
        returnMap.put('data', dataListReturn)
        returnMap.put('parameters', processorConf)
        returnMap.put('attributes', attributesListReturn)
        return returnMap
    }
    /**
     * 获取连接
     */
    static void sqlINit() {
        con = (helper.invokeMethod("getMysqlPool", null) as Map<String, Sql>).get(conName)
    }

    /**
     * 换油检测
     *
     * @param configMap 相关系统配置
     * @param data 参与计算的信号值<innerKey,value></>
     */
    static JSONObject calculationKpi(Map<String, String> configMap, JSONObject data, final String time, final String sid) {
        String JsonData = null
        try {
            Map<String, BigDecimal> hostDate = data.get(host_use_oil) as Map<String, BigDecimal>
            Map<String, BigDecimal> auxDate = data.get(aux_use_oil) as Map<String, BigDecimal>
            Map<String, BigDecimal> boilDate = data.get(boiler_oil_type) as Map<String, BigDecimal>
            // 主机使用重油指示
            BigDecimal meUseHfoStr = hostDate.get("me_use_hfo")
            //主机使用柴油/轻柴油指示
            BigDecimal meUseMdoStr = hostDate.get("me_use_mdo")
            //柴油发电机使用重油指示
            BigDecimal geUseHfoStr = auxDate.get("ge_use_hfo")
            //柴油发电机使用柴油/轻柴油指示
            BigDecimal geUseMdoStr = auxDate.get("ge_use_mdo")
            //锅炉用燃油
            BigDecimal boilerHFO = boilDate.get("boil_use_hfo")
            //锅炉用柴油
            BigDecimal boilerMOD = boilDate.get("boil_use_mdo")

            String hostUseOil = oilType(meUseHfoStr, meUseMdoStr)
            String auxUseOil = oilType(geUseHfoStr, geUseMdoStr)
            String boilerUseOil = oilType(boilerHFO, boilerMOD)

            OliChangeRecord_single monitorSingle = oilMonitor_single(hostUseOil, auxUseOil, boilerUseOil)
            if (null != monitorSingle) {
                if (monitorSingle.host_after_oil_id != null && monitorSingle.aux_after_oil_id != null && monitorSingle.boiler_after_oil_id != null) {
                    monitorSingle.sid = sid
                    monitorSingle.create_time = time
                    monitorSingle.change_time = time
                    JsonData = JSONObject.toJSONString(monitorSingle, SerializerFeature.WriteMapNullValue)
                } else {
                    log.warn("换油监测输出结果不符合规范，请检查：" + JSONObject.toJSONString(monitorSingle, SerializerFeature.WriteMapNullValue))
                }
            }
            log.debug("[${sid}] [换油检测] [${time}] result[${JsonData}] ")
            if (JsonData != null) return JSONObject.parseObject(JsonData)
            return null
        } catch (Exception e) {
            log.error("[${sid}] [换油检测] [${time}] 计算错误异常:${e} ")
            return null
        }
    }

    /**
     * 用油类型 计算
     * @param dataHFO
     * @param dataMOD
     */
    static BigDecimal oilType(BigDecimal dataHFO, BigDecimal dataMOD) {
        BigDecimal result = null
        //计算
        if (null != dataHFO && dataHFO == BigDecimal.ONE) {
            result = BigDecimal.valueOf(0)
        } else if (null != dataMOD && dataMOD == BigDecimal.ONE) {
            result = BigDecimal.valueOf(1)
        } else if (null != dataMOD && dataMOD == BigDecimal.ZERO) {
            result = BigDecimal.valueOf(0)
        }
        return result
    }

    /**
     * 单机单桨换油监测
     *
     * @param hostUseOil
     * @param auxUseOil
     * @param boilerUseOil
     * @param stmt
     * @return
     * @throws Exception
     */
    static OliChangeRecord_single oilMonitor_single(String hostUseOil, String auxUseOil, String boilerUseOil) throws Exception {
        final String sqlSelectFormat = "SELECT b.id from t_oil_change_record r LEFT JOIN t_oil_meter b on r.{0} = b.id WHERE b.oil_type = ''{1}'' ORDER BY r.change_time DESC LIMIT 1"
        final String sqlSelectNullFormat = "SELECT m.id FROM t_oil_record r LEFT JOIN t_oil_meter m ON r.oil_id  = m.id WHERE m.oil_type = ''{0}'' ORDER BY r.create_time DESC LIMIT 1"
        final String sqlSelectCurrentFormat = "select oil_type from t_oil_meter where id = {0}"
        OliChangeRecord_single changeRecord = null
        OliChangeRecord_single oilChangeRecord
        final String sqlSelectChange = "SELECT host_after_oil_id,aux_after_oil_id,boiler_after_oil_id  FROM" +
                " t_oil_change_record ORDER BY change_time DESC LIMIT 1"
        con.eachRow(sqlSelectChange) {
            res ->
                if (null == changeRecord) changeRecord = new OliChangeRecord_single()
                final Object ha = res.getObject(1)
                final Object aa = res.getObject(2)
                final Object ba = res.getObject(3)
                changeRecord.host_after_oil_id = (ha == null ? null : (Long) ha)
                changeRecord.aux_after_oil_id = (aa == null ? null : (Long) aa)
                changeRecord.boiler_after_oil_id = (ba == null ? null : (Long) ba)
        }

        if (changeRecord != null) {
            boolean flag = false
            oilChangeRecord = new OliChangeRecord_single()
            // 油品获取
            //主机
            // 油品不一样清空
            Long oidHFOId = null
            Long oidMDOId = null
            if (null == changeRecord.host_after_oil_id) {//换油记录表中，主机用油为空,则进行初始化操作
                final Long h = init_oil(hostUseOil, sqlSelectNullFormat, oidHFOId, oidMDOId) as Long
                if (null != h) {//加抽油记录表中有相关数据
                    oilChangeRecord.host_after_oil_id = (h)
                    flag = true
                }
            } else {
                String currentHostOid = null
                con.eachRow(MessageFormat.format(sqlSelectCurrentFormat, changeRecord.host_after_oil_id)) {
                    res -> currentHostOid = res.getString(1)
                }
                if (currentHostOid == null)
                    throw new Exception("t_oil_meter没有查询到当前主机油类型，语句为:" + MessageFormat.format(sqlSelectCurrentFormat,
                            changeRecord.host_after_oil_id))
                oilChangeRecord.host_before_oil_id = (changeRecord.host_after_oil_id)
                if ((currentHostOid == "HFO" && "1" == hostUseOil) || (currentHostOid == "MDO" && "0" == hostUseOil)) {
                    flag = true
                    Long hostAfterOilId = init_oil_up(hostUseOil, sqlSelectNullFormat, sqlSelectFormat, 'host_after_oil_id')
                    oilChangeRecord.host_after_oil_id = (hostAfterOilId)
                } else {
                    oilChangeRecord.host_after_oil_id = (changeRecord.host_after_oil_id)
                }
            }

            //辅机
            if (null == changeRecord.aux_after_oil_id) {//换油记录表中，辅机用油为空,则进行初始化操作
                final Long OilId = init_oil(hostUseOil, sqlSelectNullFormat, oidHFOId, oidMDOId)
                if (null != OilId) {//加抽油a记录表中有相关数据
                    oilChangeRecord.aux_after_oil_id = (OilId)
                    flag = true
                }
            } else {
                String currentAuxOid = null
                con.eachRow(MessageFormat.format(sqlSelectCurrentFormat, changeRecord.aux_after_oil_id)) {
                    res -> currentAuxOid = res.getString(1)
                }
                if (currentAuxOid == null)
                    throw new Exception("t_oil_meter没有查询到当前辅机油类型，语句为:" +
                            MessageFormat.format(sqlSelectCurrentFormat, changeRecord.aux_after_oil_id))
                oilChangeRecord.aux_before_oil_id = (changeRecord.aux_after_oil_id)
                if (currentAuxOid == "HFO" && "1" == auxUseOil || (currentAuxOid == "MDO" && "0" == auxUseOil)) {
                    flag = true
                    Long auxAfterOilId = init_oil_up(auxUseOil, sqlSelectNullFormat, sqlSelectFormat, 'aux_after_oil_id')
                    oilChangeRecord.aux_after_oil_id = (auxAfterOilId)
                } else {
                    oilChangeRecord.aux_after_oil_id = (changeRecord.aux_after_oil_id)
                }
            }

            //锅炉
            if (null == changeRecord.boiler_after_oil_id) {//换油记录表中，锅炉用油为空,则进行初始化操作
                final Long OilId = init_oil(hostUseOil, sqlSelectNullFormat, oidHFOId, oidMDOId)
                if (null != OilId) {//加抽油记录表中有相关数据
                    oilChangeRecord.boiler_after_oil_id = (OilId)
                    flag = true
                }
            } else {
                String currentBoilerOid = null
                con.eachRow(MessageFormat.format(sqlSelectCurrentFormat, changeRecord.boiler_after_oil_id)) {
                    res -> currentBoilerOid = res.getString(1)
                }
                if (currentBoilerOid == null)
                    throw new Exception("t_oil_meter没有查询到当前锅炉油类型，语句为:" +
                            MessageFormat.format(sqlSelectCurrentFormat, changeRecord.boiler_after_oil_id))
                oilChangeRecord.boiler_before_oil_id = (changeRecord.boiler_after_oil_id)
                if (currentBoilerOid == "HFO" && "1" == boilerUseOil || (currentBoilerOid == "MDO" && "0" == boilerUseOil)) {
                    flag = true
                    Long boilerAfterOilId = init_oil_up(boilerUseOil, sqlSelectNullFormat, sqlSelectFormat, 'boiler_after_oil_id')
                    oilChangeRecord.boiler_after_oil_id = (boilerAfterOilId)
                } else {
                    oilChangeRecord.boiler_after_oil_id = (changeRecord.boiler_after_oil_id)
                }
            }
            if (!flag) {
                oilChangeRecord = null
            }
        } else {//初始的时候，表中无数据则进行初始化数据
            boolean flag = false
            oilChangeRecord = new OliChangeRecord_single()
            // 油品获取
            //主机
            // 油品不一样清空
            Long oidHFOId = null
            Long oidMDOId = null
            final Long h = init_oil(hostUseOil, sqlSelectNullFormat, oidHFOId, oidMDOId)
            if (null != h) {//加抽油记录表中有相关数据
                oilChangeRecord.host_after_oil_id = (h)
                flag = true
            }

            //辅机
            final Long a = init_oil(hostUseOil, sqlSelectNullFormat, oidHFOId, oidMDOId)
            if (null != a) {//加抽油记录表中有相关数据
                oilChangeRecord.aux_after_oil_id = (a)
                flag = true
            }
            //锅炉
            final Long b = init_oil(hostUseOil, sqlSelectNullFormat, oidHFOId, oidMDOId)
            if (null != b) {//加抽油记录表中有相关数据
                oilChangeRecord.boiler_after_oil_id = (b)
                flag = true
            }
            if (!flag) {
                oilChangeRecord = null
            }
        }
        return oilChangeRecord
        // 否則是用換油記錄
    }

    /**
     * 根据用油类型 查询加抽油记录
     * @param OilType
     * @param stmt
     * @param sqlSelectNullFormat
     * @param oidHFOId
     * @param oidMDOId
     */
    private static Long init_oil(String OilType, String sqlSelectNullFormat,
                                 Long oidHFOId, Long oidMDOId) throws Exception {
        Long OilId = null
        if ("0" == OilType) {
            con.eachRow(MessageFormat.format(sqlSelectNullFormat, "HFO")) {
                res -> OilId = oidHFOId == null ? res.getLong(1) : oidHFOId
            }
        } else {
            con.eachRow(MessageFormat.format(sqlSelectNullFormat, "HFO")) {
                res -> OilId = oidMDOId == null ? res.getLong(1) : oidMDOId
            }
        }
        return OilId

    }
    /**
     * 根据用油类型 查询加抽油记录
     * @param OilType
     * @param stmt
     * @param sqlSelectNullFormat
     * @param oidHFOId
     * @param oidMDOId
     */
    private static Long init_oil_up(String UseOil, String sqlSelectNullFormat,
                                    String sqlSelectFormat, String sqlType) throws Exception {
        Long OilId = null
        con.eachRow(MessageFormat.format(sqlSelectFormat, sqlType, "1" == UseOil ? "MDO" : "HFO")) {
            res -> OilId = res.getLong(1)
        }
        if (OilId == null) {
            if ("0" == UseOil) {
                con.eachRow(MessageFormat.format(sqlSelectNullFormat, "HFO")) {
                    res -> OilId = res.getLong(1)
                }
            } else {
                con.eachRow(MessageFormat.format(sqlSelectNullFormat, "MDO")) {
                    res -> OilId = res.getLong(1)
                }
            }
        }
        return OilId
    }

    @Data
    static class OliChangeRecord_single implements Serializable {
        /**
         * 船id
         */
        private String sid

        /**
         * 主机换油前油品
         */
        private Long host_before_oil_id

        /**
         * 主机换油后油品
         */
        private Long host_after_oil_id

        /**
         * 辅机换油前油品
         */
        private Long aux_before_oil_id

        /**
         * 辅机换油后油品
         */
        private Long aux_after_oil_id

        /**
         * 锅炉换油前油品
         */
        private Long boiler_before_oil_id

        /**
         * 锅炉换油后油品
         */
        private Long boiler_after_oil_id

        /**
         * 换油时间
         */
        private String change_time

        private String create_time

        private String update_time

    }
}

