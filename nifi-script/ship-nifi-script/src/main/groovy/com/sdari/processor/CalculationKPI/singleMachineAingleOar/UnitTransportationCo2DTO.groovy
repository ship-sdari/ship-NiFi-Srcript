package com.sdari.processor.CalculationKPI.singleMachineAingleOar

import com.alibaba.fastjson.JSONObject

import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.time.Instant

/**
 *
 * @type: （单桨单桨）
 * @kpiName: 单位运输量co2
 */
class UnitTransportationCo2DTO {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static GroovyObject helper
    //指标名称
    private static kpiName = 'unit_transportation_co2'
    //计算相关参数
    final static String SID = 'sid'
    final static String COLTIME = 'coltime'
    //计算相关innerKey
    //主机使用重油指示
    static final String ME_USE_HFO = "me_use_hfo";
    //主机使用柴油/轻柴油指示
    static final String ME_USE_MDO = "me_use_mdo";
    //主机辅机锅炉进出口
    static final String ME_FO_IN_TOTAL = "me_fo_in_total";
    static final String ME_FO_OUT_TOTAL = "me_fo_out_total";
    static final String GE_FO_IN_TOTAL = "ge_fo_in_total";
    static final String GE_FO_OUT_TATAL = "ge_fo_out_total";
    static final String BOIL_FO_IN_TATAL = "boil_fo_in_total";
    static final String BOIL_FO_OUT_TATAL = "boil_fo_out_total";
    //主机辅机锅炉进出口
    static final String ME_FO_IN_RATE = "me_fo_in_rate";
    static final String ME_FO_OUT_RATE = "me_fo_out_rate";
    static final String GE_FO_IN_RATE = "ge_fo_in_rate";
    static final String GE_FO_OUT_RATE = "ge_fo_out_rate";
    static final String BOIL_FO_IN_RATE = "boil_fo_in_rate";
    static final String BOIL_FO_OUT_RATE = "boil_fo_out_rate";

    //计算相关 业务相关key
    //重油含碳量
    final static String CF_HFO = "CF_HFO";
    //轻油含碳量
    final static String CF_MDO = "CF_MDO";
    // 预设载货量,单位:吨
    final static String SHIP_CARGO_VOLUME = "CARGO_VOLUME"
    // 油耗计算方式
    final static String OIL_CALCULATION_TYPE = "OIL_CALCULATION_TYPE";
    final static BigDecimal BIG1000F = 1000f

    UnitTransportationCo2DTO(final def logger, final int pid, final String pName, final int rid, GroovyObject pch) {
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
        Connection con = ((params as HashMap).get('con')) as Connection

        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            JSONObject json = new JSONObject()
            final JSONObject JsonData = (dataList.get(i) as JSONObject)
            final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)

            String sid = jsonAttributesFormer.get(SID)
            String coltime = String.valueOf(Instant.now())
            //  String coltime = jsonAttributesFormer.get(COLTIME)
            //判断数据里是否 有 当前计算指标数据
            if (!JsonData.containsKey(kpiName)) {
                log.debug("[${sid}] [${kpiName}] [没有当前指标 计算所需的数据] result[${null}] ")
                json.put(kpiName, null)
            } else {
                Map<String, BigDecimal> maps = JsonData.get(kpiName) as Map<String, BigDecimal>
                BigDecimal result = calculationKpi(con, (shipConf.get(sid) as Map<String, String>), maps, coltime, sid)
                json.put(kpiName, result)
            }
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
     *
     * （进口质量流量-出口质量流量）*（含碳量*1000000）/载货量
     *
     * @param configMap 相关系统配置
     * @param data 参与计算的信号值<innerKey,value></>
     */
    static BigDecimal calculationKpi(Connection con, Map<String, String> configMap, Map<String, BigDecimal> data, final String time, final String sid) {
        try {
            BigDecimal result = null
            Integer oilType
            BigDecimal hfo = data.get(ME_USE_HFO);
            BigDecimal mdo = data.get(ME_USE_MDO);
            String key;
            if (null != hfo && hfo == 1) {
                key = CF_HFO;
            } else if (null != mdo && mdo == 1) {
                key = CF_MDO;
            } else {
                key = CF_HFO;
            }
            BigDecimal cf = new BigDecimal(configMap.get(key));
            BigDecimal CargoMass = selectCargoMass(configMap, sid, con, time)
            String oilCalculationType = configMap.get(OIL_CALCULATION_TYPE)
            if (oilCalculationType != null && !oilCalculationType.isEmpty()) {
                oilType = Integer.parseInt(oilCalculationType);
            } else {
                oilType = 1
            }
            // 进口质量流量计流速（主机/辅机/锅炉）
            BigDecimal me_inRate;
            BigDecimal ge_inRate;
            BigDecimal blr_inRate;

            // 出口质量流量计流速（主机/辅机/锅炉）
            BigDecimal me_outRate;
            BigDecimal ge_outRate;
            BigDecimal blr_outRate;
            if (oilType == 0) {
                // 获取主机流入流量
                me_inRate = data.get(ME_FO_IN_TOTAL);
                // 获取主机流出流量
                me_outRate = data.get(ME_FO_OUT_TOTAL);
                //60频率
                // 获取辅机流入流量
                ge_inRate = data.get(GE_FO_IN_TOTAL);
                // 获取辅机流出流量
                ge_outRate = data.get(GE_FO_OUT_TATAL);
                // 获取锅炉流入流量
                blr_inRate = data.get(BOIL_FO_IN_TATAL);
                // 获取锅炉流出流量
                blr_outRate = data.get(BOIL_FO_OUT_TATAL);
            } else {
                // 获取主机流入流量
                me_inRate = data.get(ME_FO_IN_RATE);
                // 获取主机流出流量
                me_outRate = data.get(ME_FO_OUT_RATE);
                // 获取辅机流入流量
                ge_inRate = data.get(GE_FO_IN_RATE);
                // 获取辅机流出流量
                ge_outRate = data.get(GE_FO_OUT_RATE);
                // 获取锅炉流入流量
                blr_inRate = data.get(BOIL_FO_IN_RATE);
                // 获取锅炉流出流量
                blr_outRate = data.get(BOIL_FO_OUT_RATE);
            }

            if (CargoMass == null || 0 == null || me_inRate == null || ge_inRate == null || blr_inRate == null || me_outRate == null || ge_outRate == null
                    || blr_outRate == null) {
                log.debug("[${sid}] [${kpiName}] [${time}] 载货量{${CargoMass}} 含碳量{${cf}} " +
                        "进口质量流量计流速（主机[${me_inRate}]/辅机[${ge_inRate}]/锅炉[${blr_inRate}]）" +
                        "出口质量流量计流速（主机[${me_outRate}]/辅机[${ge_outRate}]/锅炉[${blr_outRate}] result[${result}]")
                return null
            }
            //开始计算指标
            BigDecimal fint;
            try {
                BigDecimal me = me_inRate.subtract(me_outRate);
                BigDecimal ge = ge_inRate.subtract(ge_outRate);
                BigDecimal blr = blr_inRate.subtract(blr_outRate).divide(BIG1000F, 4);
                fint = me.add(ge).add(blr);
            } catch (Exception e) {
                fint = BigDecimal.ZERO;
                log.error("[${sid}] [${kpiName}] [${time}] 载货量{${fint}} 含碳量{${cf}} " +
                        "进口质量流量计流速（主机[${me_inRate}]/辅机[${ge_inRate}]/锅炉[${blr_inRate}]）" +
                        "出口质量流量计流速（主机[${me_outRate}]/辅机[${ge_outRate}]/锅炉[${blr_outRate}] " +
                        " e:[${e}]）")
            }
            result = ((fint * BigDecimal.valueOf(1000000)) * cf).divide(CargoMass, 2, BigDecimal.ROUND_HALF_UP);

            log.debug("[${sid}] [${kpiName}] [${time}] 载货量{${CargoMass}} 含碳量{${cf}} " +
                    "进口质量流量计流速（主机[${me_inRate}]/辅机[${ge_inRate}]/锅炉[${blr_inRate}]）" +
                    "出口质量流量计流速（主机[${me_outRate}]/辅机[${ge_outRate}]/锅炉[${blr_outRate}] " +
                    " result[${result}]")
            return result
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${time}] 计算错误异常:${e} ")
            return null
        }
    }

    /**
     *  selectCargoMass
     *  获取载货量
     * @param configMap
     * @param sid
     * @param con
     */
    static BigDecimal selectCargoMass(Map<String, String> configMap, String sid, Connection con, String time) {
        BigDecimal CargoMass = null;
        ResultSet resultSet
        Statement stmt
        try {
            final String sql_draft1 = ("SELECT `cargo_volume` FROM  `t_sail_voyage` where  sid =").concat(sid).concat(" order by create_time DESC;");
            stmt = con.createStatement();
            //查询频率计算（根据船及key）
            resultSet = stmt.executeQuery(sql_draft1);
            while (resultSet.next()) {
                BigDecimal cargoMass = resultSet.getBigDecimal(1);
                if (cargoMass != null) {
                    CargoMass = cargoMass
                }
            }
            if (!resultSet.isClosed()) resultSet.close();
            if (!stmt.isClosed()) stmt.close();
            if (CargoMass == null) {
                CargoMass = new BigDecimal(configMap.get(SHIP_CARGO_VOLUME));
            }
            return CargoMass
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${time}]  CargoMass [${CargoMass}] [查询载货量异常]:${e}  ")
            return CargoMass
        } finally {
            if (resultSet != null && !resultSet.isClosed()) resultSet.close()
            if (stmt != null && !stmt.isClosed()) stmt.close()
        }
    }
}

