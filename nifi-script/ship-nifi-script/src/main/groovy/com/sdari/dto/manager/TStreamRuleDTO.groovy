package com.sdari.dto.manager

import lombok.Data
import org.omg.CORBA.INTERNAL

import java.sql.ResultSet

@Data
class TStreamRuleDTO {
    //  配置规则号
    private Integer rule_id
    // 船id
    private Integer sid
    //船舶ID
    private String ship_id

    //系统ID
    private Integer sys_id

    //信号分组分表编号-新增
    private Integer cat_id

    // DOSS系统key值
    private Integer doss_key

    //通讯协议
    private String protocol

    // 信号中文名
    private String name_chn

    //信号英文名
    private String name_eng

    // 原始key值
    private String orig_key

    //     数据来源标志位
    private String data_from

    //单位
    private String unit

    //量纲转换因子
    private BigDecimal transfer_factor

    //系数-修改
    private BigDecimal coefficient

    //启用状态
    // A - 活跃
    // S - 暂时禁用
    // D - 删除"
    private String status

    //开关量/模拟量
    private String value_type

    //量程最小值
    private BigDecimal value_min

    //量程最小值
    private BigDecimal value_max

    // 应用名称
    private String inner_key

    //输入时间
    private String input_time

    //输入用户
    private String input_user

    //最后更新时间
    private String last_modify_time

    //最后更新用户
    private String last_modify_user

    //报警分表
    private List<AlarmDTO> alarm = []

    //计算分表
    private List<CalculationDTO> calculation = []

    //采集分表
    private List<CollectionDTO> collection = []

    //第三方分发分表
    private List<DistDTO> other_distributions = []

    //岸基分发分表
    private List<ShoreBasedDTO> shore_based_distributions = []

    //抽稀分表
    private List<ThinningDTO> thinning = []

    //入库分表
    private List<WarehousingDTO> warehousing = []

    @Data
    static class AlarmDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        // 报警最大值，最小值范围
        private BigDecimal alert_min
        private BigDecimal alert_max
        private BigDecimal alert_2nd_min
        private BigDecimal alert_2nd_max
        //关联设备状态标志位
        private String relate_stop_sig
        //    报警启用状态字段
        private String alert_status
        //    报警是否弹窗
        private String is_popup
        //    AMS报警报警参考
        private Integer ams_alarm_standard
        //    报警方式
        private String alert_way
    }

    @Data
    static class CalculationDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        //  参与指标计算所转换的key值
        private String calculation_key
        //指标名称标志位
        private String formula_flag
        //开启状态-新增
        private String calculation_status
    }

    @Data
    static class CollectionDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        // 采集组编号
        private String colgroup
        //SlaveID从站编号
        private Integer modbus_slave_id
        //modbus操作功能
        private Integer modbus_func_id
        //modbus寄存器地址
        private String addr
        //数据来源IP地址
        private String ip_addr
        //    用于链路中断暂存字段-删除
//        private String ip_addr_down
        //端口号-修改
        private Integer port_addr
        //    来源表名
        private String from_table_id
        //   来源列名
        private String from_column_id
        //通讯协议
        private String protocol
        //采样频率
        private Double col_freq
        // 主题
        private String topic
        // modbus信号标签
        private String modbus_sig_tag
        //数据包请求间隔
        private Double col_interval
        //数据包请求数量-修改
        private Integer col_count
        //nema0183_config关联id
        private Long nmea_id
        //启用状态-新增
        private String collection_status
    }

    @Data
    static class DistDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        //数据分发分组
        private String dist_group
        //数据分发目的IP
        private String dist_ip

        //数据分发目的端口-修改
        private Integer dist_port

        //    链路中断暂存字段-删除
//        private String dist_ip_addr_down

        //  数据分发频率
        private Double dist_freq

        //数据分发协议
        private String dist_protocol

        //    链路中断暂存字段=删除
//        private String dis_user_and_password_down

        //  用于SFTP分发的用户名和密码-修改
        private String dist_user_and_password

        //启用状态-修改
        private String dist_status
    }

    @Data
    static class ShoreBasedDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        //船岸传输组
        private String to_shore_group
        //船岸传输目的IP
        private String to_shore_ip
        //船岸传输目的port
        private Integer to_shore_port
        //船岸传输频率
        private Double to_shore_freq
        //船岸传输协议
        private String to_shore_protocol
        //岸基压缩方式
        private String compress_type
        //启用状态-修改
        private String to_shore_status
    }

    @Data
    static class ThinningDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        //    抽稀频率-修改
        private Integer sparse_rate
        //1.求累计 2.求平均 3.只取点
        private Integer dilution_type
        //开启状态-新增
        private String dilution_status
    }

    @Data
    static class WarehousingDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        //    入库库名-修改
        private String schema_id
        //数据表名
        private String table_id
        //列名
        private String column_id
        //数据类型
        private String data_type
        //启用状态-新增
        private String write_status
    }

    static Map<String, Map<String, TStreamRuleDTO>> createDto(ResultSet resBasic, ResultSet resAlarm, ResultSet resCalculation, ResultSet resCollection, ResultSet resDist, ResultSet resShoreBased, ResultSet resThinning, ResultSet resWarehousing) throws Exception {
        try {
            def createBasicDto = { dto, res ->
                dto.rule_id = res.getObject('rule_id') as Integer
                dto.sid = res.getObject('sid') as Integer
                dto.ship_id = res.getString('ship_id')
                dto.sys_id = res.getObject('sys_id') as Integer
                dto.cat_id = res.getObject('cat_id') as Integer
                dto.doss_key = res.getObject('doss_key') as Integer
                dto.protocol = res.getString('protocol')
                dto.name_chn = res.getString('name_chn')
                dto.name_eng = res.getString('name_eng')
                dto.orig_key = res.getString('orig_key')
                dto.data_from = res.getString('data_from')
                dto.unit = res.getString('unit')
                dto.transfer_factor = res.getBigDecimal('transfer_factor')
                dto.coefficient = res.getBigDecimal('coefficient')
                dto.status = res.getString('status')
                dto.value_type = res.getString('value_type')
                dto.value_min = res.getBigDecimal('value_min')
                dto.value_max = res.getBigDecimal('value_max')
                dto.inner_key = res.getString('inner_key')
            }
            def createAlarmDto = { dto, res ->
                dto.sid = res.getObject('sid') as Integer
                dto.doss_key = res.getObject('doss_key') as Integer
                dto.alert_min = res.getBigDecimal('alert_min')
                dto.alert_max = res.getBigDecimal('alert_max')
                dto.alert_2nd_min = res.getBigDecimal('alert_2nd_min')
                dto.alert_2nd_max = res.getBigDecimal('alert_2nd_max')
                dto.relate_stop_sig = res.getString('relate_stop_sig')
                dto.alert_status = res.getString('alert_status')
                dto.is_popup = res.getString('is_popup')
                dto.ams_alarm_standard = res.getObject('ams_alarm_standard') as Integer
                dto.alert_way = res.getString('alert_way')
            }
            def createCalculationDto = { dto, res ->
                dto.sid = res.getObject('sid') as Integer
                dto.doss_key = res.getObject('doss_key') as Integer
                dto.calculation_key = res.getString('calculation_key')
                dto.formula_flag = res.getString('formula_flag')
                dto.calculation_status = res.getString('calculation_status')
            }
            def createCollectionDto = { dto, res ->
                dto.sid = res.getObject('sid') as Integer
                dto.doss_key = res.getObject('doss_key') as Integer
                dto.colgroup = res.getString('colgroup')
                dto.modbus_slave_id = res.getObject('modbus_slave_id') as Integer
                dto.modbus_func_id = res.getObject('modbus_func_id') as Integer
                dto.addr = res.getString('addr')
                dto.ip_addr = res.getString('ip_addr')
//                dto.ip_addr_down = res.getString('ip_addr_down')
                dto.port_addr = res.getObject('port_addr') as Integer
                dto.from_table_id = res.getString('from_table_id')
                dto.from_column_id = res.getString('from_column_id')
                dto.protocol = res.getString('protocol')
                dto.col_freq = res.getObject('col_freq') as Double
                dto.topic = res.getString('topic')
                dto.modbus_sig_tag = res.getString('modbus_sig_tag')
                dto.col_interval = res.getObject('col_interval') as Double
                dto.col_count = res.getObject('col_count') as Integer
                dto.nmea_id = res.getObject('nmea_id') as Long
                dto.collection_status = res.getString('collection_status')
            }
            def createDistDto = { dto, res ->
                dto.sid = res.getObject('sid') as Integer
                dto.doss_key = res.getObject('doss_key') as Integer
                dto.dist_group = res.getString('dist_group')
                dto.dist_ip = res.getString('dist_ip')
                dto.dist_port = res.getObject('dist_port') as Integer
//                dto.dist_ip_addr_down = res.getString('dist_ip_addr_down')
                dto.dist_freq = res.getObject('dist_freq') as Double
                dto.dist_protocol = res.getString('dist_protocol')
//                dto.dis_user_and_password_down = res.getString('dis_user_and_password_down')
                dto.dist_user_and_password = res.getString('dist_user_and_password')
                dto.dist_status = res.getString('dist_status')
            }
            def createShoreBasedDto = { dto, res ->
                dto.sid = res.getObject('sid') as Integer
                dto.doss_key = res.getObject('doss_key') as Integer
                dto.to_shore_group = res.getString('to_shore_group')
                dto.to_shore_ip = res.getString('to_shore_ip')
                dto.to_shore_port = res.getObject('to_shore_port') as Integer
                dto.to_shore_freq = res.getObject('to_shore_freq') as Double
                dto.to_shore_protocol = res.getString('to_shore_protocol')
                dto.compress_type = res.getString('compress_type')
                dto.to_shore_status= res.getString('to_shore_status')
            }
            def createThinningDto = { dto, res ->
                dto.sid = res.getObject('sid') as Integer
                dto.doss_key = res.getObject('doss_key') as Integer
                dto.sparse_rate = res.getObject('sparse_rate') as Integer
                dto.dilution_type = res.getObject('dilution_type') as Integer
                dto.dilution_status = res.getSring('dilution_status')
            }
            def createWarehousingDto = { dto, res ->
                dto.sid = res.getObject('sid') as Integer
                dto.doss_key = res.getObject('doss_key') as Integer
                dto.schema_id = res.getString('schema_id')
                dto.table_id = res.getString('table_id')
                dto.column_id = res.getString('column_id')
                dto.data_type = res.getString('data_type')
                dto.write_status = res.getString('write_status')
            }
            Map<String, Map<String, TStreamRuleDTO>> TStreamRules = [:]
            //遍历基础表
            while (resBasic.next()) {
                TStreamRuleDTO basicDto = new TStreamRuleDTO()
                //基础表闭包调用
                createBasicDto.call(basicDto, resBasic)
                if (!TStreamRules.containsKey(basicDto.sid as String)) {
                    TStreamRules.put(basicDto.sid as String, [:])
                }
                TStreamRules.get(basicDto.sid as String)[basicDto.doss_key as String] = basicDto
            }
            //遍历报警表
            while (resAlarm.next()) {
                AlarmDTO alarmDto = new AlarmDTO()
                createAlarmDto.call(alarmDto, resAlarm)
                TStreamRules.get(alarmDto.sid as String)?.get(alarmDto.doss_key as String)?.alarm?.add(alarmDto)
            }
            //遍历计算表
            while (resCalculation.next()) {
                CalculationDTO calculationDto = new CalculationDTO()
                createCalculationDto.call(calculationDto, resCalculation)
                TStreamRules.get(calculationDto.sid as String)?.get(calculationDto.doss_key as String)?.calculation?.add(calculationDto)
            }
            //遍历采集表
            while (resCollection.next()) {
                CollectionDTO collectionDto = new CollectionDTO()
                createCollectionDto.call(collectionDto, resCollection)
                TStreamRules.get(collectionDto.sid as String)?.get(collectionDto.doss_key as String)?.collection?.add(collectionDto)
            }
            //遍历第三方分发表
            while (resDist.next()) {
                DistDTO distDto = new DistDTO()
                createDistDto.call(distDto, resDist)
                TStreamRules.get(distDto.sid as String)?.get(distDto.doss_key as String)?.other_distributions?.add(distDto)
            }
            //遍历岸基分发表
            while (resShoreBased.next()) {
                ShoreBasedDTO shoreBasedDto = new ShoreBasedDTO()
                createShoreBasedDto.call(shoreBasedDto, resShoreBased)
                TStreamRules.get(shoreBasedDto.sid as String)?.get(shoreBasedDto.doss_key as String)?.shore_based_distributions?.add(shoreBasedDto)
            }
            //遍历抽稀表
            while (resThinning.next()) {
                ThinningDTO thinningDto = new ThinningDTO()
                createThinningDto.call(thinningDto, resThinning)
                TStreamRules.get(thinningDto.sid as String)?.get(thinningDto.doss_key as String)?.thinning?.add(thinningDto)
            }
            //遍历入库表
            while (resWarehousing.next()) {
                WarehousingDTO warehousingDto = new WarehousingDTO()
                createWarehousingDto.call(warehousingDto, resWarehousing)
                TStreamRules.get(warehousingDto.sid as String)?.get(warehousingDto.doss_key as String)?.warehousing?.add(warehousingDto)
            }
            TStreamRules
        } catch (Exception e) {
            throw new Exception("TStreamRuleDTO createDto has an error", e)
        }
    }
}