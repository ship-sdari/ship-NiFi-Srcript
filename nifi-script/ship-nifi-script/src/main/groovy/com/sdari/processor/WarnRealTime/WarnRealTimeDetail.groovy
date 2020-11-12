package com.sdari.processor.WarnRealTime

import com.alibaba.fastjson.JSONObject
import groovy.sql.Sql
import lombok.Data
import lombok.Getter
import org.apache.nifi.logging.ComponentLog

import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 实时报警实现详细
 */
class WarnRealTimeDetail {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static GroovyObject helper
    private static DataMetricService service

    WarnRealTimeDetail(final ComponentLog logger, final int pid, final String pName, final int rid, GroovyObject pch) {
        log = logger
        processorId = pid
        processorName = pName
        routeId = rid
        currentClassName = this.class.canonicalName
        helper = pch
        service = new DataMetricService()
        log.info "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 初始化成功！"
    }

    static def calculation(params) {
        if (null == params) return null
        def returnMap = [:]
        def dataListReturn = []
        def attributesListReturn = []
        final List<JSONObject> dataList = (params as HashMap).get('data') as ArrayList
        final List<JSONObject> attributesList = ((params as HashMap).get('attributes') as ArrayList)
        final Map<String, Map<String, GroovyObject>> rules = (helper?.invokeMethod('getTStreamRules', null) as Map<String, Map<String, GroovyObject>>)
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            try {
                //详细处理流程
                final JSONObject jsonDataFormer = (dataList.get(i) as JSONObject)
                final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
                //循环单条数据
                String sid = jsonAttributesFormer.get('sid') as String
                String colTime = jsonAttributesFormer.get('coltime') as String
                List<DataPointDTO> points = new ArrayList<>()
                for (String dossKey in jsonDataFormer.keySet()) {
                    try {
                        GroovyObject dossRule = rules?.get(sid)?.get(dossKey)
                        if (!service.metrics.containsKey(dossKey as Integer) && (dossRule?.getProperty('alarm') as List)?.size() > 0) {
                            //如果报警抽象类不存在并且存在规则，则加载报警抽象类和规则
                            if (!service.InitMetric(dossRule)) continue
                        }
                        //开始封装报警检查点
                        DataPointDTO point = new DataPointDTO()
                        point.timestamp = Instant.parse(colTime)
                        point.dossKey = Integer.parseInt(dossKey)
                        point.value = jsonDataFormer.getBigDecimal(dossKey)
                        points.add(point)
                    } catch (Exception e) {
                        log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName} dossKey = ${dossKey}] 报警检查准备过程异常", e
                    }
                }
                Map<String, List<Object>> events = new HashMap<>()
                //同步屏蔽信号以及同步实时报警
                service.lockAlarmAndShield(events, (helper?.invokeMethod('getMysqlPool', null) as Map)?.get('mysql.connection.doss_i') as Sql, Instant.parse(colTime))
                //提交报警异步/同步检查
                service.addPoints(points, events)
                //单条数据处理结束，放入返回仓库
                dataListReturn.add(events)
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

    /**
     * 信号度量调用实现类，包括初始化度量、数据加载调用（异/同步）、检查调用（异/同步）
     */
    class DataMetricService {
        //度量总仓库
        Map<Integer, DataMetricRealDTO> metrics = new ConcurrentHashMap<>()
        //屏蔽列表
        List<String> shieldKeys = new ArrayList<>()
        //当前报警总条数（包含已经屏蔽的）
        Integer alarmTotal = 0
        //当前报警总条数（不包含已经屏蔽的）
        Integer realTotal = 0
        //自定义报警入参封装对象
        Binding binding

        /**
         * 当前报警次数统计
         * @param i
         * @return
         */
        def changeAlarmTotal(Integer i, Integer flag) {
            synchronized (this) {
                if (0 == flag) {//屏蔽
                    this.alarmTotal = this.alarmTotal + i
                } else if (1 == flag) {//未屏蔽
                    this.realTotal = this.realTotal + i
                    this.alarmTotal = this.alarmTotal + i
                }
            }
        }

        def lockAlarmAndShield(Map<String, List<Object>> events, Sql con, Instant colTime) throws Exception {
            if (null == con) throw new Exception("没有获取到可用MySQL连接！")
            //同步屏蔽信号
            con.eachRow("SELECT CONCAT(`dossKey`,`alarm_type`,`alarm_desc`) FROM `t_alarm_shield` WHERE `status` = ? GROUP BY `dossKey`,`alarm_type`,`alarm_desc`;", [0]) { row ->
                this.shieldKeys.add(row[0] as String)
            }
            //同步实时报警（将程序中报警同步到表中）为了避免异步执行导致的数据不同步风险
            //同步条件：整十分钟同步、实时报警总条数不一致时同步
            if (colTime.getEpochSecond() % 60 * 10) {
                Integer count = con.firstRow("SELECT COUNT(1) FROM `t_alarm_real` WHERE `shield_status` = ?;", [1])[0] as Integer
                if (null != realTotal && null != count && realTotal == count) {//程序状态与表存储不一致
                    for (DataMetricRealDTO metric in this.metrics.values()) {
                        DataMetricRealDTO.RealTimeAlarmEventDTO dto = metric.alarmLock()
                        if (null == dto) continue
                        if (!events.containsKey(DataMetricRealDTO.AlarmEventTypeEnum.TYPE_SEVEN.type)) {
                            events.put(DataMetricRealDTO.AlarmEventTypeEnum.TYPE_SEVEN.type, new ArrayList<>())
                        }
                        events.get(DataMetricRealDTO.AlarmEventTypeEnum.TYPE_SEVEN.type).add(dto)
                    }
                }
            }
        }
        /**
         * @param points 信号仓库
         * @param events 事件仓库
         */
        def addPoints(ArrayList<DataPointDTO> points, Map<String, List<Object>> events) {
            //第一遍循环 数据加载 同步或异步分配
            for (DataPointDTO point in points) {
                try {
                    DataMetricRealDTO metric = metrics.get(point.dossKey)
                    if (null != metric) {
                        metric.putDataPoint(point)//数据加载
                    }
                } catch (Exception e) {
                    log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName} dossKey = ${point.dossKey}] 报警检查数据加载异常", e
                }
            }
            //第二遍循环 报警检查 同步或异步分配
            for (DataPointDTO point in points) {
                try {
                    DataMetricRealDTO metric = metrics.get(point.dossKey)
                    if (null != metric) {
                        //检查并事件入仓
                        Map<String, Object> event = metric.checkDataPoint(point)
                        for (String type in event.keySet()) {
                            if (!events.containsKey(type)) {
                                events.put(type, new ArrayList<Object>())
                            }
                            events.get(type).add(event.get(type))
                        }
                    }
                } catch (Exception e) {
                    log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName} dossKey = ${point.dossKey}] 报警检查提交过程异常", e
                }
            }
        }

        boolean InitMetric(GroovyObject dossRule) {
            boolean ret = false
            Integer dossKey
            try {
                //度量类创建
                DataMetricRealDTO metric = new DataMetricRealDTO()
                //报警规则生成
                AlarmRuleDTO rule = new AlarmRuleDTO()
                //来源基础表
                rule.ship_id = dossRule.getProperty('ship_id')
                rule.sys_id = dossRule.getProperty('sys_id') as Integer
                rule.orig_key = dossRule.getProperty('orig_key')
                rule.unit = dossRule.getProperty('unit')
                rule.col_freq = Duration.ofMillis((dossRule.getProperty('collection') as List).last()['col_freq'] as Long)
                rule.name_chn = dossRule.getProperty('name_chn')
                rule.name_eng = dossRule.getProperty('name_eng')

                dossKey = (dossRule.getProperty('doss_key') as Integer)
                for (alarm in dossRule.getProperty('alarm') as List) {
                    AlarmRuleDTO.Rule ar = new AlarmRuleDTO.Rule()
                    //来源报警规则表
                    ar.id = alarm['id'] as Integer
                    ar.sid = alarm['sid'] as Integer
                    ar.doss_key = alarm['doss_key'] as Integer
                    ar.rule_name = alarm['rule_name']
                    ar.rule_type = alarm['rule_type'] as Integer
                    ar.allow_suspense = alarm['rule_type'] == 1
                    ar.expr = alarm['expr']
                    ar.gen_duration = Duration.ofMillis(alarm['gen_duration'] as Long)
                    ar.gen_count = alarm['gen_count'] as Integer
                    ar.ignore_duration = Duration.ofMillis(alarm['ignore_duration'] as Long)
                    ar.recover_duration = Duration.ofMillis(alarm['recover_duration'] as Long)
                    ar.recover_count = alarm['recover_count'] as Integer
                    ar.alarm_name_chn = alarm['alarm_name_chn']
                    ar.alarm_name_eng = alarm['alarm_name_eng']
                    ar.alarm_source = alarm['alarm_source']
                    ar.alarm_level = alarm['alarm_level'] as Integer
                    ar.alert_min = alarm['alert_min'] as BigDecimal
                    ar.alert_max = alarm['alert_max'] as BigDecimal
                    ar.alert_2nd_min = alarm['alert_2nd_min'] as BigDecimal
                    ar.alert_2nd_max = alarm['alert_2nd_max'] as BigDecimal
                    ar.relate_stop_sig = alarm['relate_stop_sig']
                    ar.alert_status = alarm['alert_status'] == 'A'
                    ar.is_popup = alarm['is_popup'] == 'A'
                    ar.ams_alarm_standard = alarm['ams_alarm_standard'] as Integer
                    ar.alert_way = alarm['alert_way']
                    //单独生成上层的min_max
                    if (null == rule.min_max && (null != ar.alert_min || null != ar.alert_2nd_min
                            || null != ar.alert_2nd_max || null != ar.alert_max)) {
                        rule.min_max = (ar.alert_min == null ? 'null' : ar.alert_min) + '/'
                        +(ar.alert_2nd_min == null ? 'null' : ar.alert_2nd_min) + ' ~ '
                        +(ar.alert_2nd_max == null ? 'null' : ar.alert_2nd_max) + '/'
                        +(ar.alert_max == null ? 'null' : ar.alert_max)
                    }
                    //如果是自定义表达式报警 则加载GroovyShell
                    if (ar.rule_type == DataMetricRealDTO.AlarmTypeVO.TYPE_CUSTOM
                            && null != ar.expr && !ar.expr.isEmpty()) {
                        if (null == binding) {
                            binding = new Binding()
                            binding.setVariable("DataMetricService", this)
                        }
                        ar.exprGroovy = new GroovyShell(this.binding)
                    }
                    //暂存报警规则
                    rule.rules.addAll(ar)
                }
                //度量类初始化
                metric.dossKey = dossKey
                metric.metric = this
                metric.alertRules = rule
                //暂存度量类
                metrics[dossKey] = metric
            } catch (Exception e) {
                log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName} dossKey = ${dossKey}] 加载报警规则异常", e
            } finally {
                return ret
            }
        }
    }


    @Data
    static class DataPointDTO {
        Instant timestamp
        Integer dossKey
        BigDecimal value
    }

    @Data
    static class AlarmRuleDTO {
        List<Rule> rules = new ArrayList<>()
        //往下为新增应用型字段

        //船名
        String ship_id
        //系统id
        Integer sys_id
        //原始键
        String orig_key
        //单位
        String unit
        //采集频率
        Duration col_freq
        //中文名称
        String name_chn
        //英文名称
        String name_eng
        //量程范围
        String min_max

        //内部统计，当前信息
        AlarmInfo currentAlarmInfo = new AlarmInfo()
        //内部统计，四个参数保持同步状态()
        AlarmInfo lastAlarmInfo = new AlarmInfo()
        //内部统计，第一次信息
        AlarmInfo firstAlarmInfo = new AlarmInfo()

        Instant stopShieldTime//确认功能（短暂屏蔽截至时间）

        @Data
        class AlarmInfo {
            Boolean alerted = false//true - 已经产生报警 false - 正常状态/默认
            Integer count//次数
            Instant alarmTime//报警时间
            Integer alarmType//报警类型
            String alarmDescChn//报警描述-中文
            String alarmDescEng//报警描述-英文
            BigDecimal alarmValue//报警值
            BigDecimal alarmSetValue//报警设定值
            Integer shieldStatus//屏蔽状态
        }

        @Data
        class Rule {
            Integer id
            // 船id
            Integer sid
            // DOSS系统key值
            Integer doss_key
            // 规则名称
            String rule_name
            // 规则类型/报警类型
            Integer rule_type
            // 是否允许临时屏蔽
            Boolean allow_suspense
            // 规则表达式
            String expr
            // 报警窗口长度
            Duration gen_duration
            // 报警窗口统计次数
            Integer gen_count
            // 忽略窗口长度
            Duration ignore_duration
            // 恢复窗口长度
            Duration recover_duration
            // 恢复窗口统计次数
            Integer recover_count
            // 报警名称-中文
            String alarm_name_chn
            // 报警名称-英文
            String alarm_name_eng
            //  报警来源
            String alarm_source
            // 报警等级
            Integer alarm_level
            // 报警最大值，最小值范围
            BigDecimal alert_min
            BigDecimal alert_max
            BigDecimal alert_2nd_min
            BigDecimal alert_2nd_max
            //关联设备状态标志位
            String relate_stop_sig
            // 报警启用状态字段
            Boolean alert_status
            // 报警是否弹窗
            Boolean is_popup
            // AMS报警报警参考
            Integer ams_alarm_standard
            // 报警方式
            String alert_way
            //自定义报警调用对象
            GroovyShell exprGroovy

            private Duration getMaxWindow() {
                Duration max = gen_duration
                if (max < recover_duration) {
                    max = recover_duration
                }
                if (max < ignore_duration) {
                    max = ignore_duration
                }
                return max
            }
        }
    }

    /**
     * @author jinkaisong@sdari.mail.com
     * @date 2020/4/08 17:18
     * @desc 实时报警每一个信号点对应的抽象对象
     */
    @Data
    static class DataMetricRealDTO {
        //信号点键
        private Integer dossKey

        //service层 指针
        private DataMetricService metric

        //最新屏蔽键列表 指针
//        private List<String> shieldKeys

        // 报警规则列表 指针
        private AlarmRuleDTO alertRules

        //度量仓库，可以用来寻找其它信号源 指针
//        Map<Integer, DataMetricRealDTO> metrics

        // datapoints 由于是实时报警所以容量为1
        private ArrayList<BigDecimal> values = new ArrayList<>(1)
        private ArrayList<Instant> timestamps = new ArrayList<>(1)

        /**
         * 数据装载
         * @param point
         * @throws Exception
         */
        private void push(DataPointDTO point) throws Exception {
            values.set(0, point.value)
            timestamps.set(0, point.timestamp)
        }

        /**
         * 数据检查
         * @param point
         * @param returnType
         * @throws Exception
         */
        private void check(DataPointDTO point, Map<String, Object> returnType) throws Exception {
            for (AlarmRuleDTO.Rule rule : this.alertRules.rules) {
                if (rule.alert_status && values.size() > 0) {//报警状态开启并且有数据
                    if (null == this.alertRules.stopShieldTime) {//当前信号无确认，报警检查
                        alertCheck(rule, point, returnType)
                    } else if (point.timestamp.isAfter(this.alertRules.stopShieldTime)) {
                        //当前信号确认时间段已经度过，还原确认截至时间并继续报警检查
                        this.alertRules.stopShieldTime = null
                        alertCheck(rule, point, returnType)
                    } else {//当前信号处于确认时间段内，则进行恢复检查
                        recoverCheck(point, returnType)
                    }
                } else {//报警状态关闭或者无数据加载进来，则进行恢复检查
                    recoverCheck(point, returnType)
                }
            }
        }

        /**
         * 超限报警检查
         * @param rtard RealTimeAlarmRuleDTO
         * @param dataPoint dataPoint
         * @param alarmType 报警类别
         * @return 信号报警事件RealTimeAlarmEventDTO* @throws Exception
         */
        private RealTimeAlarmEventDTO checkOverflow(final AlarmRuleDTO.Rule rule, final DataPointDTO point) throws Exception {
            RealTimeAlarmEventDTO rtaed = null
            if (null != point.value && null != rule.alert_min && point.value < rule.alert_min) {//偏小
                rtaed = new RealTimeAlarmEventDTO()//信号报警事件
                updateRealTimeAlarmEventDTO(rtaed, rule, point, rule.alert_min, "过小", " is too small")
            } else if (null != point.value && null != rule.alert_max && point.value > rule.alert_max) {
                //偏大
                rtaed = new RealTimeAlarmEventDTO()//信号报警事件
                updateRealTimeAlarmEventDTO(rtaed, rule, point, rule.alert_max, "过大", " is too large")
            }
            return rtaed
        }

        /**
         * AMS报警检查
         * @param rtard RealTimeAlarmRuleDTO
         * @param dataPoint dataPoint
         * @param alarmType 报警类别
         * @return 信号报警事件RealTimeAlarmEventDTO* @throws Exception
         */
        private RealTimeAlarmEventDTO checkAMS(final AlarmRuleDTO.Rule rule, final DataPointDTO point) throws Exception {
            if (null != point.value && null != rule.ams_alarm_standard && point.value.intValue() == rule.ams_alarm_standard) {
                //开关量等于报警标准
                RealTimeAlarmEventDTO rtaed = new RealTimeAlarmEventDTO()//信号报警事件
                updateRealTimeAlarmEventDTO(rtaed, rule, point, point.value, "AMS报警", " is ams alert")
                return rtaed
            }
            return null
        }

        /**
         * 空值报警检查
         * @param rtard RealTimeAlarmRuleDTO
         * @param dataPoint dataPoint
         * @param alarmType 报警类别
         * @return 信号报警事件RealTimeAlarmEventDTO* @throws Exception
         */
        private RealTimeAlarmEventDTO checkNull(final AlarmRuleDTO.Rule rule, final DataPointDTO point) throws Exception {
            if (point.getValue() == null) {
                RealTimeAlarmEventDTO rtaed = new RealTimeAlarmEventDTO()//信号报警事件
                updateRealTimeAlarmEventDTO(rtaed, rule, point, null, "是空值", " is null")
                return rtaed
            }
            return null// 如果不空值则返回null事件
        }

        /**
         * 自定义报警检查
         * @param rtard RealTimeAlarmRuleDTO
         * @param dataPoint dataPoint
         * @param alarmType 报警类别
         * @return 信号报警事件RealTimeAlarmEventDTO* @throws Exception
         */
        private RealTimeAlarmEventDTO checkCustom(final AlarmRuleDTO.Rule rule, final DataPointDTO point) throws Exception {
            if (null != rule.exprGroovy) {
                if (rule.exprGroovy.evaluate(rule.expr)) {//有自定义报警
                    RealTimeAlarmEventDTO rtaed = new RealTimeAlarmEventDTO()//信号报警事件
                    updateRealTimeAlarmEventDTO(rtaed, rule, point, null, "自定义报警", " is custom alarm")
                    return rtaed
                }
            } else {
                throw new Exception("自定义表达式不具有执行状态！")
            }
            return null// 如果不报警则返回null事件
        }

        /**
         * 更新信号报警实体
         *
         * @param rtaed RealTimeAlarmEventDTO
         * @param rtard RealTimeAlarmRuleDTO
         * @param dataPoint DataPointDTO
         * @param alarmType 报警类别
         * @param alarmSetValue 报警设定值
         * @param chPostfix 报警中文描述后缀
         * @param enPostfix 报警英文描述后缀
         */
        private void updateRealTimeAlarmEventDTO(RealTimeAlarmEventDTO rtaed, final AlarmRuleDTO.Rule rule, final DataPointDTO point, final BigDecimal alarmSetValue, final String chPostfix, final String enPostfix) throws Exception {
            rtaed.alarm_type = (rule.rule_type as String)
            rtaed.dossKey = (point.dossKey as String)
            rtaed.outer_key = this.alertRules.orig_key
            rtaed.cn_name = this.alertRules.name_chn
            rtaed.en_name = this.alertRules.name_eng
            rtaed.min_max = this.alertRules.min_max
            rtaed.alarm_set_value = alarmSetValue
            rtaed.alarm_desc = (this.alertRules.name_chn + chPostfix)
            rtaed.alarm_en_desc = (this.alertRules.name_eng + enPostfix)
            rtaed.alarm_value = point.value
            rtaed.unit = this.alertRules.unit
            rtaed.info_app = ("")
            rtaed.create_time = instantFormat(point.timestamp)
            rtaed.shield_status = (1)//屏蔽状态默认为1,未屏蔽
            //20200701 添加最后报警时间
            rtaed.last_alarm_time = instantFormat(point.timestamp)
        }

        private void alertCheck(AlarmRuleDTO.Rule rule, DataPointDTO point, Map<String, Object> returnType) throws Exception {
            RealTimeAlarmEventDTO rtaed = null//报警事件
            //根据当前规则的报警类型检查
            switch (rule.rule_type) {
                case AlarmTypeVO.TYPE_NULL://空值检查
                    rtaed = checkNull(rule, point)
                    break
                case AlarmTypeVO.TYPE_OVER_LIMIT://超限检查
                    rtaed = checkOverflow(rule, point)
                    break
                case AlarmTypeVO.TYPE_AMS://AMS检查
                    rtaed = checkAMS(rule, point)
                    break
                case AlarmTypeVO.TYPE_CUSTOM://自定义规则表达式检查
                    rtaed = checkCustom(rule, point)
                    break
                default:
                    throw new Exception("未知的报警类型：${rule.rule_type}")
            }
            //判断该信号点是否报警，最多只有一条告警信息
            if (null != rtaed) {//有告警
                //2.判断是否需要输出新报警事件
                decideIsOutPutNewEvent(rtaed, point, rule, returnType)
                //3.判断是否需要推送实时报警（是否弹窗）输出类型 -> 类型五
                decideIsOutPutPopupEvent(rtaed, rule, returnType, "realTimeAlarm")
            } else {//无告警
                //恢复检查方法
                recoverCheck(point, returnType)
            }
        }

        /**
         * 输出类型五
         *
         * @param rtaed RealTimeAlarmEventDTO
         * @param rtard RealTimeAlarmRuleDTO
         * @param returnType 返回类型列表
         * @param method 方法标志类型
         * @param type 类型
         */
        private void decideIsOutPutPopupEvent(RealTimeAlarmEventDTO rtaed, AlarmRuleDTO.Rule rule, Map<String, Object> returnType, final String type) {
            if (rule.is_popup && rtaed.shield_status == 1) {//非屏蔽状态并且配置弹窗选项为true
                //t_news内容
                Popup.TNewsDTO tnd = new Popup.TNewsDTO()
                tnd.sid = rule.sid
                tnd.create_time = rtaed.create_time
                tnd.record_time = rtaed.create_time
                String stringBuffer = "报警类型：" + rtaed.cn_name + "\n" +
                        "报警描述：" + rtaed.alarm_desc
                tnd.content = stringBuffer
                tnd.type = 4
                tnd.status = 1
                //报警推送内容
                Popup.PopupDTO popup = new Popup.PopupDTO()
                popup.alarmDescription = rtaed.alarm_desc
                popup.alarmType = rtaed.alarm_type
                popup.infoApp = rtaed.info_app
                popup.outerKey = rtaed.outer_key
                popup.dossKey = rtaed.dossKey
                popup.outerName = rtaed.cn_name
                popup.outerEnName = rtaed.en_name
                popup.alarmTotal = this.metric.realTotal
                JSONObject json = new JSONObject()
                JSONObject jsonNewAndPopup = new JSONObject()
                json.put("type", type)
                json.put("data", popup)
                json.put("sid", rule.sid)
                json.put("method", rule.rule_type)
                jsonNewAndPopup.put("news", tnd)//加入消息表
                jsonNewAndPopup.put("popup", json)//加入弹窗消息
                addReturnType(AlarmEventTypeEnum.TYPE_FIVE.type, jsonNewAndPopup, returnType)
            }
        }

        /**
         * 类型一(报警类型转变)：alarm_history-插入,alarm_real-删除,alarm_real-插入
         * 类型二（新报警事件）：alarm_real-插入
         * 类型三（屏蔽信号点报警次数累加）：alarm_shields-更新
         * 类型四（报警事件恢复）：alarm_history-插入,alarm_real-删除
         *
         * @param rtaed RealTimeAlarmEventDTO
         * @param dataPoint DataPointDTO
         * @param rtard RealTimeAlarmRuleDTO
         * @param returnType
         */
        private void decideIsOutPutNewEvent(RealTimeAlarmEventDTO rtaed, final DataPointDTO point, AlarmRuleDTO.Rule rule, Map<String, Object> returnType) {
            final String shieldKey = rule.doss_key + rule.rule_type + rtaed.alarm_desc
            boolean isUpdateLastStatus = true//是否更新最近状态，默认更新
            boolean isUpdateFirstStatus = false//是否更新最近状态，默认更新
            boolean isUpdateAlarmTotal = false//是否更新最近状态，默认更新
            //1.更新屏蔽状态
            if (this.metric.shieldKeys.contains(shieldKey)) {//信号这种报警处于屏蔽状态
                rtaed.shield_status = 0
            }
            //2.根据当前是否处于报警状态，判断类型一二事件是否有输出
            if (this.alertRules.firstAlarmInfo.alerted) {//之前有报警 -> 则判断时间连续性决定是否输出
                if (point.timestamp.isAfter(this.alertRules.lastAlarmInfo.alarmTime)) {
                    //时间顺序(向后跳跃也默认为顺序)//时间倒序，则不处理
                    //判断时间点是否向后跳跃,距离上一次报警时间的间隔小于10个频率及视为未跳跃(为了降低时间点误差的严格度,减少实时历史频繁转换的性能消耗)
                    boolean isJump = (point.timestamp.toEpochMilli() - this.alertRules.lastAlarmInfo.alarmTime.toEpochMilli()) > (10 * this.alertRules.col_freq.toMillis())
                    boolean isOutPutEvent = false
                    String outType = null
                    if (!isJump) {//时间点连续 -> 则比较报警类型和报警描述
                        if ((rtaed.alarm_type as Integer) != this.alertRules.lastAlarmInfo.alarmType || rtaed.alarm_desc != this.alertRules.lastAlarmInfo.alarmDescChn) {
                            isOutPutEvent = true
                            outType = AlarmEventTypeEnum.TYPE_ONE.type
                            if (rtaed.shield_status == 0) {
                                //只有在类型一或二产生时，才会输出类型三（累加屏蔽信号的报警次数）（为了解决更新次频繁的效率问题）
                                addReturnType(AlarmEventTypeEnum.TYPE_THREE.type, rtaed, returnType)
                            }
                            isUpdateFirstStatus = true
                            isUpdateAlarmTotal = true
                        } else {//时间点连续并且报警类型和报警描述相同 --> 检查当前报警值是否变化
                            if (null != rtaed.alarm_value && null != this.alertRules.lastAlarmInfo.alarmValue && rtaed.alarm_value != this.alertRules.lastAlarmInfo.alarmValue) {
                                //值更新
                                isOutPutEvent = true
                                outType = AlarmEventTypeEnum.TYPE_SIX.type
                                isUpdateFirstStatus = true
                            }
                        }
                    } else {//不连续(跳跃) -> 输出
                        isOutPutEvent = true
                        outType = AlarmEventTypeEnum.TYPE_ONE.type
                        if (rtaed.shield_status == 0) {
                            //只有在类型一或二产生时，才会输出类型三（累加屏蔽信号的报警次数）（为了解决更新次频繁的效率问题）
                            addReturnType(AlarmEventTypeEnum.TYPE_THREE.type, rtaed, returnType)
                        }
                        isUpdateFirstStatus = true
                        isUpdateAlarmTotal = true
                    }
                    //表事件输出与否，输出事件类型 -> 类型一（转历史并报实时）/类型六（报警值更新）
                    if (isOutPutEvent) {
                        addReturnType(outType, rtaed, returnType)
                    }
                } else {//时间倒序，则不更新内部统计
                    isUpdateLastStatus = false
                }
            } else {//之前无报警，则直接输出 -> 类型二
                isUpdateFirstStatus = true
                isUpdateAlarmTotal = true
                addReturnType(AlarmEventTypeEnum.TYPE_TWO.type, rtaed, returnType)
                if (rtaed.shield_status == 0) {
                    //只有在类型一或二产生时，才会输出类型三（累加屏蔽信号的报警次数）（为了解决更新次频繁的效率问题）
                    addReturnType(AlarmEventTypeEnum.TYPE_THREE.type, rtaed, returnType)
                }
            }
            //3.更新最近统计（每一个频率的信号点，不管如何都要更新，除了时间倒序情况外）
            if (isUpdateLastStatus) {
                this.alertRules.lastAlarmInfo.alerted = true
                this.alertRules.lastAlarmInfo.alarmTime = point.timestamp
                this.alertRules.lastAlarmInfo.alarmType = (rtaed.alarm_type as Integer)
                this.alertRules.lastAlarmInfo.alarmDescChn = rtaed.alarm_desc
                this.alertRules.lastAlarmInfo.alarmDescEng = rtaed.alarm_en_desc
                this.alertRules.lastAlarmInfo.alarmValue = rtaed.alarm_value
                this.alertRules.lastAlarmInfo.alarmSetValue = rtaed.alarm_set_value
                this.alertRules.lastAlarmInfo.shieldStatus = rtaed.shield_status
            }
            //4.更新最初统计（报警转变、报警产生、报警值变化、报警恢复时更新）
            if (isUpdateFirstStatus) {
                this.alertRules.firstAlarmInfo.alerted = true
                this.alertRules.firstAlarmInfo.alarmTime = point.timestamp
                this.alertRules.firstAlarmInfo.alarmType = (rtaed.alarm_type as Integer)
                this.alertRules.firstAlarmInfo.alarmDescChn = rtaed.alarm_desc
                this.alertRules.firstAlarmInfo.alarmDescEng = rtaed.alarm_en_desc
                this.alertRules.firstAlarmInfo.alarmValue = rtaed.alarm_value
                this.alertRules.firstAlarmInfo.alarmSetValue = rtaed.alarm_set_value
                this.alertRules.firstAlarmInfo.shieldStatus = rtaed.shield_status
            }
            //5.增加当前报警条数
            if (isUpdateAlarmTotal) {
                this.metric.changeAlarmTotal(1, rtaed.shield_status)
            }
        }

        /**
         * 报警恢复检查
         * @param dataPoint DataPointDTO
         * @param rtard rtard
         * @param returnType
         * @throws Exception
         */
        private void recoverCheck(final DataPointDTO point, Map<String, Object> returnType) throws Exception {
            if (this.alertRules.firstAlarmInfo.alerted) {//当前处于实时报警状态才需要恢复,输出 -> 类型四
                //输出 -> 类型四
                RealTimeAlarmEventDTO rtaed = new RealTimeAlarmEventDTO()
                rtaed.dossKey = String.valueOf(point.dossKey)
                rtaed.outer_key = this.alertRules.orig_key
                rtaed.create_time = instantFormat(point.timestamp)
                addReturnType(AlarmEventTypeEnum.TYPE_FOUR.type, rtaed, returnType)
                //更新当前报警总条数
                this.metric.changeAlarmTotal(-1, this.alertRules.lastAlarmInfo.shieldStatus)
                //取消最初实时报警事件状态
                this.alertRules.firstAlarmInfo.alerted = false
                this.alertRules.firstAlarmInfo.alarmTime = null
                this.alertRules.firstAlarmInfo.alarmType = null
                this.alertRules.firstAlarmInfo.alarmDescChn = null
                this.alertRules.firstAlarmInfo.alarmDescEng = null
                this.alertRules.firstAlarmInfo.alarmValue = null
                this.alertRules.firstAlarmInfo.alarmSetValue = null
                this.alertRules.firstAlarmInfo.shieldStatus = null
                //取消最近时间点预警状态
                this.alertRules.lastAlarmInfo.alerted = false
                this.alertRules.lastAlarmInfo.alarmTime = null
                this.alertRules.lastAlarmInfo.alarmType = null
                this.alertRules.lastAlarmInfo.alarmDescChn = null
                this.alertRules.lastAlarmInfo.alarmDescEng = null
                this.alertRules.lastAlarmInfo.alarmValue = null
                this.alertRules.lastAlarmInfo.alarmSetValue = null
                this.alertRules.lastAlarmInfo.shieldStatus = null
            }
        }

        private static void addReturnType(final String type, final Object o, Map<String, Object> returnType) {
            returnType.put(type, o)
        }

        private static String instantFormat(Instant instant) {
            return (instant as String).replace('T', ' ').replace('Z', '')
        }

        /**
         * 只限同步处理，输出事件类型如下：
         * 类型一(报警类型转变)：alarm_history-插入,alarm_real-删除,alarm_real-插入
         * 类型二（新报警事件）：alarm_real-插入
         * 类型三（屏蔽信号点报警次数累加）：alarm_shields-更新
         * 类型四（报警事件恢复）：alarm_history-插入,alarm_real-删除
         * 类型五（报警信息弹窗）：t_news插入,弹窗推送写入redis
         * 类型六 (实时报警值更新): alarm_real-更新
         * @param dataPoints DataPointDTO
         * @return 时间类型* @throws Exception
         */
        synchronized Map<String, Object> checkDataPoint(DataPointDTO dataPoints) throws Exception {
            Map<String, Object> returnType = new HashMap<>()//类型事件列表
            check(dataPoints, returnType)
            return returnType
        }

        /**
         * 信号点数据加载
         * @param dataPoint
         * @throws Exception
         */
        synchronized void putDataPoint(DataPointDTO dataPoint) throws Exception {
            push(dataPoint)
        }

        /**
         * 同步当前报警状态到数据库
         * 类型七 （同步报警）：alarm_real-清空,alarm_real-插入
         * @return
         * @throws Exception
         */
        synchronized RealTimeAlarmEventDTO alarmLock() throws Exception {
            if (this.alertRules.firstAlarmInfo.alerted) {
                RealTimeAlarmEventDTO rtaed = new RealTimeAlarmEventDTO()//信号报警事件
                rtaed.alarm_type = (this.alertRules.firstAlarmInfo.alarmType as String)
                rtaed.dossKey = (this.dossKey as String)
                rtaed.outer_key = this.alertRules.orig_key
                rtaed.cn_name = this.alertRules.name_chn
                rtaed.en_name = this.alertRules.name_eng
                rtaed.min_max = this.alertRules.min_max
                rtaed.alarm_set_value = this.alertRules.firstAlarmInfo.alarmSetValue
                rtaed.alarm_desc = (this.alertRules.firstAlarmInfo.alarmDescChn)
                rtaed.alarm_en_desc = (this.alertRules.firstAlarmInfo.alarmDescEng)
                rtaed.alarm_value = this.alertRules.firstAlarmInfo.alarmValue
                rtaed.unit = this.alertRules.unit
                rtaed.info_app = ("")
                rtaed.create_time = instantFormat(this.alertRules.firstAlarmInfo.alarmTime)
                rtaed.shield_status = (this.metric.shieldKeys.contains(rtaed.dossKey + rtaed.alarm_type + rtaed.alarm_desc) ? 0 : 1)
                //1,未屏蔽 0,屏蔽
                rtaed.last_alarm_time = instantFormat(this.alertRules.firstAlarmInfo.alarmTime)
                rtaed
            } else {
                null
            }
        }

        @Data
        class Popup {
            @Data
            class TNewsDTO {
                Integer sid//船号
                String content//内容
                Integer type//类别
                Integer status//状态
                String record_time//记录时间
                String create_time//创建时间
            }

            @Data
            class PopupDTO {
                String alarmType//类型
                String alarmDescription//描述
                String outerKey//原始key
                String dossKey//信号编号
                String infoApp//信号引用
                String outerName//信号名称
                String outerEnName//信号英文名称
                Integer alarmTotal//当前实时报警总条数
            }
        }


        /**
         * @author jinkaisong@sdari.mail.com
         * @date 2020/4/08 17:18
         */
        enum AlarmEventTypeEnum {
            TYPE_ONE("Type the one"),
            TYPE_TWO("Type the two"),
            TYPE_THREE("Type the three"),
            TYPE_FOUR("Type the four"),
            TYPE_FIVE("Type the five"),
            TYPE_SIX("Type the six"),
            TYPE_SEVEN("Type the seven");
            @Getter
            private String type

            private AlarmEventTypeEnum(String type) {
                this.type = type
            }

            AlarmEventTypeEnum valueToAlarmEventTypeEnum(String value) {
                AlarmEventTypeEnum[] enums = values()
                for (AlarmEventTypeEnum anEnum : enums) {
                    if (anEnum.type == value) {
                        return anEnum
                    }
                }
                return null
            }
        }

        @Data
        class AlarmTypeVO {
            //空值报警
            static final Integer TYPE_NULL = 0
            //超限报警
            static final Integer TYPE_OVER_LIMIT = 1
            //AMS报警
            static final Integer TYPE_AMS = 2
            //应用报警
            static final Integer TYPE_APPLY = 3
            //硬件或系统故障
            static final Integer TYPE_HARD_WARE_SYS = 4
            //自定义表达式报警
            static final Integer TYPE_CUSTOM = 5
        }

        @Data
        class RealTimeAlarmEventDTO {//与alarm_real表结构保持一致
            //报警类型n0/空值；1/超限；2/ams 3/应用/4系统或系统故障 5/自定义报警
            private String alarm_type
            //信号编号
            private String dossKey
            //信号原始key
            private String outer_key
            //信号中文名称
            private String cn_name
            //信号英文名称
            private String en_name
            //量程
            private String min_max
            //报警设定值
            private BigDecimal alarm_set_value
            //报警描述
            private String alarm_desc
            //英语报警描述
            private String alarm_en_desc
            //当前值
            private BigDecimal alarm_value
            //unit
            private String unit
            //信号应用
            private String info_app
            //0/屏蔽 1/未屏蔽
            private Integer shield_status
            //create_time
            private String create_time
            //last_alarm_time
            private String last_alarm_time
        }
    }
}
