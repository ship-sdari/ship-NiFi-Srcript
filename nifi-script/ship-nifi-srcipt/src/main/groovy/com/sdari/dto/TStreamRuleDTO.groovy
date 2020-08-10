package com.sdari.dto

class TStreamRuleDTO {
    //  配置规则号
    private int ruleId;
    // 船id
    private int sId;
    //船舶ID
    private String shipId;

    //系统ID
    private int sysId;

    // DOSS系统key值
    private int dossKey;

    // 信号中文名
    private String nameChn;

    //信号英文名
    private String nameEng;

    // 原始key值
    private String origKey;

    //  参与指标计算所转换的key值
    private String calculationKey;

    // 采集组编号
    private String colgroup;

    //SlaveID从站编号
    private int modBusSlaveId;

    //modbus操作功能
    private int modBusFuncId;

    // 主题
    private String topic;

    // modbus信号标签
    private String ModBusSigTag;


    //modbus寄存器地址
    private String modBusAddr;

    //数据来源IP地址
    private String ipAddr;

    //    用于链路中断暂存字段
    private String ipaddrdown;

    //端口号
    private String portAddr;

    //     数据来源标志位
    private String dataFrom;

    //    来源表名
    private String fromtableid;

    //   来源列名
    private String fromcolumnid;

    //    入库库名
    private String schema;

    //数据表名
    private String tableId;

    //列名
    private String columnId;

    //数据类型
    private String datatype;

    //单位
    private String unit;

    //量纲转换因子
    private BigDecimal transferFactor;

    //系数
    private double coefficient;

    //通讯协议
    private String protoCal;

    //采样频率
    private String colFreq;

    //船岸传输组
    private String toShoreGroup;

    //数据包请求间隔
    private String colInterval;

    //数据包请求数量
    private int colcount;

    //船岸传输目的IP
    private String toShoreIpAddr;

    //船岸传输频率
    private String toShoreFreq;

    //船岸传输协议
    private String toShoreProtoCal;

    //岸基压缩方式
    private String compressType;

    //数据分发分组
    private String distGroup;

    //数据分发目的IP
    private String distIpAddr;

    //    链路中断暂存字段
    private String distipaddrdown;

    //  数据分发频率
    private String distfreq;

    //数据分发协议
    private String distProtoCal;

    //  用于SFTP分发的用户名和密码
    private String disUserAndPasswordDown;

    //    链路中断暂存字段
    private String disUserAndPassword;

    //启用状态
    // A - 活跃
    // S - 暂时禁用
    // D - 删除"
    private String status = "A";

    //开关量/模拟量
    private String valueType;

    //量程最小值
    private BigDecimal valueMin;

    //量程最小值
    private BigDecimal valueMax;


    // 报警最大值，最小值范围
    private BigDecimal alertMin;
    private BigDecimal alertMax;
    private BigDecimal alert2ndMin;
    private BigDecimal alert2ndMax;

    //关联设备状态标志位
    private String relatesTopSig;

    //指标名称标志位
    private String Formulagrp;

    //    报警启用状态字段
    private String AlertStatus;

    // 应用名称
    private String Innerkey;

    //    报警是否弹窗
    private String IsPopup;

    //    抽稀频率
    private String sparseRate;

    //    开关量标识
    private String dataFlag;

    //1.求累计 2.求平均 3.只取点
    private int dilutionType;

    //nema0183_config关联id
    private Long nmeaId;

    //    报警方式
    private String alertWay;

    //输入时间
    private String InputTime;

    //输入用户
    private String InputUser;

    //最后更新时间
    private String LastModifyTime;

    //最后更新用户
    private String LastModifyUser;

    TStreamRuleDTO t() {
        TStreamRuleDTO t = new TStreamRuleDTO();
        t.dossKey = 11
        t.nameChn = "sda"
        println("tt  " + t.status)

        return t
    }
}