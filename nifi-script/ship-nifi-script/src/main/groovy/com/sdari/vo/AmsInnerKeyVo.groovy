package com.sdari.vo

class AmsInnerKeyVo {



    /**
     * 船艏向(真北,度)
     */
    public static final String HDG = "hdg";

    /**
     * 航向(真北,度)
     */
    public static final String COG = "cog";

    /**
     * 相对风向
     */
    public static final String RWD = "rwd";

    /**
     * 相对风速
     */
    public static final String RWS = "rws";

    /**
     * 主机转速
     */
    public static final String ME_ECS_SPEED = "me_ecs_speed";
    /**
     * 主机1转速
     * 左轴转速平均值
     */
    public static final String shaft1_speed_avg = "shaft1_speed_avg";

    /**
     * 主机2转速
     * 右轴转速平均值
     */
    public static final String shaft2_speed_avg = "shaft2_speed_avg";

    /**
     * 纵向对地速度
     */
    public static final String VG_SL = "vg";

    /**
     * 主机使用重油指示
     */
    public static final String ME_USE_HFO = "me_use_hfo";
    /**
     * 主机1使用重油指示
     * 双机双桨
     */
    public static final String ME1_USE_HFO = "me1_use_hfo";
    /**
     * 主机2使用重油指示
     * 双机双桨
     */
    public static final String ME2_USE_HFO = "me2_use_hfo";
    /**
     * 主机使用柴油/轻柴油指示
     */
    public static final String ME_USE_MDO = "me_use_mdo";
    /**
     * 主机1使用柴油/轻柴油指示
     * 双机双桨
     */
    public static final String ME1_USE_MDO = "me1_use_mdo";
    /**
     * 主机2使用柴油/轻柴油指示
     * 双机双桨
     */
    public static final String ME2_USE_MDO = "me2_use_mdo";
    /**
     * 柴油发电机使用重油指示
     */
    public static final String GE_USE_HFO = "ge_use_hfo";
    /**
     * 柴油发电机使用柴油/轻柴油指示
     */
    public static final String GE_USE_MDO = "ge_use_mdo";
    /*
     * 锅炉用燃油
     */
    public static final String BOIL_USE_HFO = "boil_use_hfo";
    /*
     * 锅炉用柴油
     */
    public static final String BOIL_USE_MDO = "boil_use_mdo";
    /**
     * 纵向对水速度
     */
    public static final String VS = "vs";

    /**
     * 船舶横摇
     */
    public static final String ROLL = "roll";

    /**
     * 船舶横摇(艉部)
     */
    public static final String ROLLA = "rolla";


    public static final String PITCH = "pitch";

    /*
     * 船舶纵摇(艉部)
     */
    public static final String PITCHA = "pitcha";

    /*
     * 船舶升沉(艉部)
     */
    public static final String HEAVEA = "heavea";
    public static final String HEAVE = "heave";

    /**
     * 船舶横摇(艏部)
     */
    public static final String ROLLF = "rollf";
    /*
     * 船舶纵摇首部
     */
    public static final String PITCHF = "pitchf";
    /*
     * 船舶纵摇首部
     */
    public static final String HEAVEF = "heavef";


    /**
     * 船回转率(度/分)
     */
    public static final String ROT = "rot";

    /**
     * 首吃水液位
     */
    public static final String DF = "df";

    /**
     * 尾吃水液位
     */
    public static final String DA = "da";

    /**
     * 左中吃水液位
     */
    public static final String DMP = "dmp";
    /**
     * 右中吃水液位
     */
    public static final String DMS = "dms";
    /**
     * EEOI
     */
    public static final String EEOI = "eeoi";

    /**
     * 单位运输油耗
     */
    public static final String UNIT_TRANTSPORTATION_FUEL = "unit_trantsportation_fuel";

    /**
     * 单位距离油耗
     */
    public static final String UNIT_DISTANCE_FUEL = "unit_distance_fuel";

    /**
     * 舵角(度)
     */
    public static final String RUDDER_2 = "rudder";

    /**
     * 系统健康度
     */
    public static final String SYS_HEALTH = "sys_health";

    /**
     * 全球定位系统-纬度
     */
    public static final String LAT = "lat";

    /**
     * 全球定位系统-经度
     */
    public static final String LON = "lon";

    /**
     * 累计航行距离对地
     */
    public static final String DTG = "dtg";

    /**
     * 主机燃油/柴油消耗质量流量(进油管)
     */
    public static final String ME_FO_IN_RATE = "me_fo_in_rate";

    /**
     * 主机燃油/柴油消耗质量流量(回油管)
     */
    public static final String ME_FO_OUT_RATE = "me_fo_out_rate";

    /**
     * 柴油发电机燃油/柴油消耗质量流量（进）   辅机
     */
    public static final String GE_FO_IN_RATE = "ge_fo_in_rate";

    /**
     * 柴油发电机燃油/柴油消耗质量流量（出）  辅机
     */
    public static final String GE_FO_OUT_RATE = "ge_fo_out_rate";

    /**
     * 锅炉燃油进口质量流量（进）
     */
    public static final String BOIL_FO_IN_RATE = "boil_fo_in_rate";

    /**
     * 锅炉燃油进口质量流量（出）
     */
    public static final String BOIL_FO_OUT_RATE = "boil_fo_out_rate";

    /*
     * 轴功率(KW)
     */
    public static final String SHAFT_POWER = "shaft_power";

    /*
     * 轴扭矩(KNM)
     */
    public static final String SHAFT_TORQUE = "shaft_torque";
    /*
     * 轴转速
     */
    public static final String SHAFT_SPEED = "shaft_speed";


//    /*
//     * 进口质量流量计流速  主机
//     */
//    public static final String Mg_Oil_In = "mg_oil_in";
//    /*
//     * 进口质量流量计流速  辅机
//     */
//    public static final String M_Oil_Ae_In = "m_oil_ae_in";
//    /*
//     * 进口质量流量计流速  锅炉
//     */
//    public static final String Boiler_Oil_In = "boiler_oil_in";
//    /*
//     * 出口质量流量计流速  主机
//     */
//    public static final String Mg_Oil_Out = "mg_oil_out";
//    /*
//     * 出口质量流量计流速  辅机
//     */
//    public static final String M_Oil_Ae_Out = "m_oil_ae_out";
//    /*
//     * 出口质量流量计流速  锅炉
//     */
//    public static final String Boiler_Oil_Out = "boiler_oil_out";


    /*
     * 轉速
     */
    public static final String Shaft_Rpm = "shaft_rpm";

    /*
     * 水深
     */
    public static final String DBT = "dbt";
    /*
     * 尾管后轴承温度1
     */
    public static final String AFT_BEAR_T1 = "aft_bear_t1";
    /*
     * 尾管后轴承温度1
     */
    public static final String AFT_BEAR_T2 = "aft_bear_t2";

    /*
     * 辅机1功率
     */
    public static final String GE1_POWER = "ge1_power";
    /*
     * 辅机2功率
     */
    public static final String GE2_POWER = "ge2_power";
    /*
     * 辅机3功率
     */
    public static final String GE3_POWER = "ge3_power";
    /*
     * 辅机4功率
     */
    public static final String GE4_POWER = "ge4_power";

    /*
     * 辅机1转速
     */
    public static final String GE1_SPEED = "ge1_speed";
    /*
     * 辅机2转速
     */
    public static final String GE2_SPEED = "ge2_speed";
    /*
     * 辅机3转速
     */
    public static final String GE3_SPEED = "ge3_speed";
    /*
     * 辅机4转速
     */
    public static final String GE4_SPEED = "ge4_speed";
    /*
     * 燃料油储存舱（上）液位
     */
    public static final String HFO_TK1S_L = "hfo_tk1s_l";
    /*
     * 燃料油储存舱（下）液位
     */
    public static final String HFO_TK1S2_L = "hfo_tk1s2_l";
    /*
     * 燃料油储存舱（左）液位
     */
    public static final String HFO_TK2P_L = "hfo_tk2p_l";
    /*
     * 燃料油储存舱（右）液位
     */
    public static final String HFO_TK2S_L = "hfo_tk2s_l";


    public static final String ME_CYL1_EGAS_T = "me_cyl1_egas_t";
    public static final String ME_CYL2_EGAS_T = "me_cyl2_egas_t";
    public static final String ME_CYL3_EGAS_T = "me_cyl3_egas_t";
    public static final String ME_CYL4_EGAS_T = "me_cyl4_egas_t";
    public static final String ME_CYL5_EGAS_T = "me_cyl5_egas_t";
    public static final String ME_CYL6_EGAS_T = "me_cyl6_egas_t";
    public static final String ME_CYL7_EGAS_T = "me_cyl7_egas_t";


    public static final String ME_TC1_EGAS_IN_T = "me_tc1_egas_in_t";
    public static final String ME_TC2_EGAS_IN_T = "me_tc2_egas_in_t";

    public static final String ME_TC1_EGAS__T = "me_tc1_egas_t";
    public static final String ME_TC2_EGAS__T = "me_tc2_egas_t";


    public static final String ME_CYL1_JCW_OUT_T = "me_cyl1_jcw_out_t";
    public static final String ME_CYL2_JCW_OUT_T = "me_cyl2_jcw_out_t";
    public static final String ME_CYL3_JCW_OUT_T = "me_cyl3_jcw_out_t";
    public static final String ME_CYL4_JCW_OUT_T = "me_cyl4_jcw_out_t";
    public static final String ME_CYL5_JCW_OUT_T = "me_cyl5_jcw_out_t";
    public static final String ME_CYL6_JCW_OUT_T = "me_cyl6_jcw_out_t";
    public static final String ME_CYL7_JCW_OUT_T = "me_cyl7_jcw_out_t";


    public static final String ME_AC1_AIR_OUT_T = "me_ac1_air_out_t";
    public static final String ME_AC2_AIR_OUT_T = "me_ac2_air_out_t";

    public static final String ME_AC1_AIR_IN_T = "me_ac1_air_in_t";
    public static final String ME_AC2_AIR_IN_T = "me_ac2_air_in_t";

    /*
     * 主机估算负载
     */
    public static final String ME_ECS_POWER = "me_ecs_power";

    /*
     * ECA距离告警阈值
     */
    public static final String ECA_ALARM_DIST = "ECA_ALARM_DIST";

    /*
     * 废气锅炉出口蒸汽压力
     */
    public static final String EGB_STEAM_P = "egb_steam_p";

    /*
     * 燃油锅炉燃油压力
     */
    public static final String OFB_STEAM_P = "ofb_steam_p";

    /*
     * No.1 柴油发电机组增压器1进口废气温度
     */
    public static final String GE1_TC_EGAS_IN_T1 = "ge1_tc_egas_in_t1";

    /*
     * No.1 柴油发电机组增压器2进口废气温度
     */
    public static final String GE1_TC_EGAS_IN_T2 = "ge1_tc_egas_in_t2";


    /*
     * No.1 柴油发电机1号气缸出口废气温度
     */
    public static final String GE1_CYL1_EGAS_T = "ge1_cyl1_egas_t";
    /*
     * No.1 柴油发电机2号气缸出口废气温度
     */
    public static final String GE1_CYL2_EGAS_T = "ge1_cyl2_egas_t";
    public static final String GE1_CYL3_EGAS_T = "ge1_cyl3_egas_t";
    public static final String GE1_CYL4_EGAS_T = "ge1_cyl4_egas_t";
    public static final String GE1_CYL5_EGAS_T = "ge1_cyl5_egas_t";
    public static final String GE1_CYL6_EGAS_T = "ge1_cyl6_egas_t";

    public static final String ME_CYLC1_JCW_OUT_T = "me_cylc1_cw_out_t";
    public static final String ME_CYLC2_JCW_OUT_T = "me_cylc2_cw_out_t";
    public static final String ME_CYLC3_JCW_OUT_T = "me_cylc3_cw_out_t";
    public static final String ME_CYLC4_JCW_OUT_T = "me_cylc4_cw_out_t";
    public static final String ME_CYLC5_JCW_OUT_T = "me_cylc5_cw_out_t";
    public static final String ME_CYLC6_JCW_OUT_T = "me_cylc6_cw_out_t";
    public static final String ME_CYLC7_JCW_OUT_T = "me_cylc7_cw_out_t";

    /*
     * 安吉23  主机辅机锅炉进出口
     */
    public static final String ME_FO_IN_TOTAL = "me_fo_in_total";
    public static final String ME_FO_OUT_TOTAL = "me_fo_out_total";
    public static final String GE_FO_IN_TOTAL = "ge_fo_in_total";
    public static final String GE_FO_OUT_TATAL = "ge_fo_out_total";
    public static final String BOIL_FO_IN_TATAL = "boil_fo_in_total";
    public static final String BOIL_FO_OUT_TATAL = "boil_fo_out_total";
    /*
     * 43543艉管后轴承温度
     */
    public static final String AFT_BEAR_T = "aft_bear_t";

    /**
     * Python 默认值
     */
    public static final String Python_null = "{\"sum\": 0, \"sum_me\": 0, \"sum_me_pc\": 0, \"sum_ge\": 0, \"sum_ge_pc\":0, \"sum_br\": 0.0, \"sum_br_pc\": 0.0," +
            " \"me_other\": 0, \"me_other_pc\": 0, \"me_exhaust\": 0, \"me_exhaust_pc\": 0, \"me_propulsion\": 0, \"me_propulsion_pc\": 0, \"me_cooling\": 0, " +
            "\"me_cooling_pc\": 0, \"me_cooling_ac\":0, \"me_cooling_ac_pc\":0, \"me_cooling_jk\": 0, \"me_cooling_jk_pc\": 0, \"me_cooling_lu\": 0, \"me_cooling_lu_pc\": 0," +
            " \"me_exhaust_loss\": 0, \"me_exhaust_loss_pc\": 0, \"transfer\": 0, \"transfer_pc\": 0, \"transfer_loss\": 0, \"transfer_loss_pc\": 0.0, \"propulsion_loss\": 0," +
            " \"propulsion_loss_pc\": 0, \"me_turbo\": 0.0, \"me_turbo_pc\": 0.0, \"propulsion_end\": 0, \"propulsion_end_pc\": 0, \"ge_other1\": 0.0, \"ge_other_pc1\": 0.0, " +
            "\"ge_exhaust1\": 0.0, \"ge_exhaust_pc1\": 0.0, \"ge_propulsion1\": 0.0, \"ge_propulsion_pc1\": 0.0, \"ge_cooling1\": 0.0, \"ge_cooling_pc1\": 0.0, \"ge_cooling_ac1\": 0.0," +
            " \"ge_cooling_ac_pc1\": 0.0, \"ge_cooling_jk1\": 0.0, \"ge_cooling_jk_pc1\": 0.0, \"ge_cooling_lu1\": 0.0, \"ge_cooling_lu_pc1\": 0.0, \"ge_exhaust_loss1\": 0.0, " +
            "\"ge_exhaust_loss_pc1\": 0.0, \"ge_turbo1\": 0.0, \"ge_turbo_pc1\": 0.0, \"ge_other2\": 0.0, \"ge_other_pc2\": 0.0, \"ge_exhaust2\": 0.0, \"ge_exhaust_pc2\": 0.0," +
            " \"ge_propulsion2\": 0.0, \"ge_propulsion_pc2\": 0.0, \"ge_cooling2\": 0.0, \"ge_cooling_pc2\": 0.0, \"ge_cooling_ac2\": 0.0, \"ge_cooling_ac_pc2\": 0.0," +
            " \"ge_cooling_jk2\": 0.0, \"ge_cooling_jk_pc2\": 0.0, \"ge_cooling_lu2\": 0.0, \"ge_cooling_lu_pc2\": 0.0, \"ge_exhaust_loss2\": 0.0, \"ge_exhaust_loss_pc2\": 0.0, " +
            "\"ge_turbo2\": 0.0, \"ge_turbo_pc2\": 0.0, \"ge_other3\": 0, \"ge_other_pc3\": 0, \"ge_exhaust3\": 0, \"ge_exhaust_pc3\": 0, \"ge_propulsion3\": 0, \"ge_propulsion_pc3\": 0, " +
            "\"ge_cooling3\": 0, \"ge_cooling_pc3\": 0, \"ge_cooling_ac3\": 0, \"ge_cooling_ac_pc3\": 0, \"ge_cooling_jk3\": 0, \"ge_cooling_jk_pc3\": 0, \"ge_cooling_lu3\": 0, " +
            "\"ge_cooling_lu_pc3\": 0, \"ge_exhaust_loss3\": 0, \"ge_exhaust_loss_pc3\": 0, \"ge_turbo3\": 0, \"ge_turbo_pc3\": 0}";

    /*
     * 双机双桨版特有inner Key
     * */
    public static final String boost1_fo_total = "boost1_fo_total";//NO.1燃油供油单元总消耗
    public static final String ge_fo1_total = "ge_fo1_total";//NO.1燃油供油单元辅机总消耗
    public static final String ge_fo2_total = "ge_fo2_total";//NO.2燃油供油单元辅机总消耗
    public static final String shaft1_power_avg = "shaft1_power_avg";//左轴功率平均值
    public static final String shaft2_power_avg = "shaft2_power_avg";//右轴功率平均值
    public static final String boost2_fo_total = "boost2_fo_total";//NO.2燃油供油单元总消耗
    public static final String gross_ton = "gross_ton";//船舶总吨
    public static final String vg_gps = "vg_gps";//GPS航速
    public static final String Vs_log = "Vs_log";//计程仪对水航速
    public static final String Vsl = "Vsl";//船舶失速

}
