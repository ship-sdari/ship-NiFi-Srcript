package com.sdari.kpi

import com.sdari.vo.AmsInnerKeyVo

//package com.sdari.groovy.kpi



class AuxOilDTO {

    private int sid;
    private String indexGroup;
    private String Time_1;


    /**
     * 指标名称：
     * 辅机燃料消耗流量
     *
     * 指标DTO:aux_oil
     *
     * 公式：
     * NO.1燃油供油单元辅机总消耗+NO.2燃油供油单元辅机总消耗
     *
     * 关联inner key 说明:
     * ge_fo1_total:NO.1燃油供油单元总消耗
     * ge_fo2_total:NO.2燃油供油单元总消耗
     * @return BigDecimal* @param configMap 系统配置值 key ->value
     * @param data 类变量，用来保存所有的指标名对应的innerkey和值
     */
    private BigDecimal calculation(Map<String, String> configMap, Map<String, BigDecimal> data) {
        try {
            BigDecimal result = null;
            //NO.1燃油供油单元辅机总消耗
            BigDecimal ge_fo1_total = data.get(AmsInnerKeyVo.ge_fo1_total);
            //NO.2燃油供油单元辅机总消耗
            BigDecimal ge_fo2_total = data.get(AmsInnerKeyVo.ge_fo2_total);
            //参与值不全
            if (ge_fo1_total == null || ge_fo2_total == null) {
             //   logger.debug("[{}] [{}] [{}] NO.1燃油供油单元辅机总消耗:[{}] NO.2燃油供油单元辅机总消耗:[{}] result[{}]", sid, indexGroup, Time_1, ge_fo1_total, ge_fo2_total, null);
                return null;
            }
            println (configMap.get("test"))
            //代入公式
            result = (ge_fo1_total.add(ge_fo2_total));
      //      logger.debug("[{}] [{}] [{}] NO.1燃油供油单元辅机总消耗:[{}] NO.2燃油供油单元辅机总消耗:[{}] result[{}]", sid, indexGroup, Time_1, ge_fo1_total, ge_fo2_total, result);
            return result;
        } catch (Exception e) {
 //           logger.debug("[{}] [{}] [{}] 计算错误异常:{}", sid, indexGroup, Time_1, e.getMessage());
            return null;
        }
    }
}
