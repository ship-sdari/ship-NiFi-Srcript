package com.sdari.processor.CompressData

import com.alibaba.fastjson.JSONObject
import org.apache.commons.io.IOUtils
import org.apache.nifi.logging.ComponentLog

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 子脚本模板
 */
class AppendAttributes {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    AppendAttributes(final ComponentLog logger, final int pid, final String pName, final int rid) {
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
        final List<InputStream> dataList = (params as HashMap).get('data') as ArrayList
        final List<JSONObject> attributesList = ((params as HashMap).get('attributes') as ArrayList)
        final Map<String, Map<String, JSONObject>> rules = ((params as HashMap).get('rules') as Map<String, Map<String, JSONObject>>)
        final Map processorConf = ((params as HashMap).get('parameters') as HashMap)
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            try {//详细处理流程
                final InputStream jsonDataFormer = (dataList.get(i) as InputStream)
                final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
                //开始追加属性
                OutputStream out = new ByteArrayOutputStream()
                IOUtils.copy(jsonDataFormer, out)
                String append = 's0ay' +  jsonAttributesFormer.getString('compress.type')
                out.write(append.getBytes('ISO_8859_1'))
                InputStream returnIn = new ByteArrayInputStream(out.toByteArray())
                jsonDataFormer.close()//输入流关闭
                out.close()//中转输出流关闭
                jsonAttributesFormer.put('filename', jsonAttributesFormer.getString('file.name'))
                //单条数据处理结束，放入返回仓库
                dataListReturn.add(returnIn)
                attributesListReturn.add(jsonAttributesFormer)
            } catch (Exception e) {
                throw new Exception("[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 处理单条数据时异常", e)
            }
        }
        //全部数据处理完毕，放入返回数据后返回
        returnMap.put('rules', rules)
        returnMap.put('attributes', attributesListReturn)
        returnMap.put('parameters', processorConf)
        returnMap.put('data', dataListReturn)
        return returnMap
    }
}
