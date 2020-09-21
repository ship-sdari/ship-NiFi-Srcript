package com.sdari.processor.MergeDataByToShoreGroup

import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.SerializerFeature
import lombok.Data
import org.apache.commons.io.IOUtils
import org.apache.nifi.annotation.behavior.EventDriven
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.annotation.lifecycle.OnStopped
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.ValidationContext
import org.apache.nifi.components.ValidationResult
import org.apache.nifi.dbcp.DBCPService
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.*
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.io.OutputStreamCallback
import org.luaj.vm2.ast.Str

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

@EventDriven
@CapabilityDescription('合并岸基分发的数据，该处理器为功能处理器，所以无子脚本流程')
class MergeDataByToShoreGroup implements Processor {
    static def log
    //处理器id，同处理器管理表中的主键一致，由调度处理器中的配置同步而来
    private String id
    private String currentClassName = this.class.canonicalName
    private DBCPService dbcpService = null
    private GroovyObject pch
    private Map<String, MergeGroupDTO> mergeGroupDTOMap = new ConcurrentHashMap<>()
    private List<MergeGroupDTO> history = new ArrayList<>()
    final static Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description('FlowFiles that were used originally are routed here')
            .build()

    @Override
    Set<Relationship> getRelationships() {
        Set<Relationship> set = new HashSet<Relationship>()
        set.add(REL_ORIGINAL)//单独添加初始数据路由
        Map<String, Relationship> relationshipMap = (pch?.invokeMethod("getRelationships", null) as Map<String, Relationship>)
        if (relationshipMap != null && relationshipMap.size() > 0) {
            for (String relation : relationshipMap.keySet()) {
                Relationship relationship = relationshipMap.get(relation)
                set.add(relationship)
            }
        }
        Collections.unmodifiableSet(set) as Set<Relationship>
    }

    @Override
    List<PropertyDescriptor> getPropertyDescriptors() {
        List<PropertyDescriptor> descriptorList = new ArrayList<>()
        Map<String, PropertyDescriptor> descriptorMap = (pch?.invokeMethod("getDescriptors", null) as Map<String, PropertyDescriptor>)
        if (descriptorMap != null && descriptorMap.size() > 0) {
            for (String name : descriptorMap.keySet()) {
                descriptorList.add(descriptorMap.get(name))
            }
        }
        Collections.unmodifiableList(descriptorList) as List<PropertyDescriptor>
    }

    /**
     * 实现自定义处理器的不可缺方法
     * @param context 上下文
     */
    void initialize(ProcessorInitializationContext context) {

    }
    /**
     * A method that executes only once when initialized
     *
     * @param context
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        try {
            pch.invokeMethod("initComponent", null)//相关公共配置实例更新查询
            pch.invokeMethod("initScript", [log, currentClassName])
            path2package()//将暂存本地的历史数据包读取出来
            log.info "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器起始运行完毕"
        } catch (Exception e) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器起始运行异常", e
        }
    }

    /**
     * A method that executes only once when stop
     *
     * @param context
     */
    @OnStopped
    public void onStopped(final ProcessContext context) {
        try {
            package2path()//相关打包数据暂存并清空
            pch.invokeMethod("releaseComponent", null)//相关公共配置实例清空
            log.info "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器停止运行完毕"
        } catch (Exception e) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器停止运行异常", e
        }
    }

    void package2path() {
        for (mergeDto in mergeGroupDTOMap.values()) {
            try {
                JSONObject json = mergeDto.merge
                if (json.size() == 0) return
                final String fileName = String.join('-', [mergeDto.sid, mergeDto.shipCollectProtocol, mergeDto.shipCollectFreq, mergeDto.shoreGroup, mergeDto.shoreIp, mergeDto.shorePort, mergeDto.shoreFreq as String, mergeDto.compressType] as Iterable<? extends CharSequence>) + '.json'
                final String path = (pch.getProperty('parameters') as HashMap)?.getOrDefault('merge.path', "/home/sdari/app/nifi/share/merged/${mergeDto.sid}/") as String
                File file = new File(path)
                if (!file.isDirectory()) file.mkdirs()//目录不存在就创建
                File full = new File(path + fileName)
                if (!full.exists()) file.createNewFile()//文件不存在就创建
                BufferedWriter writer = new BufferedWriter(new FileWriter(full))
                writer.write(JSONObject.toJSONString(json, SerializerFeature.WriteMapNullValue))
                writer.close()
            } catch (Exception e) {
                log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器停止时，暂存打包数据异常", e
            }
        }
        mergeGroupDTOMap?.clear()//清空用于打包的抽象数据类型仓库
        history?.clear()//清空存储历史包的抽象数据类型仓库
    }

    void path2package() {
        final String path = (pch.getProperty('parameters') as HashMap)?.getOrDefault('merge.path', "/home/sdari/app/nifi/share/merged/${(pch?.invokeMethod('getProcessor', null) as GroovyObject)?.getProperty('sid') as String}/") as String
        File p = new File(path)
        if (!p.isDirectory()) return
        File[] files = p.listFiles()
        for (file in files) {
            try {
                if (file.isFile()) {
                    List<String> attrs = file.name?.substring(0, file.name?.length() - 5)?.split('-')?.toList()
                    if (null == attrs || attrs.size() != 8) throw new Exception('文件名解析异常')
                    MergeGroupDTO merge = new MergeGroupDTO()
                    merge.sid = attrs.get(0)
                    merge.shipCollectProtocol = attrs.get(1)
                    merge.shipCollectFreq = attrs.get(2)
                    merge.shoreGroup = attrs.get(3)
                    merge.shoreIp = attrs.get(4)
                    merge.shorePort = attrs.get(5)
                    merge.shoreFreq = Double.parseDouble(attrs.get(6))
                    merge.compressType = attrs.get(7)
                    InputStream inputStream = file.newInputStream()
                    merge.merge.putAll JSONObject.parseObject(IOUtils.toString(inputStream, 'UTF-8'))
                    history.add merge
                    inputStream.close()
                    if (!file.delete()) throw new Exception('文件删除失败')
                }
            } catch (Exception e) {
                log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器启动时，历史包${file.name}恢复异常", e
            }
        }
    }

    /**
     * 详细处理模块
     * @param context
     * @param sessionFactory
     * @throws org.apache.nifi.processor.exception.ProcessException
     */
    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession()
        FlowFile flowFile = session.get()
        if (flowFile == null) {
            session.commit()
            return
        }
        if (!(pch?.getProperty('isInitialized') as AtomicBoolean)?.get() || 'A' != (pch?.getProperty('processor') as GroovyObject)?.getProperty('status')) {
            //工具类初始化有异常或者该处理管理表处于不是开启状态就删除流文件不做任何处理
            session.remove(flowFile)
            session.commit()
            return
        }
        /*以下为正常处理数据文件的部分*/
        final AtomicReference<JSONObject> datas = new AtomicReference<>()
        session.read(flowFile, { inputStream ->
            try {
                datas.set(JSONObject.parseObject(IOUtils.toString(inputStream, StandardCharsets.UTF_8)))
            } catch (Exception e) {
                log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 读取流文件失败", e
                onFailure(session, flowFile)
                session.commit()
            }
        })
        try {
            if (null == datas.get() || datas.get().size() == 0) {
                throw new Exception("[Processor_id = ${id} Processor_name = ${currentClassName}] 的接收的数据为空!")
            }
            def relationships = pch.invokeMethod("getRelationships", null) as Map<String, Relationship>
            final def attributesMap = pch.invokeMethod("updateAttributes", [flowFile.getAttributes()]) as Map<String, String>
            //调用脚本需要传的参数[attributesMap-> flowFile属性][data -> flowFile数据]
            def attributesList = []
            def dataList = []
            switch (datas.get().getClass().canonicalName) {
                case 'com.alibaba.fastjson.JSONObject':
                    attributesList.add(attributesMap.clone())
                    dataList.add(datas.get())
                    break
                case 'com.alibaba.fastjson.JSONArray':
                    datas.get().each { o -> attributesList.add(attributesMap.clone()) }
                    dataList = datas.get()
                    break
                default:
                    throw new Exception("暂不支持处理当前所接收的数据类型：${datas.get().getClass().canonicalName}")
            }
            //循环路由名称 根据路由状态处理 [路由名称->路由实体]
            String routeName = ''
            for (routesDTO in (pch.getProperty('routeConf') as Map<String, GroovyObject>)?.values()) {
                try {
                    routeName = routesDTO.getProperty('route_name') as String
                    if ('A' == routesDTO.getProperty('route_running_way')) {
                        log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] Route = ${routeName} 的运行方式，暂不支持并行执行方式，请检查路由管理表!"
                        continue
                    }
                    //结果存储
                    List<JSONArray> returnDataList
                    List<JSONObject> returnAttributesList
                    //路由方式 A-正常路由 I-源文本路由 S-不路由
                    def routeStatus = 'S'
                    //路由关系
                    switch (routesDTO.getProperty('status')) {
                    //路由关系禁用
                        case "S":
                            routeStatus = 'S'
                            break
                    //路由关系忽略，应当源文本路由
                        case "I":
                            routeStatus = 'I'
                            break
                    //路由关系正常执行
                        default:
                            //数据录入并检查
                            returnDataList = []
                            returnAttributesList = []
                            //循环list中的每一条数据
                            for (int i = 0; i < (dataList as ArrayList).size(); i++) {
                                try {
                                    MergeGroupDTO merge
                                    final JSONObject jsonDataFormer = ((dataList as ArrayList).get(i) as JSONObject)
                                    final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
                                    final String toShoreGroup = jsonAttributesFormer.getString('shore.group')
                                    final String shipCollectProtocol = jsonAttributesFormer.getString('ship.collect.protocol')
                                    final String shipCollectFreq = jsonAttributesFormer.getString('ship.collect.freq')
                                    final String coltime = (jsonAttributesFormer.getString('coltime'))
                                    final String key = shipCollectProtocol + '/' + shipCollectFreq + '/' + toShoreGroup
                                    if (!mergeGroupDTOMap.containsKey(key)) {
                                        merge = new MergeGroupDTO()
                                        merge.sid = jsonAttributesFormer.getString('sid')
                                        merge.shipCollectProtocol = shipCollectProtocol
                                        merge.shipCollectFreq = shipCollectFreq
                                        merge.shoreGroup = toShoreGroup
                                        merge.shoreIp = jsonAttributesFormer.getString('shore.ip')
                                        merge.shorePort = jsonAttributesFormer.getString('shore.port')
                                        merge.shoreFreq = Double.parseDouble(jsonAttributesFormer.getString('shore.freq'))
                                        merge.compressType = jsonAttributesFormer.getString('compress.type')
                                        mergeGroupDTOMap.put(key, merge)
                                    }
                                    merge = mergeGroupDTOMap.get(key)
                                    JSONArray merged = merge.addAndCheckOut(coltime, JSONObject.toJSONString(jsonDataFormer, SerializerFeature.WriteMapNullValue))
                                    if (null != merged) {//达到合并状态
                                        returnDataList.add(merged)
                                        returnAttributesList.add(jsonAttributesFormer)
                                        routeStatus = 'A'
                                    } else {//未达到合并状态就检查历史仓库
                                        if (history.size() > 0) {//有历史
                                            Iterator its = history.iterator()
                                            while (its.hasNext()) {
                                                try {
                                                    MergeGroupDTO hisMerge = its.next() as MergeGroupDTO
                                                    JSONArray hisMerged = hisMerge.out()
                                                    JSONObject newAttributes = new JSONObject()
                                                    newAttributes.put('sid', hisMerge.sid)
                                                    newAttributes.put('ship.collect.protocol', hisMerge.shipCollectProtocol)
                                                    newAttributes.put('ship.collect.freq', hisMerge.shipCollectFreq)
                                                    newAttributes.put('shore.group', hisMerge.shoreGroup)
                                                    newAttributes.put('shore.ip', hisMerge.shoreIp)
                                                    newAttributes.put('shore.port', hisMerge.shorePort)
                                                    newAttributes.put('shore.freq', hisMerge.shoreFreq as String)
                                                    newAttributes.put('compress.type', hisMerge.compressType)
                                                    returnDataList.add hisMerged
                                                    returnAttributesList.add newAttributes
                                                    its.remove()
                                                } catch (Exception e) {
                                                    log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] Route = ${routeName} 的历史数据包路由有异常", e
                                                }
                                            }
                                            routeStatus = 'A'
                                        }
                                    }
                                } catch (Exception e) {
                                    log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] Route = ${routeName} 的数据插入和检查过程有异常", e
                                }
                            }
                    }
                    //如果脚本执行了路由下去
                    switch (routeStatus) {
                        case 'A':
                            def flowFiles = []
                            if ((null == returnDataList || null == returnAttributesList) || (returnDataList.size() != returnAttributesList.size())) {
                                throw new Exception('结果条数与属性条数不一致，请检查子脚本处理逻辑！')
                            }
                            for (int i = 0; i < returnDataList.size(); i++) {
                                FlowFile flowFileNew = session.create()
                                try {
                                    session.putAllAttributes(flowFileNew, (returnAttributesList.get(i) as Map<String, String>))
                                    //FlowFile write 数据
                                    session.write(flowFileNew, { out ->
                                        IOUtils.writeLines(returnDataList.get(i), "\n", out, "UTF-8")
                                    } as OutputStreamCallback)
                                    flowFiles.add(flowFileNew)
                                } catch (Exception e) {
                                    log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] Route = ${routeName} 创建流文件异常", e
                                    session.remove(flowFileNew)
                                }
                            }
                            if (null == relationships.get(routeName)) throw new Exception('没有该创建路由关系，请核对管理表！')
                            session.transfer(flowFiles, relationships.get(routeName))
                            break
                        case 'I':
                            if (null == relationships.get(routeName)) throw new Exception('没有该创建路由关系，请核对管理表！')
                            session.transfer(session.clone(flowFile), relationships.get(routeName))
                            break
                        default:
                            //不路由
                            break
                    }
                } catch (Exception e) {
                    log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] Route = ${routeName} 的处理过程有异常", e
                } finally {
                    //路由关系循环只循环第一个，即默认配置的最有效输出路由只有merged一个
                    break
                }
            }
            session.transfer(flowFile, REL_ORIGINAL)//源文件路由
        } catch (final Throwable t) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 的处理过程有异常", t
            onFailure(session, flowFile)
        } finally {
            session.commit()
        }
    }


    @Override
    Collection<ValidationResult> validate(ValidationContext context) { null }

    @Override
    PropertyDescriptor getPropertyDescriptor(String name) {
        return (pch.getProperty('descriptors') as Map<String, PropertyDescriptor>).get(name)
    }

    @Override
    void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {}

    @Override
    String getIdentifier() { null }

    /**
     * 失败路由处理
     * @param session
     * @param flowFile
     */
    private void onFailure(final ProcessSession session, final FlowFile flowFile) {
        session.transfer(flowFile, (pch.getProperty('relationships') as Map<String, Relationship>).get('failure'))
    }
    /**
     * 任务功能处理器最开始的同步和初始化调用方法，由调度处理器调用
     * 同步 脚本id及dbcpService
     * 实例化公共工具类
     * @param pid 处理器id
     * @param service 数据库连接的控制服务对象
     * @throws Exception
     */
    void scriptByInitId(pid, service, processorComponentHelperText) throws Exception {
        id = pid //同步处理器id
        try {
            dbcpService = service
            //工具类实例化
            GroovyClassLoader classLoader = new GroovyClassLoader()
            Class aClass = classLoader.parseClass(processorComponentHelperText as String)
            pch = aClass.newInstance(pid as int, service) as GroovyObject//有参构造
            pch.invokeMethod("initComponent", null)//相关公共配置实例查询
            log.info "[Processor_id = ${id} Processor_name = ${currentClassName}] 任务功能处理器最开始的同步和初始化调用正常结束！"
        } catch (Exception e) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 任务功能处理器最开始的同步和初始化调用方法异常", e
        }
    }

    /**
     * 设置该处理器的logger
     * @param logger
     * @throws Exception
     */
    void setLogger(final ComponentLog logger) {
        try {
            log = logger
            log.info "[Processor_id = ${id} Processor_name = ${currentClassName}] setLogger 执行成功，日志已设置完毕"
        } catch (Exception e) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 设置日志的调用方法异常", e
        }
    }

    @Data
    static class MergeGroupDTO {
        private String sid
        private String shipCollectProtocol
        private String shipCollectFreq
        private String shoreGroup
        private String shoreIp
        private String shorePort
        private Double shoreFreq
        private String compressType
        private JSONObject merge = new JSONObject(new TreeMap<String, Object>())

        private void push(String time, String data) throws Exception {
            merge.put(time, data)
        }

        private boolean check() throws Exception {
            List<String> times = new ArrayList<>(merge.keySet())
//            times = times.stream().sorted() as List<String>
            long gap = Instant.parse(times.max()).getEpochSecond() - Instant.parse(times.min()).getEpochSecond()
            return gap >= shoreFreq
        }

        synchronized private JSONArray out() throws Exception {
            JSONArray array = new JSONArray()
            array.addAll(merge.values())//输出仓库
            merge.clear()//清空仓库
            return array
        }

        synchronized JSONArray addAndCheckOut(String time, String data) throws Exception {//同步类实例
            push(time, data)
            boolean isReturn = check()
            if (isReturn) {
                return out()
            } else {
                return null
            }
        }
    }
}

//脚本部署时需要放开该注释
//processor = new MergeDataByToShoreGroup()