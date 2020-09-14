package com.sdari.processor.AnalysisByMeteorological

import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.SerializerFeature
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
import ucar.nc2.NetcdfFile

import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

@EventDriven
@CapabilityDescription('岸基-气象文件解析处理器')
class AnalysisByMeteorological implements Processor {
    static def log
    //处理器id，同处理器管理表中的主键一致，由调度处理器中的配置同步而来
    private String id
    private String currentClassName = this.class.canonicalName
    private DBCPService dbcpService = null
    private GroovyObject pch

    //时间相关参数
    final static String start_time_type = "yyyyMMddHHmm"
    final static String meteorological_time = "meteorological.time"

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that were successfully processed")
            .build()
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be processed")
            .build()

    @Override
    Set<Relationship> getRelationships() {
        Collections.unmodifiableSet([REL_SUCCESS, REL_FAILURE] as Set<Relationship>)
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
            log.info "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器起始运行完毕"
        } catch (Exception e) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器起始运行异常", e
        }
    }

    @OnStopped
    public void OnStopped(final ProcessContext context) {
        try {
            pch.invokeMethod("releaseComponent", null)//相关公共配置实例清空
            log.info "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器停止运行完毕"
        } catch (Exception e) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器关闭异常", e
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
            final def attributesMap = flowFile.getAttributes()
            final String path = attributesMap.get('absolute.path')
            final String filename = attributesMap.get('filename')
            def attributesMaps=new HashMap()
            String fileName = filename
            String[] split = fileName.substring(22, 35).split("_")
            String s = split[0] + split[1]
            Date createTime = DateByParse(s)
            long time = (long) ((createTime.getTime()) / 1000)

            attributesMaps.put('absolute.path',path)
            attributesMaps.put('filename',filename)
            attributesMaps.put(meteorological_time, String.valueOf(time))

            List<JSONObject> lists = readGzFile(path + filename)
            if (lists.size() > 0) {
                session.putAllAttributes(flowFile, attributesMaps)
                //FlowFile write 数据
                session.write(flowFile, { out ->
                    out.write(JSONArray.toJSONBytes(lists,
                            SerializerFeature.WriteMapNullValue))
                } as OutputStreamCallback)
                session.transfer(flowFile, REL_SUCCESS)
            } else {
                session.transfer(flowFile, REL_FAILURE)
            }

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
    private static void onFailure(final ProcessSession session, final FlowFile flowFile) {
        session.transfer(flowFile, REL_FAILURE)
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


    /**
     *  从指定路劲中获取nc文件，并进行清洗
     * @param FilePath nc文件地址
     */
    private static List<JSONObject> readGzFile(String FilePath) throws Exception {
        List<JSONObject> jsonObjects = new ArrayList<>(2000)
        Set<String> keySet = new HashSet<>()
        // 循环读取nc文件中的数据
        NetcdfFile ncFiles = null
        try {
            ncFiles = NetcdfFile.open(FilePath)
            // 读取时间的值
            double[] timeArray = (double[]) ncFiles.findVariable("time").read().copyTo1DJavaArray()
            // 读取经度的值
            double[] latitudeArray = (double[]) ncFiles.findVariable("latitude").read().copyTo1DJavaArray()
            // 读取纬度的值
            double[] longitudeArray = (double[]) ncFiles.findVariable("longitude").read().copyTo1DJavaArray()
            // 读取流向的值
            double[][][] current_directionArray = (double[][][]) ncFiles.findVariable("current_direction").read().copyToNDJavaArray()
            // //读取流速的值
            double[][][] current_magnitudeArray = (double[][][]) ncFiles.findVariable("current_magnitude").read().copyToNDJavaArray()
            // 读取风向的值
            double[][][] wind_directionArray = (double[][][]) ncFiles.findVariable("wind_direction").read().copyToNDJavaArray()
            // 读取风速的值
            double[][][] wind_magnitudeArray = (double[][][]) ncFiles.findVariable("wind_magnitude").read().copyToNDJavaArray()
            // 读取浪向的值
            double[][][] wave_directionArray = (double[][][]) ncFiles.findVariable("wave_direction").read().copyToNDJavaArray()
            // 读取浪高的值
            double[][][] wave_heightArray = (double[][][]) ncFiles.findVariable("wave_height").read().copyToNDJavaArray()
            // 读取海平面绝对压力的值
            double[][][] surface_pressureArray = (double[][][]) ncFiles.findVariable("surface_pressure").read().copyToNDJavaArray()
            // 读取海平面绝对温度的值
            double[][][] surface_temperatureArray = (double[][][]) ncFiles.findVariable("surface_temperature").read().copyToNDJavaArray()
            //读取浪周期
            double[][][] wave_period = (double[][][]) ncFiles.findVariable("wave_period").read().copyToNDJavaArray()
            //读取涌向
            double[][][] swell_direction = (double[][][]) ncFiles.findVariable("swell_direction").read().copyToNDJavaArray()
            //读取涌高
            double[][][] swell_height = (double[][][]) ncFiles.findVariable("swell_height").read().copyToNDJavaArray()
            //读取涌周期
            double[][][] swell_period = (double[][][]) ncFiles.findVariable("swell_period").read().copyToNDJavaArray()
            //读取500mb等高线
            double[][][] mb_height = (double[][][]) ncFiles.findVariable("500mB_height").read().copyToNDJavaArray()
            for (int p = 0; p < timeArray.length; p++) {
                for (int m = 0; m < latitudeArray.length; m++) {
                    for (int n = 0; n < longitudeArray.length; n++) {
                        JSONObject jsonObject = new JSONObject()
                        //   Long weatherTime = new Double(timeArray[p] * 3600).longValue() + time
                        jsonObject.put('time', Long.valueOf((long) timeArray[p]))
                        if (longitudeArray[n] < (-180)) {
                            longitudeArray[n] = longitudeArray[n] + 360
                        }
                        if (longitudeArray[n] >= 180) {
                            longitudeArray[n] = longitudeArray[n] - 360
                        }
                        jsonObject.put('longitude', longitudeArray[n])
                        jsonObject.put('latitude', latitudeArray[m])
                        String rowkey = getRowkey(jsonObject)
                        if (!keySet.contains(rowkey)) {
                            keySet.add(rowkey)
                            if (current_directionArray[p][m][n] < 0.0 || current_directionArray[p][m][n] >= 360.0) {
                                current_directionArray[p][m][n] = 0.0
                            }
                            jsonObject.put('currentDirection', current_directionArray[p][m][n])
                            if (current_magnitudeArray[p][m][n] < 0.0) {
                                current_magnitudeArray[p][m][n] = 0.0
                            }
                            jsonObject.put('currentMagnitude', current_magnitudeArray[p][m][n])
                            if (wind_directionArray[p][m][n] < 0.0 || wind_directionArray[p][m][n] >= 360.0) {
                                wind_directionArray[p][m][n] = 0.0
                            }
                            jsonObject.put('windDirection', wind_directionArray[p][m][n])
                            if (wind_magnitudeArray[p][m][n] < 0.0) {
                                wind_magnitudeArray[p][m][n] = 0.0
                            }
                            jsonObject.put('windMagnitude', wind_magnitudeArray[p][m][n])
                            if (wave_directionArray[p][m][n] < 0.0 || wave_directionArray[p][m][n] >= 360.0) {
                                wave_directionArray[p][m][n] = 0.0
                            }
                            jsonObject.put('waveDirection', wave_directionArray[p][m][n])
                            if (wave_heightArray[p][m][n] < 0.0) {
                                wave_heightArray[p][m][n] = 0.0
                            }
                            jsonObject.put('waveHeight', wave_heightArray[p][m][n])
                            if (surface_pressureArray[p][m][n] <= (-101325)) {
                                surface_pressureArray[p][m][n] = 101325.0
                            }
                            jsonObject.put('surfacePressure', surface_pressureArray[p][m][n])
                            if (surface_temperatureArray[p][m][n] <= 0.0) {
                                surface_temperatureArray[p][m][n] = 288.0
                            }
                            jsonObject.put('surfaceTemperature', surface_temperatureArray[p][m][n])
                            if (wave_period[p][m][n] <= 0.0) {
                                wave_period[p][m][n] = 0.0
                            }
                            jsonObject.put('wavePeriod', wave_period[p][m][n])
                            if (swell_direction[p][m][n] < 0.0 || swell_direction[p][m][n] >= 360) {
                                swell_direction[p][m][n] = 0.0
                            }
                            jsonObject.put('swellDirection', swell_direction[p][m][n])
                            if (swell_height[p][m][n] < 0.0) {
                                swell_height[p][m][n] = 0.0
                            }
                            jsonObject.put('swellHeight', swell_height[p][m][n])
                            if (swell_period[p][m][n] <= 0.0) {
                                swell_period[p][m][n] = 0.0
                            }
                            jsonObject.put('swellPeriod', swell_period[p][m][n])
                            if (mb_height[p][m][n] <= 0.0) {
                                mb_height[p][m][n] = 0.0
                            }
                            jsonObject.put('mb', mb_height[p][m][n])
                            jsonObject.put('rowkey', rowkey)
                            jsonObjects.add(jsonObject)
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace()
            log.error("nc文件解析失败:{}", FilePath)
            return jsonObjects
        } finally {
            if (null != ncFiles) {
                ncFiles.close()
                ncFiles = null
            }
        }
        return jsonObjects
    }

    /**
     * 时间格式转换
     */
    static Date DateByParse(String time) {
        SimpleDateFormat t = new SimpleDateFormat(start_time_type)
        t.setTimeZone(TimeZone.getTimeZone("UTC"))
        return t.parse(time)
    }
    /**
     * 字符串截取
     */
    static String leftPad(String origin, Integer size, String index) {
        int dexLength = size - origin.length()
        String string = ""
        for (int i = 0; i < dexLength; i++) {
            string += index
        }
        return string + origin
    }
    /**
     * 气象文件生成rowKey方法
     */
    static String getRowkey(JSONObject json) {
        if (json.get('longitude') == null || json.get('latitude') == null || json.get('time') == null) {
            return null
        }
        // lon 经度 -180~180, 为负数前面补1,为正数前面补0, 保证4位
        int intLon = new Double(json.get('longitude') as String).intValue()
        String lon = (json.get('longitude') >= 0 ? "0" : "1") + leftPad(String.valueOf(Math.abs(intLon)), 3, "0")
        // lat 纬度 -90~90, 为负数前面补1,为正数前2q面补0, 保证3位
        int intLat = new Double(json.get('latitude') as String).intValue()
        String lat = (json.get('latitude') >= 0 ? "0" : "1") + leftPad(String.valueOf(Math.abs(intLat)), 2, "0")
        return json.get('time') + lon + lat
    }


}

//脚本部署时需要放开该注释
//processor = new AnalysisByMeteorological()
