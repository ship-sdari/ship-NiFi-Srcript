package com.sdari.processor.CompressData

import com.alibaba.fastjson.JSONObject
import com.fasterxml.jackson.core.JsonEncoding
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import lombok.Data
import org.apache.commons.compress.compressors.CompressorException
import org.apache.commons.compress.compressors.CompressorInputStream
import org.apache.commons.compress.compressors.CompressorOutputStream
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.compress.utils.IOUtils
import org.apache.nifi.logging.ComponentLog

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 将分发数据进行检查、压缩、格式化的脚本
 */
class CompressDetail {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    CompressDetail(final ComponentLog logger, final int pid, final String pName, final int rid) {
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
                //开始压缩步骤
                OutputStream out = new ByteArrayOutputStream()
                String compressType = jsonAttributesFormer.getString('compress.type')
                ColumnStore columnStore
                switch (compressType) {
                    case 'delta_rle_lzma':
                        columnStore = new ColumnStore(ColumnStore.DELTA_RLE, CompressorStreamFactory.LZMA)
                        break
                    case 'delta_zstd':
                        columnStore = new ColumnStore(ColumnStore.DELTA, CompressorStreamFactory.LZMA)
                        break
                    default:
                        throw new Exception("当前不支持改压缩算法：${compressType}")
                }
                columnStore.compressFile(jsonDataFormer, out)//压缩
                InputStream returnIn = new ByteArrayInputStream(out.toByteArray())
                jsonDataFormer.close()//输入流关闭
                out.close()//中转输出流关闭
                //单条数据处理结束，放入返回仓库
                dataListReturn.add(returnIn)
                attributesListReturn.add(jsonAttributesFormer)
            } catch (Exception e) {
                log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 处理单条数据时异常", e
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

class ColumnStore {
    public static String DELTA = "delta"
    public static String RLE = "rle"
    public static String DELTA_RLE = "delta_rle"

    private String encoding = DELTA
    private String compression = CompressorStreamFactory.SNAPPY_RAW

    ColumnStore(String enconding, String compression) {
        this.encoding = enconding
        this.compression = compression
    }

    // convert row format to column format
    void row2col(CSTore csTore) throws Exception {
        if (csTore.isColumnar)
            return
        csTore.columns.forEach({ col -> csTore.csTore.add(new ArrayList<>()) })
        for (ArrayList<Object> row : csTore.data) {
            for (int i = 0; i < row.size(); i++) {
                csTore.csTore.get(i).add(row.get(i))
            }
        }
        csTore.isColumnar = true
    }

    // convert column format to row format
    void col2row(CSTore csTore) throws Exception {
        if (csTore.csTore.size() == 0 || !csTore.isColumnar) return
        for (int i = 0; i < csTore.csTore.get(0).size(); i++) {
            ArrayList<Object> row = new ArrayList<>()
            for (int j = 0; j < csTore.csTore.size(); j++) {
                row.add(csTore.csTore.get(j).get(i))
            }
            csTore.data.add(row)
        }
        csTore.isColumnar = false
    }

    void compressFile(InputStream input, OutputStream out) throws Exception {
        CompressionText zip = new CompressionText(compression)
        CSTore csTore = new CSTore()

        JsonFile.loadRowFormat(input, csTore)
        row2col(csTore)
        if (this.encoding == DELTA) {
            for (int i = 0; i < csTore.csTore.size(); i++) {
                if (csTore.columnsType.containsKey(i) && csTore.columnsType.get(i)) {
                    continue
                }
                csTore.csTore.set(i, DecodeDelta.encode(csTore.csTore.get(i)))
            }
        } else if (this.encoding == RLE) {
            for (int i = 0; i < csTore.csTore.size(); i++) {
                if (csTore.columnsType.containsKey(i) && csTore.columnsType.get(i)) {
                    continue
                }
                csTore.csTore.set(i, DecodeRLE.encode(csTore.csTore.get(i)))
            }
        } else if (this.encoding == DELTA_RLE) {
            for (int i = 0; i < csTore.csTore.size(); i++) {
                if (csTore.columnsType.containsKey(i) && csTore.columnsType.get(i)) {
                    continue
                }
                csTore.csTore.set(i, DecodeDelta.encode(csTore.csTore.get(i)))
                csTore.csTore.set(i, DecodeRLE.encode(csTore.csTore.get(i)))
            }
        }
        //创建中间OutputStream
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        JsonFile.saveColFormat(outputStream, csTore)
        //创建中间InputStream并将outputStream转换为它
        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())
        outputStream.close()
        //开始常规压缩,压缩内容写入out
        zip.compressFile(inputStream, out)
    }

    void decompressFile(InputStream input, OutputStream out) throws Exception {
        CompressionText zip = new CompressionText(compression)
        CSTore csTore = new CSTore()

        //创建outputStream
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        zip.decompressFile(input, outputStream)
        //创建中间InputStream并将outputStream转换为它
        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())
        outputStream.close()
        JsonFile.loadColFormat(inputStream, csTore)
        if (encoding == DELTA) {
            for (int i = 0; i < csTore.csTore.size(); i++) {
                if (csTore.columnsType.containsKey(i) && csTore.columnsType.get(i)) {
                    continue
                }
                csTore.csTore.set(i, DecodeDelta.decode(csTore.csTore.get(i)))
            }
        } else if (this.encoding == RLE) {
            for (int i = 0; i < csTore.csTore.size(); i++) {
                if (csTore.columnsType.containsKey(i) && csTore.columnsType.get(i)) {
                    continue
                }
                csTore.csTore.set(i, DecodeRLE.decode(csTore.csTore.get(i)))
            }
        } else if (this.encoding == DELTA_RLE) {
            for (int i = 0; i < csTore.csTore.size(); i++) {
                if (csTore.columnsType.containsKey(i) && csTore.columnsType.get(i)) {
                    continue
                }
                csTore.csTore.set(i, DecodeRLE.decode(csTore.csTore.get(i)))
                csTore.csTore.set(i, DecodeDelta.decode(csTore.csTore.get(i)))
            }
        }
        col2row(csTore)
        //开始format数据并写入out
        JsonFile.saveRowFormat(out, csTore)
    }

    class CompressionText {
        private String method = CompressorStreamFactory.SNAPPY_RAW

        CompressionText(String method) {
            this.method = method
        }

        void compressFile(InputStream inputStream, OutputStream out) throws Exception {
            try {
                CompressorOutputStream zipout = new CompressorStreamFactory()
                        .createCompressorOutputStream(method, out)

                IOUtils.copy(inputStream, zipout)
                zipout.close()
                inputStream.close()
            } catch (IOException | CompressorException e) {
                throw e
            }
        }

        void decompressFile(InputStream inputStream, OutputStream outputStream) throws Exception {
            //zip文件输入流
            try {
                CompressorInputStream input = new CompressorStreamFactory()
                        .createCompressorInputStream(method, inputStream)

                IOUtils.copy(input, outputStream)
                input.close()
            } catch (IOException | CompressorException e) {
                throw e
            }
        }
    }

    /**
     * @author jinkaisong* date:2020-04-23 14:10
     */
    @Data
    class CSTore {
        private HashMap<String, Object> meta = new HashMap<>()
        private boolean isColumnar = true
        private ArrayList<String> columns = new ArrayList<>()
        private ArrayList<ArrayList<Object>> data = new ArrayList<>()
        private ArrayList<ArrayList<Object>> csTore = new ArrayList<>()
        private HashMap<Integer, Boolean> columnsType = new HashMap<>()//列名所在索引的位置，对应的数据类型，true表示字符型
    }

    class DecodeDelta {
        static ArrayList<Object> encode(ArrayList<Object> column) throws Exception {

            if ((column.size() == 0) || (column.get(0) instanceof String)) {
                return column
            }
            ArrayList<Object> out = new ArrayList<>()

            BigDecimal last = new BigDecimal(0)
            BigDecimal current
            for (Object o : column) {
                BigDecimal item = (BigDecimal) o
                if (item != null) {
                    current = item.subtract(last)
                    out.add(current)
                    last = item
                } else {
                    out.add(null)
//                last = new BigDecimal(0)
                }
            }
            return out
        }

        static ArrayList<Object> decode(ArrayList<Object> column) throws Exception {
            if ((column.size() == 0) || (column.get(0) instanceof String)) {
                return column
            }
            ArrayList<Object> out = new ArrayList<>()
            BigDecimal last = new BigDecimal(0)
            BigDecimal current
            for (Object o : column) {
                BigDecimal item = (BigDecimal) o
                if (item != null) {
                    current = last.add(item)
                    out.add(current)
                    last = current
                } else {
                    out.add(null)
//                last = new BigDecimal(0)
                }
            }

            return out
        }
    }

    class DecodeRLE {
        static ArrayList<Object> encode(ArrayList<Object> column) throws Exception {
            if ((column.size() == 0) || (column.get(0) instanceof String)) {
                return column
            }
            ArrayList<Object> out = new ArrayList<>()

            BigDecimal last = (BigDecimal) column.get(0)
            int length = 1
            for (int i = 1; i < column.size(); i++) {
                BigDecimal ind = (BigDecimal) column.get(i)
                if (((ind != null) && ind == last)
                        || ((ind == null) && (last == null))) {
                    length++
                } else {
                    out.add(new BigDecimal(length))
                    out.add(last)
                    last = ind
                    length = 1
                }
            }
            // add last record
            out.add(new BigDecimal(length))
            out.add(last)
            return out
        }

        static ArrayList<Object> decode(ArrayList<Object> column) throws Exception {
            if ((column.size() == 0) || (column.get(0) instanceof String)) {
                return column
            }
            ArrayList<Object> out = new ArrayList<>()
            int length
            BigDecimal value
            for (int i = 0; i < column.size(); i++) {
                length = ((BigDecimal) column.get(i)).intValue()
                i++
                value = (BigDecimal) column.get(i)
                for (int j = 0; j < length; j++) {
                    out.add(value)
                }
            }

            return out
        }

    }

    class JsonFile {
        static void loadRowFormat(InputStream input, CSTore csTore) throws Exception {
            try {
                JsonFactory f = new JsonFactory()
                JsonParser p = f.createParser(input)
                csTore.isColumnar = false
                int i = -1
                int j = -1

                // loop until token equal to "}"
                JsonToken t = p.nextToken()
                String key = ""
                boolean isMeta = false
                while (t != null) {
                    // begin of record
                    if (t == JsonToken.START_OBJECT) {
                        i++
                        csTore.data.add(new ArrayList<>())
                        j = -1
                    } else if (t == JsonToken.END_OBJECT) {
                        //fill empty value
                        if (isMeta) {
                            i = -1
                            j = -1
                            Iterator iterator1 = csTore.columns.iterator()
                            while (iterator1.hasNext()) {
                                String str = (String) iterator1.next()
                                if (str == null || str.isEmpty() || str == "meta") {
                                    iterator1.remove()
                                }
                            }
                            Iterator iterator2 = csTore.data.iterator()
                            while (iterator2.hasNext()) {
                                ArrayList list = (ArrayList) iterator2.next()
                                if (list.size() == 0) {
                                    iterator2.remove()
                                }
                            }
                            isMeta = false
                        }
                    } else if (t == JsonToken.FIELD_NAME) {
                        key = p.getCurrentName()
                        if (!isMeta) isMeta = key.equalsIgnoreCase("meta")
                        if (isMeta) {
                        } else if (key.equalsIgnoreCase("data")) {
                            i = -1
                            j = -1
                            //fill empty value
                            Iterator iterator1 = csTore.columns.iterator()
                            while (iterator1.hasNext()) {
                                String str = (String) iterator1.next()
                                if (str == null || str.isEmpty() || str == "meta") {
                                    iterator1.remove()
                                }
                            }
                            Iterator iterator2 = csTore.data.iterator()
                            while (iterator2.hasNext()) {
                                ArrayList list = (ArrayList) iterator2.next()
                                if (list.size() == 0) {
                                    iterator2.remove()
                                }
                            }
                        } else {
                            j++
                            if (i == 0) {
                                csTore.columns.add(key)
                            }
                            if (j >= csTore.columns.size()) {
                                throw new Exception("error: field overflow with header at row: " + i
                                        + ", field: " + key)
                            } else if (key.compareToIgnoreCase(csTore.columns.get(j)) != 0) {
                                throw new Exception("error: field mismatch with header at row: " + i
                                        + ", field: " + key
                                        + ", should be: " + csTore.columns.get(j))
                            }
                        }
                    } else if (t == JsonToken.VALUE_NUMBER_FLOAT || t == JsonToken.VALUE_NUMBER_INT) {
                        if (isMeta) {
                            csTore.meta.put(key, p.getDecimalValue())
                        } else {
                            csTore.data.get(i).add(p.getDecimalValue())
                        }
                    } else if (t == JsonToken.VALUE_NULL) {
                        if (isMeta) {
                            csTore.meta.put(key, null)
                        } else {
                            csTore.data.get(i).add(null)
                        }
                    } else if (t == JsonToken.VALUE_STRING) {
                        if (isMeta) {
                            csTore.meta.put(key, p.getValueAsString())
                        } else {
                            csTore.data.get(i).add(p.getValueAsString())
                            if (!csTore.columnsType.containsKey(j)) {//写入当前列的数据类型true表示是数值型
                                csTore.columnsType.put(j, true)
                            }
                        }
                    }
                    t = p.nextToken()
                }
                p.close()
            } catch (IOException e) {
                throw new Exception(e)
            }
        }

        static void saveRowFormat(OutputStream out, CSTore csTore) throws Exception {

            try {
                JsonFactory f = new JsonFactory()
//            File jsonFile = new File(dest)
                JsonGenerator g = f.createGenerator(out, JsonEncoding.UTF8)

                if (csTore.meta.size() > 0) {
                    g.writeStartObject()
                    // write meta object
                    g.writeFieldName("meta")
                    g.writeStartObject()
                    for (Map.Entry<String, Object> entry : csTore.meta.entrySet()) {
                        if (entry.getValue() instanceof String) {
                            g.writeStringField(entry.getKey(), (String) entry.getValue())
                        } else if (entry.getValue() instanceof BigDecimal) {
                            g.writeNumberField(entry.getKey(), (BigDecimal) entry.getValue())
                        } else if (entry.getValue() == null) {
                            g.writeNullField(entry.getKey())
                        }
                    }
                    g.writeEndObject()

                    // write data object start
                    g.writeFieldName("data")
                    g.writeStartArray()
                } else {
                    g.writeStartArray()
                }

                for (ArrayList<Object> row : csTore.data) {
                    g.writeStartObject()
                    for (int i = 0; i < row.size(); i++) {
                        Object o = row.get(i)
                        if (o == null) {
                            g.writeNullField(csTore.columns.get(i))
                        } else if (o instanceof String) {
                            g.writeStringField(csTore.columns.get(i), (String) row.get(i))
                        } else {
                            g.writeNumberField(csTore.columns.get(i), (BigDecimal) row.get(i))
                        }
                    }
                    g.writeEndObject()
                }

                if (csTore.meta.size() > 0) {
                    // write data object end
                    g.writeEndArray()
                    g.writeEndObject()
                } else {
                    g.writeEndArray()
                }
                g.close()

            } catch (IOException e) {
                throw new Exception(e)
            }
        }

        static void loadColFormat(InputStream inputStream, CSTore csTore) throws Exception {
            try {
                JsonFactory f = new JsonFactory()
//            File jsonFile = new File(source)
                JsonParser p = f.createParser(inputStream)
                boolean isMeta = false
                csTore.isColumnar =true
                String key = ""
                JsonToken t = p.nextToken()
                int i = -1

                while (t != null) {
                    if (t == JsonToken.START_OBJECT) {
                    } else if (t == JsonToken.END_OBJECT) {
                        if (isMeta) {
                            isMeta = false
                            i = -1
                        }
                    } else if (t == JsonToken.START_ARRAY) {
                        csTore.csTore.add(new ArrayList<>())

                    } else if (t == JsonToken.FIELD_NAME) {
                        key = p.getCurrentName()
                        if (!isMeta) isMeta = key.equalsIgnoreCase("meta")
                        if (isMeta) {
                        } else {
                            csTore.columns.add(p.getCurrentName())
                            i++
                        }
                    } else if (t == JsonToken.VALUE_NUMBER_FLOAT || t == JsonToken.VALUE_NUMBER_INT) {
                        if (isMeta) {
                            csTore.meta.put(key, p.getValueAsString())
                        } else {
                            csTore.csTore.get(i).add(p.getDecimalValue())
                        }

                    } else if (t == JsonToken.VALUE_NULL) {
                        if (isMeta) {
                            csTore.meta.put(key, p.getValueAsString())
                        } else {
                            csTore.csTore.get(i).add(null)
                        }
                    } else if (t == JsonToken.VALUE_STRING) {
                        if (isMeta) {
                            csTore.meta.put(key, p.getValueAsString())
                        } else {
                            csTore.csTore.get(i).add(p.getValueAsString())
                            if (!csTore.columnsType.containsKey(i)) {//写入当前列的数据类型true表示是数值型
                                csTore.columnsType.put(i, true)
                            }
                        }
                    }
                    t = p.nextToken()
                }
                p.close()
            } catch (IOException e) {
                throw new Exception(e)
            }
        }

        static void saveColFormat(ByteArrayOutputStream out, CSTore csTore) throws Exception {

            try {
                JsonFactory f = new JsonFactory()
//            File jsonFile = new File(dest)
                JsonGenerator g = f.createGenerator(out, JsonEncoding.UTF8)
                g.writeStartObject()
                if (csTore.meta.size() > 0) {
                    g.writeFieldName("meta")
                    g.writeStartObject()
                    for (Map.Entry<String, Object> entry : csTore.meta.entrySet()) {
                        if (entry.getValue() instanceof String) {
                            g.writeStringField(entry.getKey(), (String) entry.getValue())
                        } else if (entry.getValue() instanceof BigDecimal) {
                            g.writeNumberField(entry.getKey(), (BigDecimal) entry.getValue())
                        } else if (entry.getValue() == null) {
                            g.writeNullField(entry.getKey())
                        }
                    }
                    g.writeEndObject()
                }

                for (int i = 0; i < csTore.csTore.size(); i++) {
//                g.writeStartObject()
                    g.writeArrayFieldStart(csTore.columns.get(i))
                    for (Object item : csTore.csTore.get(i)) {
                        if (item == null) {
                            g.writeNull()
                        } else if (item instanceof String) {
                            g.writeString((String) item)
                        } else {
                            g.writeNumber((BigDecimal) item)
                        }
                    }
                    g.writeEndArray()
//                g.writeEndObject()
                }
                g.writeEndObject()
                g.close()
            } catch (IOException e) {
                throw new Exception(e)
            }
        }
    }
}
