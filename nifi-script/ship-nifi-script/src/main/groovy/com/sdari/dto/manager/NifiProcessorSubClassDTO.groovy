package com.sdari.dto.manager

import groovy.json.JsonBuilder
import lombok.Data

import java.sql.ResultSet

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/10 17:38
 */
@Data
class NifiProcessorSubClassDTO {
    private Integer processor_id
    private Integer route_id
    private String sub_full_path
    private String sub_script_name
    private String sub_script_text
    private String sub_script_desc
    private String sub_running_way
    private Integer running_order
    private String status

    static List<NifiProcessorSubClassDTO> createDto(ResultSet res) throws Exception {
        try {
            def NifiProcessorSubClasses = []
            while (res.next()) {
                NifiProcessorSubClassDTO dto = new NifiProcessorSubClassDTO()
                /*dto.setProperty('processor_id', res.getInt('processor_id'))
                dto.setProperty('route_id', res.getInt('route_id'))
                dto.setProperty('sub_full_path', res.getString('sub_full_path'))
                dto.setProperty('sub_script_name', res.getString('sub_script_name'))
                dto.setProperty('sub_running_way', res.getString('sub_running_way'))
                dto.setProperty('running_order', res.getInt('running_order'))
                dto.setProperty('status', res.getString('status'))*/

                dto.processor_id = res.getInt('processor_id')
                dto.route_id = res.getInt('route_id')
                dto.sub_full_path = res.getString('sub_full_path')
                dto.sub_script_name = res.getString('sub_script_name')
                dto.sub_script_text = res.getString('sub_script_text')
                dto.sub_script_desc = res.getString('sub_script_desc')
                dto.sub_running_way = res.getString('sub_running_way')
                dto.running_order = res.getInt('running_order')
                dto.status = res.getString('status')
                NifiProcessorSubClasses.add(dto)
            }
            NifiProcessorSubClasses
        } catch (Exception e) {
            throw new Exception("NifiProcessorSubClassDTO createDto has an error", e)
        }
    }

    static JsonBuilder jsonBuilderDto(NifiProcessorSubClassDTO dto) throws Exception {
        try {
            def builder = new JsonBuilder()
            builder dto.collect(), { NifiProcessorSubClassDTO d ->
                processor_id d.processor_id
                route_id d.route_id
                sub_full_path d.sub_full_path
                sub_script_name d.sub_script_name
                sub_running_way d.sub_running_way
                running_order d.running_order
                status d.status
            }
            builder
        } catch (Exception e) {
            throw new Exception("NifiProcessorSubClassDTO jsonBuilderDto has an error", e)
        }
    }
}
