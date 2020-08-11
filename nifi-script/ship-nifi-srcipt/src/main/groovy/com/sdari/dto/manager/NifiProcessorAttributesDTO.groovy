package com.sdari.dto.manager

import lombok.Data

import java.sql.ResultSet

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/10 17:38
 */
@Data
class NifiProcessorAttributesDTO {
    private Integer processor_id
    private String attribute_name
    private String attribute_value
    private String attribute_type
    private String status

    static List<NifiProcessorAttributesDTO> createDto(ResultSet res) throws Exception{
        try {
            def nifiProcessorAttributes = []
            while (res.next()) {
                NifiProcessorAttributesDTO dto = new NifiProcessorAttributesDTO()
                dto.setProperty('processor_id', res.getInt('processor_id'))
                dto.setProperty('attribute_name', res.getString('attribute_name'))
                dto.setProperty('attribute_value', res.getString('attribute_value'))
                dto.setProperty('attribute_type', res.getString('attribute_type'))
                dto.setProperty('status', res.getString('status'))
                nifiProcessorAttributes.add(dto)
            }
            nifiProcessorAttributes
        } catch (Exception e) {
            throw new Exception("NifiProcessorAttributesDTO createDto has an error", e)
        }

    }
}
