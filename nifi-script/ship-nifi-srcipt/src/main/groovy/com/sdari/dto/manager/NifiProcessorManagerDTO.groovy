package com.sdari.dto.manager

import lombok.Data

import java.sql.ResultSet

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/10 17:38
 */
@Data
class NifiProcessorManagerDTO {
    private Integer processor_id
    private String processor_name
    private Integer sid
    private String full_path
    private String script_name
    private String is_need_rules
    private String status
    private String processor_desc

    static NifiProcessorManagerDTO createDto(ResultSet res) throws Exception {
        try {
            NifiProcessorManagerDTO dto = new NifiProcessorManagerDTO()
            while (res.next()) {
                dto.setProperty('processor_id', res.getInt('processor_id'))
                dto.setProperty('processor_name', res.getString('processor_name'))
                dto.setProperty('sid', res.getInt('sid'))
                dto.setProperty('full_path', res.getString('full_path'))
                dto.setProperty('script_name', res.getString('script_name'))
                dto.setProperty('is_need_rules', res.getString('is_need_rules'))
                dto.setProperty('status', res.getString('status'))
                dto.setProperty('processor_desc', res.getString('processor_desc'))
            }
            dto
        } catch (Exception e) {
            throw new Exception("NifiProcessorManagerDTO createDto has an error", e)
        }
    }
}
