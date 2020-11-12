package com.sdari.dto.manager

import lombok.Data

import java.sql.ResultSet

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/10 17:38
 */
@Data
class NifiProcessorConnectionDTO {
    private Integer id
    private String url
    private String username
    private String password
    private String driver
    private String status

    static Map<Integer, NifiProcessorConnectionDTO> createDto(ResultSet res) throws Exception {
        try {
            def nifiProcessorConnection = [:]
            while (res.next()) {
                NifiProcessorConnectionDTO dto = new NifiProcessorConnectionDTO()
                dto.setProperty('id', res.getInt('id'))
                dto.setProperty('url', res.getString('url'))
                dto.setProperty('username', res.getString('username'))
                dto.setProperty('password', res.getString('password'))
                dto.setProperty('driver', res.getString('driver'))
                dto.setProperty('status', res.getString('status'))
                nifiProcessorConnection.put(dto.id, dto)
            }
            nifiProcessorConnection as Map<Integer, NifiProcessorConnectionDTO>
        } catch (Exception e) {
            throw new Exception("NifiProcessorConnectionDTO createDto has an error", e)
        }

    }
}
