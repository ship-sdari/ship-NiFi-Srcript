package com.sdari.dto.manager

import lombok.Data

import java.sql.ResultSet

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/10 17:38
 */
@Data
class NifiProcessorRoutesDTO {
    private Integer processor_id
    private Integer route_id
    private String route_name
    private String route_desc
    private String route_running_way
    private String status

    static List<NifiProcessorRoutesDTO> createDto(ResultSet res) throws Exception {
        try {
            def nifiProcessorRoutes = []
            while (res.next()) {
                NifiProcessorRoutesDTO dto = new NifiProcessorRoutesDTO()
                dto.setProperty('processor_id', res.getInt('processor_id'))
                dto.setProperty('route_id', res.getInt('route_id'))
                dto.setProperty('route_name', res.getString('route_name'))
                dto.setProperty('route_desc', res.getString('route_desc'))
                dto.setProperty('route_running_way', res.getString('route_running_way'))
                dto.setProperty('status', res.getString('status'))
                nifiProcessorRoutes.add(dto)
            }
            nifiProcessorRoutes
        } catch (Exception e) {
            throw new Exception("NifiProcessorRoutesDTO createDto has an error", e)
        }
    }
}
