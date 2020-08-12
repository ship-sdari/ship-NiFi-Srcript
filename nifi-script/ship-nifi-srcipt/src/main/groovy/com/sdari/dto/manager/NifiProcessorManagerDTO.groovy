package com.sdari.dto.manager

import lombok.Data

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
    private String status
    private String processor_desc
}
