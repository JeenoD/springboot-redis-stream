package com.jeeno.springbootredisstream.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author JEENO
 * @version 0.0.1
 * @date 2021/3/31 15:52
 */
@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class UserDO {

    private String name;
}
