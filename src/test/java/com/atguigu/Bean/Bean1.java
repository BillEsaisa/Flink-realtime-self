package com.atguigu.Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PackageName:com.atguigu.Bean
 * @ClassNmae:Bean1
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/20 17:09
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Bean1 {
    private String id;
    private String name;
    private Long ts;
}
