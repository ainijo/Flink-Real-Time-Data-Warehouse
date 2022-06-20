package org.example.bean;

/*
    自定义注解，让对应的字段不参与序列化
 */

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

// 作用域
@Target(ElementType.FIELD)
// 保留
@Retention(RUNTIME)
// 注意是 @interface
public @interface TransientSink {

}
