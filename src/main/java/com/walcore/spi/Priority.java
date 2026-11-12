package com.walcore.spi;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 插件优先级。值越大越优先。
 *
 * <p>仲裁规则（ARCH §10.3 步骤 5）：
 * <ol>
 *   <li>若 {@code WalConfig} 显式指定了 {@code wal.spi.<type>=<id>}，<b>只考虑该 id</b>，
 *       其余候选即使优先级更高也忽略；找不到则按 {@code onPluginLoadFailure} 处理。</li>
 *   <li>否则按 {@code @Priority.value()} 降序；<b>同分按 {@code id()} 字典序升序</b>
 *       （保证同一 classpath 下结果可复现，不受 {@code ServiceLoader} 迭代顺序影响）。</li>
 * </ol>
 *
 * <p>约定区间：内置实现 {@code 0}；厂商推荐实现 {@code 10~50}；用户自定义覆盖 {@code 60~100}。
 * 未标注的类等价于 {@code @Priority(0)}。
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Priority {

    /**
     * 优先级的整数值，建议落在 0..100。
     *
     * @return 优先级
     */
    int value();
}
