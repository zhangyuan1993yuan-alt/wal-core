package com.walcore.spi;

import com.walcore.api.WalConfig;
import com.walcore.internal.metrics.MetricsRegistry;

import java.nio.file.Path;

/**
 * 插件可见的核心上下文。
 *
 * <p><b>刻意只暴露必要能力</b>：不给插件全量 {@code Wal} 句柄。原因很实际——插件若能拿到
 * {@code Wal}，就能在组提交线程里调用 {@code append()}，瞬间破坏"LSN 分配必须在分区写锁内"
 * 这条关键不变量，且极难排查。插件要写日志请通过业务侧的回调，不要从框架内部反推。
 */
public interface PluginContext {

    /**
     * 本次实例的不可变配置。
     *
     * @return 配置
     */
    WalConfig config();

    /**
     * 启动时的分区数（不含运行期动态新增的分区）。
     *
     * @return 分区数
     */
    int partitionCount();

    /**
     * 指标注册中心。插件只允许注册/更新<b>自身</b>的指标（约定前缀 {@code wal.plugin.<id>.}）。
     *
     * @return 注册中心
     */
    MetricsRegistry metrics();

    /**
     * 加载本插件的 ClassLoader（通常为 {@code PluginClassLoader}），插件加载自身资源时使用，
     * 避免误用线程上下文加载器导致类版本错乱。
     *
     * @return ClassLoader
     */
    ClassLoader pluginClassLoader();

    /**
     * 日志根目录（与 {@code WalConfig.baseDir()} 相同）。
     *
     * @return 根目录
     */
    Path baseDir();

    /**
     * 记录一条告警。框架会把 {@code code} 作为指标标签累加，并把消息交给内部日志门面。
     *
     * @param code 稳定的告警码（如 {@code wal.plugin.codec.selfcheck.failed}）
     * @param message 人可读的描述
     * @param cause 根因，可为 null
     */
    void warn(String code, String message, Throwable cause);
}
