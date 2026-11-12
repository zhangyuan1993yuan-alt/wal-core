package com.walcore.spi;

import com.walcore.api.WalVersion;

/**
 * 所有插件的公共基座（五类 SPI + 快照契约都继承本接口）。
 *
 * <p><b>插件必须遵守的三条铁律</b>：
 * <ol>
 *   <li>JAR 内<b>不得</b>打包 {@code com.walcore.*} 的类。shade 打包会把 SPI 接口复制一份，
 *       导致宿主的 {@code instanceof} 永远为 false、{@code ServiceLoader} 找不到却也不报错
 *       ——这是本项目最容易踩的坑（ARCH §10.4.2）。插件必须以 {@code provided} 方式依赖
 *       wal-core。</li>
 *   <li>{@link #apiVersion()} 必须等于 {@link WalVersion#API_VERSION}，否则被剔除。</li>
 *   <li>{@link #close()} 必须<b>幂等且可重入</b>：热切换失败的路径上可能会关闭两次。</li>
 * </ol>
 *
 * <p><b>生命周期</b>：{@code ServiceLoader} 实例化（无参构造，因此实现类必须有无参构造器）
 * → {@link #init(PluginContext)} → 被框架使用 → {@link #close()}。
 * 框架保证 {@code init()} 成功后才开始使用；{@code init()} 抛异常的实现会被剔除，
 * 且<b>不会污染</b>其它已成功的实现。
 */
public interface WalPlugin extends AutoCloseable {

    /**
     * 插件唯一标识，全局不得重复。内置实现以 {@code "builtin:"} 前缀。
     *
     * <p>该字符串是配置契约的一部分（{@code wal.spi.codec=builtin:raw}），
     * 一旦发布<b>不得修改</b>。
     *
     * @return 插件 id，非空
     */
    String id();

    /**
     * 插件声明的 API 版本，必须等于 {@link WalVersion#API_VERSION}。
     *
     * @return API 版本
     */
    int apiVersion();

    /**
     * 初始化。框架在此注入配置、指标注册中心、类加载器等能力。
     *
     * <p>实现应在此完成"自重检"（如依赖的原生库是否可用）。抛异常即视为不可用。
     *
     * @param context 插件可见的核心上下文
     * @throws PluginException 初始化失败
     */
    void init(PluginContext context) throws PluginException;

    /**
     * 释放资源。必须幂等、可重入；不得阻塞超过 100ms（会在组提交线程上调用）。
     */
    @Override
    void close();
}
