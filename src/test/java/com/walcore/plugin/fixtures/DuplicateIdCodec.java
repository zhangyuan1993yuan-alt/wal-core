package com.walcore.plugin.fixtures;

/**
 * 测试插件：id 与 {@link PassThroughCodec} <b>完全相同</b>，用于验证"同名 id 去重"。
 *
 * <p>它与 {@link PassThroughCodec} 是两个不同的类（不同 class name），因此
 * {@code ServiceLoader} 会把两个都交出来——正好构造出"两个来源提供同一个 id"的冲突，
 * 而框架必须只保留一个并记录 {@code duplicate} 诊断。
 */
public class DuplicateIdCodec extends PassThroughCodec {

    @Override
    public String id() {
        return PassThroughCodec.ID;
    }

    @Override
    public int codecId() {
        return PassThroughCodec.CODEC_ID;
    }
}
