package com.walcore.api;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link WalException} 族的<b>契约测试</b>。
 *
 * <h2>为什么错误码值得一套专门的测试</h2>
 * {@code errorCode()} 是使用方<b>唯一的机器可读失败判据</b>——调用方会写
 * {@code if ("WAL_DISK_FULL".equals(e.errorCode())) alert();}，也会把它当告警标签、
 * 当重试策略的 switch key、当 SLO 的分母。它一旦被"顺手改个名"，对方的告警规则会**静默失效**
 * （既不报错也不触发），比抛异常难查一个数量级。
 *
 * <p>所以这里的断言是"逐字相等"而不是"非空"：把每个错误码当成**对外协议**锁死。
 */
@DisplayName("WalException 错误码契约")
class WalExceptionContractTest {

    /**
     * 全量错误码 → 构造器。新增异常子类时必须在这里同步登记，否则 {@link #allCodesAreRegistered()}
     * 会以"数量不符"失败，逼迫实现者显式处理（这是有意的强约束）。
     *
     * @return 参数流（错误码 + 工厂）
     */
    static Stream<Arguments> codeTable() {
        return Stream.of(
                Arguments.of("WAL_CLOSED", (Supplier<WalException>)
                        () -> new WalException.ClosedException("实例已关闭")),
                Arguments.of("WAL_ALREADY_OPEN", (Supplier<WalException>)
                        () -> new WalException.AlreadyOpenException("目录已被占用")),
                Arguments.of("WAL_OVERLOADED", (Supplier<WalException>)
                        () -> new WalException.OverloadedException("过载")),
                Arguments.of("WAL_DISK_FULL", (Supplier<WalException>)
                        () -> WalException.OverloadedException.diskFull("磁盘满")),
                Arguments.of("WAL_BACKPRESSURE", (Supplier<WalException>)
                        () -> new WalException.BackpressureException("背压超时")),
                Arguments.of("WAL_OFFHEAP_EXHAUSTED", (Supplier<WalException>)
                        () -> new WalException.OffHeapExhaustedException("堆外预算耗尽")),
                Arguments.of("WAL_IO", (Supplier<WalException>)
                        () -> new WalException.IoException("写失败")),
                Arguments.of("WAL_TIMEOUT", (Supplier<WalException>)
                        () -> new WalException.TimeoutException("等待持久化超时")),
                Arguments.of("WAL_SEGMENT_MISSING", (Supplier<WalException>)
                        () -> new WalException.SegmentMissingException("段 [10,20] 缺失")),
                Arguments.of("WAL_CORRUPTED", (Supplier<WalException>)
                        () -> new WalException.CorruptedException("CRC 不符")),
                Arguments.of("WAL_INCOMPATIBLE_FORMAT", (Supplier<WalException>)
                        () -> new WalException.IncompatibleFormatException("格式版本过高")),
                Arguments.of("WAL_RECOVERY_FAILED", (Supplier<WalException>)
                        () -> new WalException.RecoveryFailedException("恢复失败", null)),
                Arguments.of("WAL_PARTITION", (Supplier<WalException>)
                        () -> new WalException.PartitionException("分区不存在")),
                Arguments.of("WAL_PLUGIN", (Supplier<WalException>)
                        () -> new WalException.PluginLoadException("插件 init 失败")),
                Arguments.of("WAL_CODEC", (Supplier<WalException>)
                        () -> new WalException.CodecException("编码器输出长度不符")),
                Arguments.of("WAL_TAILER_OVERFLOW", (Supplier<WalException>)
                        () -> new WalException.TailerOverflowException("订阅者落后过多")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("codeTable")
    @DisplayName("每个错误码逐字稳定，且消息原样保留")
    void errorCodeIsExactAndMessagePreserved(String expectedCode, Supplier<WalException> factory) {
        WalException e = factory.get();
        assertEquals(expectedCode, e.errorCode(), "错误码是不允许变的对外协议");
        assertNotNull(e.getMessage(), "异常必须带消息，否则运维日志里只剩一个错误码");
        assertFalse(e.getMessage().isEmpty(), "异常消息不得为空串");
    }

    @Test
    @DisplayName("错误码全局唯一（无两个异常共用一个码）")
    void allCodesAreDistinct() {
        Set<String> seen = new HashSet<>();
        List<String> duplicates = new ArrayList<>();
        for (Arguments args : codeTable().toList()) {
            String code = (String) args.get()[0];
            if (!seen.add(code)) {
                duplicates.add(code);
            }
        }
        assertTrue(duplicates.isEmpty(),
                "错误码出现重复，使用方无法靠 errorCode 区分处置动作: " + duplicates);
        assertEquals(16, seen.size(), "错误码总数应为 16，实际 = " + seen.size());
    }

    @Test
    @DisplayName("错误码格式统一：大写下划线、WAL_ 前缀")
    void errorCodeFormatIsUniform() {
        for (Arguments args : codeTable().toList()) {
            String code = (String) args.get()[0];
            assertTrue(code.startsWith("WAL_"),
                    "错误码必须以 WAL_ 开头（便于日志里 grep）: " + code);
            assertTrue(code.matches("WAL_[A-Z0-9_]+"),
                    "错误码只允许大写字母/数字/下划线: " + code);
        }
    }

    @Test
    @DisplayName("同一错误码两次构造结果相同（不依赖实例状态）")
    void errorCodeIsStateless() {
        for (Arguments args : codeTable().toList()) {
            @SuppressWarnings("unchecked")
            Supplier<WalException> factory = (Supplier<WalException>) args.get()[1];
            assertEquals(factory.get().errorCode(), factory.get().errorCode(),
                    "同一子类的两次实例必须给出相同错误码");
        }
    }

    @Test
    @DisplayName("cause 链完整保留：包装后仍能 rootCause 到原始异常")
    void causeChainIsPreserved() {
        IllegalStateException root = new IllegalStateException("底层原因");
        WalException alpha = new WalException.IoException("force 失败", root);
        WalException beta = new WalException.CodecException("包装一层", alpha);

        assertSame(root, beta.getCause().getCause(), "两层包装后必须还能拿到最初的根因");
        WalException phi = new WalException.PluginLoadException("插件失败", root);
        assertSame(root, phi.getCause());
        WalException psi = new WalException.CorruptedException("损坏", root);
        assertSame(root, psi.getCause());
        WalException omega = new WalException.BackpressureException("背压", root);
        assertSame(root, omega.getCause());
        WalException adr = new WalException.OverloadedException("过载", root);
        assertSame(root, adr.getCause());
    }

    @Test
    @DisplayName("单参数构造的 cause 为 null（不要凭空造 cause）")
    void singleArgConstructorHasNoCause() {
        assertNull(new WalException.ClosedException("x").getCause());
        assertNull(new WalException.AlreadyOpenException("x").getCause());
        assertNull(new WalException.TimeoutException("x").getCause());
        assertNull(new WalException.SegmentMissingException("x").getCause());
        assertNull(new WalException.IncompatibleFormatException("x").getCause());
        assertNull(new WalException.PartitionException("x").getCause());
        assertNull(new WalException.TailerOverflowException("x").getCause());
    }

    @Test
    @DisplayName("diskFull 与普通 Overloaded 是两个码：磁盘满必须能被单独识别")
    void diskFullIsDistinguishableFromOverload() {
        WalException full = WalException.OverloadedException.diskFull("磁盘满");
        WalException busy = new WalException.OverloadedException("过载");
        assertNotEquals(full.errorCode(), busy.errorCode(),
                "ARCH §14 #1 要求磁盘满单独告警，两者共用错误码会让告警规则失效");
        assertEquals("WAL_DISK_FULL", full.errorCode());
        assertEquals("WAL_OVERLOADED", busy.errorCode());
        assertSame(full.getClass(), busy.getClass(),
                "两者是同一子类（处置动作相同：拒写、保读），只靠 errorCode 分流");
        assertNotNull(WalException.OverloadedException.diskFull("x", new java.io.IOException()).getCause());
    }

    @Test
    @DisplayName("WalException 是 RuntimeException 且抽象：只能抛具体子类")
    void isUncheckedAndAbstract() {
        assertTrue(RuntimeException.class.isAssignableFrom(WalException.class),
                "必须是非受检异常，否则 append 路径要被迫 try/catch");
        assertTrue(java.lang.reflect.Modifier.isAbstract(WalException.class.getModifiers()),
                "基类抽象，避免有人直接抛 new WalException");
        assertThrows(RuntimeException.class, () -> {
            throw new WalException.ClosedException("x");
        });
    }

    @Test
    @DisplayName("toString 含具体类名（日志里能直接定位是哪种失败）")
    void toStringCarriesTypeAndMessage() {
        WalException e = new WalException.SegmentMissingException("段 [3,7] 缺失");
        String s = e.toString();
        assertTrue(s.contains("SegmentMissingException"), "toString 必须含具体类名: " + s);
        assertTrue(s.contains("段 [3,7] 缺失"), "toString 必须含消息: " + s);
    }
}
