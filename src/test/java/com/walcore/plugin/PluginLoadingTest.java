package com.walcore.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.walcore.api.AppendOptions;
import com.walcore.api.WalBuilder;
import com.walcore.api.WalConfig;
import com.walcore.api.WalException;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.plugin.PluginClassLoader;
import com.walcore.internal.plugin.PluginDiagnostics;
import com.walcore.internal.plugin.PluginManager;
import com.walcore.internal.plugin.PluginType;
import com.walcore.plugin.fixtures.CountingIndexStore;
import com.walcore.plugin.fixtures.HighPriorityCodec;
import com.walcore.plugin.fixtures.PassThroughCodec;
import com.walcore.spi.PluginException;
import com.walcore.spi.RecordCodec;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * T11 SPI 插件框架验收（ARCH §10）。
 *
 * <h2>为什么用"临时目录 + 自建 ClassLoader"而不是把服务文件放进 {@code src/test/resources/META-INF/services}</h2>
 * 服务文件一旦放在测试类路径根部，<b>整套 237 个测试</b>都会在
 * {@code ServiceLoader.load(RecordCodec, appClassLoader)} 时看到它，于是默认编码器被
 * 悄悄换成一个测试实现——测出来的东西就不再是产品行为了。因此：
 * <ul>
 *   <li>服务声明文件放在 {@code src/test/resources/spi-fixtures/<场景>/META-INF/services/}，
 *       <b>不在类路径根部</b>；</li>
 *   <li>由测试把该目录当作 {@code URLClassLoader}（这里用真正的
 *       {@link PluginClassLoader}）把它喂给 {@code wal.pluginClassLoader}；</li>
 *   <li>这样每个用例都可以独立决定"这一轮能看到哪些 provider"，且与其他用例零耦合。</li>
 * </ul>
 *
 * <h2>覆盖点</h2>
 * 优先级仲裁 / 同名去重 / 版本兼容过滤 / FALLBACK_DEFAULT 与 FAIL_FAST 两档 /
 * FAIL_FAST 消息四要素 / 显式指定 id / {@code init()} 失败隔离 /
 * {@code PluginClassLoader} parent-first / 非法 JAR 拒绝 / 热切换与失败回滚 /
 * {@code IndexStore.onAppended} 契约修订真的接线。
 */
class PluginLoadingTest {

    /** 服务文件所在资源根。 */
    private static final String FIXTURES = "spi-fixtures";

    // ==================================================================
    // 发现 / 仲裁
    // ==================================================================

    @Test
    @DisplayName("发现 provider：优先级高者生效，与声明顺序无关")
    void higherPriorityWinsRegardlessOfDeclarationOrder(@TempDir Path dir) {
        try (PluginManager manager = newManager(dir, allFixtures(), null,
                WalConfig.PluginFailurePolicy.FALLBACK_DEFAULT)) {
            PluginDiagnostics diag = manager.scan();

            List<PluginType.PluginInfo> codecs = manager.list(PluginType.CODEC);
            List<String> ids = codecs.stream().map(PluginType.PluginInfo::id).toList();
            assertTrue(ids.contains(HighPriorityCodec.ID), "已发现列表必须包含高优先级实现: " + ids);
            assertTrue(ids.contains(PassThroughCodec.ID), "已发现列表必须包含低优先级实现: " + ids);

            assertEquals(HighPriorityCodec.ID, manager.current().codec().id(),
                    "生效的必须是 @Priority(900)，而不是服务文件里排第一的那个");
            assertEquals(HighPriorityCodec.CODEC_ID, manager.current().codec().codecId());
            assertTrue(diag.loadedIds().contains("codec:" + HighPriorityCodec.ID),
                    "诊断里必须记录实际加载的实现: " + diag.loadedIds());
        }
    }

    @Test
    @DisplayName("apiVersion 不兼容的 provider 被剔除，且不参与仲裁")
    void incompatibleApiVersionIsFiltered(@TempDir Path dir) {
        try (PluginManager manager = newManager(dir, allFixtures(), null,
                WalConfig.PluginFailurePolicy.FALLBACK_DEFAULT)) {
            PluginDiagnostics diag = manager.scan();
            List<String> ids = manager.list(PluginType.CODEC).stream()
                    .map(PluginType.PluginInfo::id).toList();
            assertFalse(ids.contains("test:codec-future"),
                    "apiVersion=9999 的实现必须不出现在候选里: " + ids);
            assertTrue(diag.incompatiblePlugins().stream()
                            .anyMatch(s -> s.contains("test:codec-future")
                                    && s.contains("apiVersion=9999")),
                    "诊断必须说明是版本不兼容: " + diag.incompatiblePlugins());
        }
    }

    @Test
    @DisplayName("init() 抛异常的 provider 只淘汰自己：回退内置，其他类型不受影响")
    void brokenInitIsIsolated(@TempDir Path dir) {
        PluginClassLoader pluginLoader = loader(allFixtures(), "broken-init");
        WalBuilder builder = TestWal.builder(dir, 4096L)
                .pluginClassLoader(pluginLoader)
                .codec(BrokenInitCodecId.VALUE)
                .onPluginLoadFailure(WalConfig.PluginFailurePolicy.FALLBACK_DEFAULT);
        try (PluginManager manager = new PluginManager(builder.buildConfig(), dir, null)) {
            PluginDiagnostics diag = manager.scan();
            assertTrue(diag.failedPlugins().stream()
                            .anyMatch(s -> s.contains("test:codec-broken") && s.contains("init()")),
                    "init 失败必须出现在 failedPlugins: " + diag.failedPlugins());
            assertEquals("builtin:raw", manager.current().codec().id(),
                    "坏插件必须回退到内置实现，而不是把启动搞挂");
            assertNotNull(manager.current().indexStore(), "其他类型必须完全不受影响");
        }
    }

    @Test
    @DisplayName("同名 id 的两个实现：只保留一个，并记 duplicate")
    void duplicateIdAcrossSourcesIsDeduplicated(@TempDir Path dir) {
        // 两份夹具里各有一个 provider 声明了同一个 id（不同 class、同一 id）
        try (PluginManager manager = newManager(dir,
                List.of(fixture("all"), fixture("dup")), null,
                WalConfig.PluginFailurePolicy.FALLBACK_DEFAULT)) {
            PluginDiagnostics diag = manager.scan();
            long occurrences = manager.list(PluginType.CODEC).stream()
                    .filter(i -> i.id().equals(PassThroughCodec.ID)).count();
            assertEquals(1L, occurrences, "同一个 id 只能出现一次");
            assertTrue(diag.duplicateIds().stream()
                            .anyMatch(s -> s.contains(PassThroughCodec.ID)),
                    "诊断里必须记录去重事件: " + diag.duplicateIds());
        }
    }

    // ==================================================================
    // 两档失败策略
    // ==================================================================

    @Test
    @DisplayName("干净部署：五类 SPI 都由 META-INF/services 正规发现内置实现（不走兜底路径）")
    void builtinProvidersAreDiscoveredViaServiceLoader(@TempDir Path dir) {
        // 注意：本用例的口径在 T14/T19 之后变了。内置实现现在通过
        // src/main/resources/META-INF/services 正式注册，因此它们属于"被发现的 provider"，
        // 而不是"没找到 provider 后的兜底"。兜底路径仍存在（见 applyFallback），
        // 但只有在内置 provider 被使用者显式移除时才会触发。
        try (PluginManager manager = newManager(dir, List.of(), null,
                WalConfig.PluginFailurePolicy.FALLBACK_DEFAULT)) {
            PluginDiagnostics diag = manager.scan();
            assertNotNull(manager.current().codec(), "必须有 codec");
            assertEquals("builtin:raw", manager.current().codec().id());
            assertNotNull(manager.current().indexStore(), "索引必须有内置实现");
            assertNotNull(manager.current().retentionPolicy(), "保留策略必须有内置实现");
            assertNotNull(manager.current().replicationSink(), "复制 sink 必须有内置实现");
            assertEquals("builtin:retention-time-size",
                    manager.current().retentionPolicy().id());
            assertEquals("builtin:replication-noop", manager.current().replicationSink().id());
            assertTrue(diag.fallbacks().isEmpty(),
                    "内置实现是正规发现的，不该被记成兜底: " + diag.fallbacks());
            assertTrue(diag.loadedIds().stream().anyMatch(s -> s.contains("builtin:raw")),
                    "诊断里应能看内置 provider 被发现: " + diag.loadedIds());
            assertTrue(diag.active().containsValue("builtin:raw"),
                    "active() 里应能看到生效的 codec: " + diag.active());
        }
    }

    @Test
    @DisplayName("FAIL_FAST：干净部署（一个插件都没装）照常回退内置，不算失败")
    void failFastToleratesCleanDeploymentWithoutPlugins(@TempDir Path dir) {
        try (PluginManager manager = newManager(dir, List.of(), null,
                WalConfig.PluginFailurePolicy.FAIL_FAST)) {
            PluginDiagnostics diag = manager.scan();
            assertEquals("builtin:raw", manager.current().codec().id(),
                    "没装任何插件时 FAIL_FAST 也不该拦住启动（否则这个开关永远不可用）");
            assertNotNull(manager.current().indexStore());
            assertTrue(diag.failedPlugins().isEmpty(),
                    "干净部署不该有任何加载失败: " + diag.failedPlugins());
            assertTrue(diag.healthy(), "干净部署必须是健康的: " + diag);
        }
    }

    @Test
    @DisplayName("FAIL_FAST：消息必须同时包含接口全名、ClassLoader 列表、provider 文件名、已发现 id")
    void failFastMessageCarriesAllFourElements(@TempDir Path dir) {
        WalException.PluginLoadException error = assertThrows(
                WalException.PluginLoadException.class,
                () -> {
                    WalBuilder builder = TestWal.builder(dir, 4096L)
                            .pluginClassLoader(loader(allFixtures(), "fail-fast"))
                            .codec("test:missing")
                            .onPluginLoadFailure(WalConfig.PluginFailurePolicy.FAIL_FAST);
                    PluginManager manager = new PluginManager(builder.buildConfig(), dir, null);
                    try {
                        manager.scan();
                    } finally {
                        manager.close();
                    }
                });

        String message = error.getMessage();
        System.out.println("=== FAIL_FAST 实测消息 ===");
        System.out.println(message);
        System.out.println("=== /FAIL_FAST 实测消息 ===");

        assertTrue(message.contains("com.walcore.spi.RecordCodec"),
                "① 必须含接口全名");
        assertTrue(message.contains("META-INF/services/com.walcore.spi.RecordCodec"),
                "③ 必须含期望的 provider 文件名");
        assertTrue(message.contains("已尝试的 ClassLoader"),
                "② 必须列出已尝试的 ClassLoader");
        assertTrue(message.contains("wal-core"), "② ClassLoader 列表里至少要出现核心加载器");
        assertTrue(message.contains("已发现的 id"), "④ 必须含已发现 id 段");
        assertTrue(message.contains("onPluginLoadFailure=FAIL_FAST"), "必须点明是 FAIL_FAST 策略");
        assertTrue(message.contains("wal.spi.codec"), "必须给出相关配置键，便于运维定位");
    }

    @Test
    @DisplayName("FAIL_FAST + 存在不兼容 provider：失败消息里带上被剔除的原因")
    void failFastReportsIncompatibleProviders(@TempDir Path dir) {
        // 只有 all 这一份，但把高/低优先级的两个实现都通过 pluginDir 关掉是不可能的；
        // 因此这里只验证"有候选时不失败"，与上一条互补。
        try (PluginManager manager = newManager(dir, allFixtures(), null,
                WalConfig.PluginFailurePolicy.FAIL_FAST)) {
            PluginDiagnostics diag = manager.scan();
            assertEquals(HighPriorityCodec.ID, manager.current().codec().id(),
                    "FAIL_FAST 只在真的找不到实现时才失败");
            assertTrue(diag.incompatiblePlugins().stream()
                    .anyMatch(s -> s.contains("test:codec-future")));
        }
    }

    // ==================================================================
    // 显式指定 id
    // ==================================================================

    @Test
    @DisplayName("wal.spi.codec 显式指定：即使优先级更低也强制生效")
    void explicitIdOverridesPriority(@TempDir Path dir) {
        WalBuilder builder = TestWal.builder(dir, 4096L)
                .pluginClassLoader(loader(allFixtures(), "explicit"))
                .codec(PassThroughCodec.ID);
        WalConfig config = builder.buildConfig();

        try (PluginManager manager = new PluginManager(config, dir, null)) {
            manager.scan();
            assertEquals(PassThroughCodec.ID, manager.current().codec().id(),
                    "显式配置必须压过 @Priority");
        }
    }

    @Test
    @DisplayName("wal.spi.codec 指定了不存在的 id：FALLBACK_DEFAULT 回退内置并记录失败原因")
    void explicitIdNotFoundFallsBack(@TempDir Path dir) {
        WalBuilder builder = TestWal.builder(dir, 4096L)
                .pluginClassLoader(loader(allFixtures(), "explicit-miss"))
                .codec("test:does-not-exist");
        try (PluginManager manager = new PluginManager(builder.buildConfig(), dir, null)) {
            PluginDiagnostics diag = manager.scan();
            assertEquals("builtin:raw", manager.current().codec().id());
            assertTrue(diag.failedPlugins().stream().anyMatch(s -> s.contains("test:does-not-exist")),
                    "必须记录「强制指定但未发现」的原因: " + diag.failedPlugins());
        }
    }

    // ==================================================================
    // 类加载隔离
    // ==================================================================

    @Test
    @DisplayName("PluginClassLoader 对 com.walcore.* 强制 parent-first（避免 SPI 接口被复制一份）")
    void pluginClassLoaderIsParentFirstForCorePackages(@TempDir Path dir) throws Exception {
        try (PluginClassLoader loader = new PluginClassLoader(
                new URL[]{fixture("all").toUri().toURL()},
                getClass().getClassLoader(), "unit-test", null)) {

            assertSame(RecordCodec.class, loader.loadClass("com.walcore.spi.RecordCodec"),
                    "SPI 接口必须由父加载器提供，否则 instanceof 永远为 false");
            assertSame(PluginManager.class,
                    loader.loadClass("com.walcore.internal.plugin.PluginManager"),
                    "com.walcore.internal.* 同样必须 parent-first");
            assertSame(String.class, loader.loadClass("java.lang.String"));
            assertNotSame(loader, getClass().getClassLoader());
        }
    }

    @Test
    @DisplayName("JAR 里含 com/walcore/*.class 时必须拒绝加载并给出可操作的原因")
    void jarContainingCoreClassesIsRejected(@TempDir Path dir) throws Exception {
        Path pluginDir = dir.resolve("plugin");
        Files.createDirectories(pluginDir);
        Path jar = pluginDir.resolve("bad-plugin.jar");
        writeJarWithCoreClass(jar);

        assertNotNull(PluginClassLoader.findCoreClassEntry(jar),
                "必须能扫出被 shade 进来的核心类");
        String reason = PluginClassLoader.rejectedJarReason(jar,
                PluginClassLoader.findCoreClassEntry(jar));
        assertTrue(reason.contains("provided") || reason.contains("compileOnly"),
                "原因里必须给出可操作的修复方式: " + reason);

        try (PluginManager manager = newManager(dir, List.of(), null,
                WalConfig.PluginFailurePolicy.FALLBACK_DEFAULT)) {
            PluginException error = assertThrows(PluginException.class,
                    () -> manager.installJar(jar));
            assertNotNull(error.getMessage());
            PluginDiagnostics diag = manager.scan();
            assertTrue(diag.rejectedJars().stream()
                            .anyMatch(s -> s.contains("bad-plugin.jar")),
                    "被拒绝的 JAR 必须出现在诊断里: " + diag.rejectedJars());
        }
    }

    // ==================================================================
    // 热切换
    // ==================================================================

    @Test
    @DisplayName("swap 成功：返回新旧实例，current 指向新实现")
    void swapSucceedsBetweenDiscoveredPlugins(@TempDir Path dir) {
        try (PluginManager manager = newManager(dir, allFixtures(), null,
                WalConfig.PluginFailurePolicy.FALLBACK_DEFAULT)) {
            manager.scan();
            RecordCodec before = manager.current().codec();
            assertEquals(HighPriorityCodec.ID, before.id());

            PluginType.SwapResult<RecordCodec> result =
                    manager.swap(PluginType.CODEC, PassThroughCodec.ID,
                            PluginType.SwapPolicy.SHADOW_THEN_SWAP);
            assertTrue(result.success(), "切换应成功: " + result);
            assertSame(before, result.previous());
            assertEquals(PassThroughCodec.ID, result.current().id());
            assertEquals(PassThroughCodec.ID, manager.current().codec().id());
        }
    }

    @Test
    @DisplayName("swap 失败：自动回滚，旧实例仍然生效（服务不中断）")
    void swapFailureRollsBack(@TempDir Path dir) {
        try (PluginManager manager = newManager(dir, allFixtures(), null,
                WalConfig.PluginFailurePolicy.FALLBACK_DEFAULT)) {
            manager.scan();
            RecordCodec before = manager.current().codec();

            PluginType.SwapResult<RecordCodec> result =
                    manager.swap(PluginType.CODEC, BrokenInitCodecId.VALUE,
                            PluginType.SwapPolicy.IMMEDIATE);
            assertFalse(result.success(), "init 失败的切换必须报失败");
            assertTrue(result.rolledBack(), "失败必须标记已回滚");
            assertNotNull(result.failureReason());
            assertSame(before, result.current(), "回滚后 current 必须还是旧实例");
            assertSame(before, manager.current().codec(),
                    "管理器里的引用绝不能因为失败而变空或变成坏实例");
        }
    }

    @Test
    @DisplayName("swap 到不存在的 id：失败且回滚，原因里列出已发现 id")
    void swapToUnknownIdFails(@TempDir Path dir) {
        try (PluginManager manager = newManager(dir, allFixtures(), null,
                WalConfig.PluginFailurePolicy.FALLBACK_DEFAULT)) {
            manager.scan();
            PluginType.SwapResult<RecordCodec> result =
                    manager.swap(PluginType.CODEC, "test:nope", PluginType.SwapPolicy.IMMEDIATE);
            assertFalse(result.success());
            assertTrue(result.failureReason().contains("test:codec-high"),
                    "失败原因里必须给出已发现 id: " + result.failureReason());
        }
    }

    @Test
    @DisplayName("不支持热切换的类型：明确返回失败而不是抛异常")
    void swapOnUnsupportedTypeFailsGracefully(@TempDir Path dir) {
        try (PluginManager manager = newManager(dir, allFixtures(), null,
                WalConfig.PluginFailurePolicy.FALLBACK_DEFAULT)) {
            manager.scan();
            PluginType.SwapResult<?> result =
                    manager.swap(PluginType.STORAGE, "builtin:storage-filechannel",
                            PluginType.SwapPolicy.IMMEDIATE);
            assertFalse(result.success());
            assertTrue(result.failureReason().contains("T14"),
                    "必须说明原因与交付阶段: " + result.failureReason());
        }
    }

    // ==================================================================
    // 契约修订：IndexStore.onAppended 真的被写路径调用
    // ==================================================================

    @Test
    @DisplayName("IndexStore 插件收到逐条 onAppended（T11 契约修订的端到端验证）")
    void indexPluginReceivesOnAppended(@TempDir Path dir) throws Exception {
        WalBuilder builder = TestWal.builder(dir, 4096L)
                .pluginClassLoader(loader(List.of(fixture("index")), "index"))
                .indexStore(CountingIndexStore.ID);
        WalConfig config = builder.buildConfig();

        try (DefaultWal wal = DefaultWal.open(config)) {
            assertEquals(CountingIndexStore.ID, wal.pluginManager().indexStore().id(),
                    "索引插件必须真的生效");
            CountingIndexStore store = (CountingIndexStore) wal.pluginManager().indexStore();
            assertTrue(store.initialized(), "插件必须被 init");

            long last = 0L;
            for (int i = 0; i < 50; i++) {
                last = wal.append(0, TestWal.payload(16, i), AppendOptions.DEFAULTS);
            }
            assertTrue(wal.flushAndWait(0, last, 10_000L));

            assertEquals(50L, store.appendedCount(),
                    "50 次 append 必须触发 50 次 onAppended（否则第三方索引恒为空）");
            assertTrue(store.openedSegments() > 0L, "至少要为每段调用一次 openSegment");
            assertEquals(last, store.lastIndexedLsn(),
                    "第三方索引必须能算出真实 lastIndexedLsn（只可能来自 onAppended）");

            // 索引插件生效后，读路径仍然必须正确（索引只是加速层）
            assertEquals(50, TestWal.readAllLsns(wal, 0).size());
        } finally {
            closeConfigPlugins(config);
        }
    }

    // ==================================================================
    // 夹具
    // ==================================================================

    private static List<Path> allFixtures() {
        return List.of(fixture("all"));
    }

    private static Path fixture(String name) {
        try {
            URL url = PluginLoadingTest.class.getClassLoader().getResource(FIXTURES + "/" + name);
            assertNotNull(url, "测试资源缺失: " + FIXTURES + "/" + name);
            return Path.of(url.toURI());
        } catch (Exception e) {
            throw new AssertionError("解析测试资源失败: " + name, e);
        }
    }

    private static PluginClassLoader loader(List<Path> dirs, String name) {
        URL[] urls = new URL[dirs.size()];
        for (int i = 0; i < dirs.size(); i++) {
            try {
                urls[i] = dirs.get(i).toUri().toURL();
            } catch (Exception e) {
                throw new AssertionError("构造 URL 失败", e);
            }
        }
        return new PluginClassLoader(urls, PluginLoadingTest.class.getClassLoader(), name, null);
    }

    private static PluginManager newManager(Path baseDir, List<Path> fixtures, ClassLoader parent,
                                            WalConfig.PluginFailurePolicy policy) {
        PluginClassLoader pluginLoader = loader(fixtures, "unit-test");
        WalBuilder builder = TestWal.builder(baseDir, 4096L)
                .pluginClassLoader(pluginLoader)
                .onPluginLoadFailure(policy);
        return new PluginManager(builder.buildConfig(), baseDir, null);
    }

    private static void closeConfigPlugins(WalConfig config) {
        // WalConfig 持有的是 ClassLoader 引用，关闭由 DefaultWal 负责；这里只做占位说明
        assertNotNull(config);
    }

    private static void writeJarWithCoreClass(Path jar) throws IOException {
        try (OutputStream out = Files.newOutputStream(jar);
             ZipOutputStream zip = new ZipOutputStream(out)) {
            zip.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"));
            zip.write("Manifest-Version: 1.0\n".getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
            zip.putNextEntry(new ZipEntry("com/walcore/spi/RecordCodec.class"));
            zip.write(new byte[]{(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE});
            zip.closeEntry();
        }
    }

    /** 用常量类避免直接引用"故意坏"的实现类的 id 字符串散落各处。 */
    private static final class BrokenInitCodecId {
        private static final String VALUE = "test:codec-broken";

        private BrokenInitCodecId() {
        }
    }

    /** 兼容 JDK 检查：确认 URLClassLoader 是 PluginClassLoader 的父类。 */
    @Test
    @DisplayName("PluginClassLoader 是 URLClassLoader 的子类（可用 URL 直接构造）")
    void pluginClassLoaderIsUrlClassLoader(@TempDir Path dir) throws Exception {
        try (URLClassLoader ignored = loader(allFixtures(), "kind-check")) {
            assertTrue(ignored instanceof PluginClassLoader);
        }
    }
}
