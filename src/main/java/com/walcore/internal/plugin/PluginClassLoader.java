package com.walcore.internal.plugin;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * 插件专用类加载器（ARCH §10.4.2 的精确规则）。
 *
 * <h2>加载顺序（顺序即语义，不可调整）</h2>
 * <ol>
 *   <li><b>JDK 类</b>（{@code java.*} / {@code javax.*} / {@code jdk.*} / {@code sun.*}
 *       / {@code com.sun.*}）→ <b>强制 parent</b>。否则插件里若带了一份
 *       {@code java.util.List}（shade 常见事故），会得到互相不可赋值的两个类型。</li>
 *   <li><b>{@code com.walcore.*}</b>（spi / api / internal 全部）→ <b>强制 parent</b>。
 *       这是最关键的一条：{@code RecordCodec} 若被插件复制一份，那就不是同一个
 *       接口类型，{@code instanceof} 永远 {@code false}，而 {@code ServiceLoader}
 *       也不会报错——故障表现为"插件装了但没生效"，极难排查。</li>
 *   <li><b>其余类</b> → <b>child first</b>：插件自带的依赖（如 lz4-java）优先用插件
 *       目录里的版本，避免与宿主应用的版本互相污染。找不到再委派 parent。</li>
 * </ol>
 *
 * <h2>核心类污染防护</h2>
 * {@link #findCoreClassEntry(Path)} 在加载 JAR 之前就扫描条目，一旦发现
 * {@code com/walcore/} 下的 {@code .class}，直接拒绝整个 JAR 并给出具体类名。
 * 这条防护针对的是"插件用 maven-shade 把 wal-core 打进来"这个最高频的踩坑方式。
 */
public final class PluginClassLoader extends URLClassLoader {

    /** 由本加载器强制 parent-first 的包前缀。 */
    private static final String[] PARENT_FIRST_PREFIXES = {
            "java.", "javax.", "jdk.", "sun.", "com.sun.", "com.walcore."
    };

    /** 插件 JAR 中禁止出现的类条目前缀。 */
    private static final String FORBIDDEN_ENTRY_PREFIX = "com/walcore/";

    static {
        registerAsParallelCapable();
    }

    private final String name;
    private final Path jarPath;

    /**
     * 构造插件类加载器。
     *
     * @param urls 插件 JAR（可多个）
     * @param parent 父加载器，通常传 {@code PluginClassLoader.class.getClassLoader()}
     * @param name 人类可读名称，用于诊断输出（通常是 JAR 文件名）
     * @param jarPath 主 JAR 路径，可为 null
     */
    public PluginClassLoader(URL[] urls, ClassLoader parent, String name, Path jarPath) {
        super(urls, parent);
        this.name = name == null ? "plugin" : name;
        this.jarPath = jarPath;
    }

    /**
     * 加载器名称。
     *
     * @return 名称
     */
    public String name() {
        return name;
    }

    /**
     * 对应 JAR 路径。
     *
     * @return 路径，可能为 null
     */
    public Path jarPath() {
        return jarPath;
    }

    @Override
    protected Class<?> loadClass(String className, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(className)) {
            for (String prefix : PARENT_FIRST_PREFIXES) {
                if (className.startsWith(prefix)) {
                    return super.loadClass(className, resolve);
                }
            }
            Class<?> loaded = findLoadedClass(className);
            if (loaded == null) {
                try {
                    loaded = findClass(className);
                } catch (ClassNotFoundException notInPlugin) {
                    loaded = super.loadClass(className, false);
                }
            }
            if (resolve) {
                resolveClass(loaded);
            }
            return loaded;
        }
    }

    @Override
    public URL getResource(String resourceName) {
        // META-INF/services 必须由本加载器优先提供，否则 ServiceLoader 看不到插件声明
        URL own = findResource(resourceName);
        return own != null ? own : super.getResource(resourceName);
    }

    @Override
    public Enumeration<URL> getResources(String resourceName) throws IOException {
        List<URL> all = new ArrayList<>();
        Enumeration<URL> own = findResources(resourceName);
        while (own.hasMoreElements()) {
            all.add(own.nextElement());
        }
        Enumeration<URL> parent = getParent() == null
                ? Collections.emptyEnumeration() : getParent().getResources(resourceName);
        while (parent.hasMoreElements()) {
            URL url = parent.nextElement();
            if (!all.contains(url)) {
                all.add(url);
            }
        }
        return Collections.enumeration(all);
    }

    @Override
    public String toString() {
        return "PluginClassLoader{" + name + (jarPath == null ? "" : ", " + jarPath) + '}';
    }

    // ------------------------------------------------------------------
    // 核心类污染检测
    // ------------------------------------------------------------------

    /**
     * 扫描一个 JAR，检查它是否打包了 {@code com.walcore.*} 下的类。
     *
     * @param jarPath JAR 路径
     * @return 首个违规条目名（如 {@code com/walcore/spi/RecordCodec.class}）；
     *         干净则返回 {@code null}；JAR 无法打开时抛 {@link IOException}
     * @throws IOException 无法读取 JAR
     */
    public static String findCoreClassEntry(Path jarPath) throws IOException {
        try (JarFile jar = new JarFile(jarPath.toFile())) {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String entryName = entry.getName();
                if (entryName.startsWith(FORBIDDEN_ENTRY_PREFIX) && entryName.endsWith(".class")) {
                    return entryName;
                }
            }
        }
        return null;
    }

    /**
     * 生成"插件不得包含核心类"的拒绝原因（{@link com.walcore.spi.PluginException} 的消息）。
     *
     * @param jarPath JAR 路径
     * @param offendingEntry 违规条目
     * @return 拒绝原因
     */
    public static String rejectedJarReason(Path jarPath, String offendingEntry) {
        return "插件不得包含核心类: " + offendingEntry.replace('/', '.')
                + "（JAR=" + jarPath.getFileName() + "）。"
                + "请把 wal-core 依赖改为 provided/compileOnly，"
                + "否则 SPI 接口会被复制一份，导致 instanceof 恒为 false。";
    }
}
