package com.walcore.internal.util;

/**
 * 参数与状态校验工具（零依赖版本，替代 Guava 的 {@code Preconditions}）。
 *
 * <p>本类<b>严禁</b>出现在 {@code append()} 热路径上：所有方法在失败时都会构造异常对象与
 * 格式化消息，属于分配操作。热路径的边界检查请用裸 {@code if + throw} 或
 * {@code FrameLayout.isXxx()} 这类无分配判断。
 */
public final class Preconditions {

    private Preconditions() {
    }

    /**
     * 校验对象引用非 null。
     *
     * @param ref 被校验引用
     * @param name 参数名（出现在异常消息中）
     * @param <T> 引用类型
     * @return 原引用，便于链式使用
     * @throws NullPointerException 当 {@code ref == null}
     */
    public static <T> T checkNotNull(T ref, String name) {
        if (ref == null) {
            throw new NullPointerException(name + " 不得为 null");
        }
        return ref;
    }

    /**
     * 校验布尔条件成立。
     *
     * @param condition 条件
     * @param message 失败消息（惰性拼接，仅在失败时执行）
     * @throws IllegalArgumentException 条件不成立
     */
    public static void checkArgument(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * 校验布尔条件成立（带格式化参数）。
     *
     * @param condition 条件
     * @param template 消息模板，使用 {@code %s} 占位
     * @param args 模板参数
     * @throws IllegalArgumentException 条件不成立
     */
    public static void checkArgument(boolean condition, String template, Object... args) {
        if (!condition) {
            throw new IllegalArgumentException(format(template, args));
        }
    }

    /**
     * 校验对象状态合法。
     *
     * @param condition 条件
     * @param message 失败消息
     * @throws IllegalStateException 条件不成立
     */
    public static void checkState(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    /**
     * 校验对象状态合法（带格式化参数）。
     *
     * @param condition 条件
     * @param template 消息模板
     * @param args 模板参数
     * @throws IllegalStateException 条件不成立
     */
    public static void checkState(boolean condition, String template, Object... args) {
        if (!condition) {
            throw new IllegalStateException(format(template, args));
        }
    }

    /**
     * 校验整数为严格正数。
     *
     * @param value 数值
     * @param name 参数名
     * @return 原值
     * @throws IllegalArgumentException 当 {@code value <= 0}
     */
    public static int checkPositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " 必须为正数，实际 = " + value);
        }
        return value;
    }

    /**
     * 校验长整数为严格正数。
     *
     * @param value 数值
     * @param name 参数名
     * @return 原值
     * @throws IllegalArgumentException 当 {@code value <= 0}
     */
    public static long checkPositive(long value, String name) {
        if (value <= 0L) {
            throw new IllegalArgumentException(name + " 必须为正数，实际 = " + value);
        }
        return value;
    }

    /**
     * 校验整数落在闭区间 {@code [min, max]}。
     *
     * @param value 数值
     * @param min 下界（含）
     * @param max 上界（含）
     * @param name 参数名
     * @return 原值
     * @throws IllegalArgumentException 越界
     */
    public static int checkInRange(int value, int min, int max, String name) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    name + " 必须落在 [" + min + ", " + max + "]，实际 = " + value);
        }
        return value;
    }

    /**
     * 校验长整数落在闭区间 {@code [min, max]}。
     *
     * @param value 数值
     * @param min 下界（含）
     * @param max 上界（含）
     * @param name 参数名
     * @return 原值
     * @throws IllegalArgumentException 越界
     */
    public static long checkInRange(long value, long min, long max, String name) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    name + " 必须落在 [" + min + ", " + max + "]，实际 = " + value);
        }
        return value;
    }

    /**
     * 极简 {@code %} 模板替换，替代 {@link String#format}（后者依赖 Formatter，开销更大）。
     *
     * @param template 模板
     * @param args 参数
     * @return 替换后的字符串
     */
    private static String format(String template, Object... args) {
        if (args == null || args.length == 0) {
            return template;
        }
        StringBuilder sb = new StringBuilder(template.length() + (args.length << 3));
        int argIndex = 0;
        int cursor = 0;
        while (cursor < template.length()) {
            int hit = template.indexOf("%s", cursor);
            if (hit < 0 || argIndex >= args.length) {
                sb.append(template, cursor, template.length());
                break;
            }
            sb.append(template, cursor, hit).append(args[argIndex++]);
            cursor = hit + 2;
        }
        return sb.toString();
    }
}
