package com.walcore.internal.partition;

import com.walcore.api.WalException;
import com.walcore.internal.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 分区注册表：<b>{@code volatile PartitionImpl[] slots} + 写时复制（COW）</b>。
 *
 * <h2>读路径为什么能做到"零锁零 volatile 读放大"</h2>
 * {@link #get(int)} 只做<b>一次</b> volatile 读（读 {@code slots} 引用本身），
 * 之后就是普通数组下标访问：
 * <pre>{@code
 * PartitionImpl[] s = this.slots;              // 唯一一次 volatile load
 * return (pid >= 0 && pid < s.length) ? s[pid] : null;
 * }</pre>
 * 如果把数组声明成 {@code PartitionImpl[]} 再对元素做 {@code volatile} 语义，每次 append
 * 都会多一次屏障；而 COW 把"可见性"集中在数组引用这一处，代价被压到最低。
 *
 * <h2>写路径</h2>
 * {@link #register}/{@link #unregister} 在 {@code writeMutex} 内复制数组、修改、整体替换引用。
 * 并发读者要么看到旧数组（完整一致），要么看到新数组（完整一致），<b>绝不会</b>看到
 * {@code ArrayIndexOutOfBoundsException}。分区增删频率极低（人工运维级），复制成本可忽略。
 */
public final class PartitionRegistry {

    private final int maxPartitions;
    private final Object writeMutex = new Object();

    /** 唯一需要 volatile 的引用。 */
    private volatile PartitionImpl[] slots;

    /** 存活分区 ID 的快照（同样 COW）。 */
    private volatile int[] activeIds = new int[0];

    private volatile int activeCount;

    /**
     * 构造注册表。
     *
     * @param maxPartitions 分区 ID 上限（槽位数组长度）
     */
    public PartitionRegistry(int maxPartitions) {
        Preconditions.checkInRange(maxPartitions, 1, 65_536, "maxPartitions");
        this.maxPartitions = maxPartitions;
        this.slots = new PartitionImpl[maxPartitions];
    }

    /**
     * 读取分区（<b>热路径</b>：一次 volatile load + 一次数组访问）。
     *
     * @param partitionId 分区 ID
     * @return 分区；不存在或越界返回 null
     */
    public PartitionImpl get(int partitionId) {
        PartitionImpl[] s = this.slots;
        if (partitionId < 0 || partitionId >= s.length) {
            return null;
        }
        return s[partitionId];
    }

    /**
     * 读取分区，不存在则抛 {@link WalException.PartitionException}。
     *
     * @param partitionId 分区 ID
     * @return 分区
     * @throws WalException.PartitionException 分区不存在
     */
    public PartitionImpl require(int partitionId) {
        PartitionImpl p = get(partitionId);
        if (p == null) {
            throw new WalException.PartitionException(
                    "分区不存在: " + partitionId + "（有效范围 0.." + (maxPartitions - 1) + "）");
        }
        return p;
    }

    /**
     * 注册分区（COW）。
     *
     * @param partition 分区
     * @throws WalException.PartitionException 槽位已被占用
     */
    public void register(PartitionImpl partition) {
        Preconditions.checkNotNull(partition, "partition");
        int pid = partition.id();
        if (pid < 0 || pid >= maxPartitions) {
            throw new WalException.PartitionException(
                    "分区 ID 越界: " + pid + "（上限 " + maxPartitions + "）");
        }
        synchronized (writeMutex) {
            PartitionImpl[] current = this.slots;
            if (current[pid] != null) {
                throw new WalException.PartitionException("分区 " + pid + " 已存在");
            }
            PartitionImpl[] next = current.clone();
            next[pid] = partition;
            this.slots = next;
            rebuildActiveIds(next);
        }
    }

    /**
     * 注销分区（COW）。
     *
     * @param partitionId 分区 ID
     * @return 被移除的分区；不存在返回 null
     */
    public PartitionImpl unregister(int partitionId) {
        synchronized (writeMutex) {
            PartitionImpl[] current = this.slots;
            if (partitionId < 0 || partitionId >= current.length || current[partitionId] == null) {
                return null;
            }
            PartitionImpl removed = current[partitionId];
            PartitionImpl[] next = current.clone();
            next[partitionId] = null;
            this.slots = next;
            rebuildActiveIds(next);
            return removed;
        }
    }

    /**
     * 重建 ID 快照（必须在 {@code writeMutex} 内调用）。
     *
     * @param next 新槽位数组
     */
    private void rebuildActiveIds(PartitionImpl[] next) {
        int n = 0;
        for (PartitionImpl p : next) {
            if (p != null) {
                n++;
            }
        }
        int[] ids = new int[n];
        int i = 0;
        for (int pid = 0; pid < next.length; pid++) {
            if (next[pid] != null) {
                ids[i++] = pid;
            }
        }
        Arrays.sort(ids);
        this.activeIds = ids;
        this.activeCount = n;
    }

    /**
     * 存活分区 ID 快照（升序，不可变语义的副本）。
     *
     * @return ID 数组（返回内部快照引用，调用方不得修改）
     */
    public int[] activeIds() {
        return activeIds;
    }

    /**
     * 存活分区 ID 的可修改副本。
     *
     * @return 新数组
     */
    public int[] activeIdsCopy() {
        return activeIds.clone();
    }

    /**
     * 存活分区数。
     *
     * @return 数量
     */
    public int activeCount() {
        return activeCount;
    }

    /**
     * 槽位数组长度（分区 ID 上限）。
     *
     * @return 长度
     */
    public int maxPartitions() {
        return maxPartitions;
    }

    /**
     * 存活分区的快照列表（顺序与 {@link #activeIds()} 一致）。
     *
     * @return 列表
     */
    public List<PartitionImpl> snapshot() {
        int[] ids = activeIds;
        PartitionImpl[] s = slots;
        List<PartitionImpl> out = new ArrayList<>(ids.length);
        for (int id : ids) {
            PartitionImpl p = s[id];
            if (p != null) {
                out.add(p);
            }
        }
        return out;
    }

    /**
     * 是否已注册某个分区。
     *
     * @param partitionId 分区 ID
     * @return 已注册返回 true
     */
    public boolean isRegistered(int partitionId) {
        return get(partitionId) != null;
    }

    @Override
    public String toString() {
        return "PartitionRegistry{active=" + activeCount + ", maxPartitions=" + maxPartitions + '}';
    }
}
