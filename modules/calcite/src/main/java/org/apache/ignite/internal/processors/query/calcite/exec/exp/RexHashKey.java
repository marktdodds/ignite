package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;

public class RexHashKey {

    public static void main(String[] args) {
        RexHashKey k1 = new RexHashKey(new ImmutableList.Builder<>().add(1).add(23).build());
        RexHashKey k2 = new RexHashKey(new ImmutableList.Builder<>().add(1).add(23).build());

        System.out.println(k1.hashCode());
        System.out.println(k2.hashCode());
        System.out.println(k1.equals(k2));

    }

    private final ImmutableList<Object> key;

    public RexHashKey(ImmutableList<Object> key) {
        this.key = key;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key.toArray());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RexHashKey)) return false;
        return Arrays.equals(key.toArray(), ((RexHashKey) obj).key.toArray());
    }
}
