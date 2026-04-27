package com.github.jsbxyyx.transaction.rmq.util;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author jsbxyyx
 * @since 1.0.0
 */
public class MqId {

    public static String nextId() {
        return nextId(System.currentTimeMillis());
    }

    public static String nextId(long timestampMs) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        // msb: 48-bit unix_ts_ms | 4-bit version(7) | 12-bit rand_a
        long msb = (timestampMs << 16) | 0x7000L | (rnd.nextLong() & 0x0FFFL);
        // lsb: 2-bit variant(10) | 62-bit rand_b
        long lsb = (rnd.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new UUID(msb, lsb).toString();
    }

}
