/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.Collator;
import java.util.Arrays;
import java.util.Set;

public class CalculateAverage_aramponi {

    private static final Unsafe UNSAFE;

    static {
        try {
            Field unsafe = Unsafe.class.getDeclaredField("theUnsafe");
            unsafe.setAccessible(true);
            UNSAFE = (Unsafe) unsafe.get(Unsafe.class);
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    static class Info {
        final int hash;
        final byte[] city;
        final String cityStr;
        int minTemperature;
        int maxTemperature;
        // int meanTemperature;
        int sumTemperature;
        int count;
        Info next;

        Info(int hash, byte[] city, int length, int temperature) {
            this.hash = hash;
            this.city = new byte[length];
            System.arraycopy(city, 0, this.city, 0, length);
            this.cityStr = new String(this.city);
            this.maxTemperature = temperature;
            this.minTemperature = temperature;
            // this.meanTemperature = temperature;
            this.sumTemperature = temperature;
            this.count = 1;
        }

        @Override
        public String toString() {
            return STR."\{new String(city)}=\{minTemperature / 10.0}/\{sumTemperature / count / 10.0}/\{maxTemperature / 10.0}";
        }

    }

    static class SimpleMap {
        static final int MAP_CAPACITY = 16001;

        Info[] info = new Info[MAP_CAPACITY]; // optimized for no collision
        int size = 0;

        public void put(int hash, byte[] buffer, int length, int temperature) {
            int idx = Math.abs(hash % MAP_CAPACITY);
            // element at this
            putIdx(hash, idx, buffer, length, temperature);
        }

        private void putIdx(int hash, int idx, byte[] buffer, int length, int temperature) {
            Info current = info[idx];
            Info previous = null;
            // ensure byte array are equals
            outerloop: while (current != null) {
                // first compare length
                if (length == current.city.length) {
                    // if (Arrays.equals(buffer, current.city)) {
                    // break;
                    // }
                    // if (buffer[0] == current.city[0]) {
                    // for (int i = 0; i < length; i += SPECIES.length()) {
                    // VectorMask<Byte> mask = SPECIES.indexInRange(i, length);
                    // var v2 = ByteVector.fromArray(SPECIES, buffer, i, mask);
                    // var v1 = ByteVector.fromArray(SPECIES, current.city, i, mask);
                    // var v3 = v1.lanewise(XOR, v2, mask).reduceLanes(FIRST_NONZERO);
                    // if (v3 != 0) {
                    // System.out.println("cqll " + current.cityStr + " " + new String(buffer) + " " + v3 );
                    // previous = current;
                    // current = current.next;
                    // break outerloop;
                    // }
                    // }
                    // }
                    // break; // while loop
                    if (current.hash != hash)
                        for (int i = 0; i < length; i++)
                            if (current.city[i] != buffer[i]) {
                                previous = current;
                                current = current.next;
                                break outerloop;
                            }
                    break; // while loop
                }
                previous = current;
                current = current.next;
            }
            // 2 cases :
            if (current == null) {
                current = new Info(hash, buffer, length, temperature);
                size++;
            }
            else {
                if (temperature < current.minTemperature)
                    current.minTemperature = temperature;
                else if (temperature > current.maxTemperature)
                    current.maxTemperature = temperature;
                current.sumTemperature += temperature; // = (current.meanTemperature * current.count + temperature) / (current.count + 1);
                current.count++;
            }
            if (info[idx] == null)
                info[idx] = current;
            else if (previous != null && previous.next == null)
                previous.next = current;
        }

        public void merge(SimpleMap map) {
            System.out.println(Thread.currentThread().getName() + " merging");
            for (int i = 0; i < MAP_CAPACITY; i++) {
                for (Info from = map.info[i]; from != null; from = from.next) {
                    boolean found = false;
                    for (Info to = info[i]; to != null; to = to.next) {
                        if (from.city.length == to.city.length) {
                            boolean equals = true;
                            for (int j = 0; j < from.city.length; j++) {
                                if (from.city[j] != to.city[j]) {
                                    equals = false;
                                    break;
                                }
                            }
                            if (equals) {
                                if (from.minTemperature < to.minTemperature)
                                    to.minTemperature = from.minTemperature;
                                if (from.maxTemperature > to.maxTemperature)
                                    to.maxTemperature = from.maxTemperature;
                                // to.meanTemperature = (to.meanTemperature * to.count + from.meanTemperature * from.count) / (to.count + from.count);
                                to.sumTemperature += from.sumTemperature;
                                to.count += from.count;
                                found = true;
                                break;
                            }
                        }
                    }
                    if (!found) {
                        putIdx(from.hash, i, from.city, from.city.length, from.sumTemperature);
                    }
                }
            }
        }

        private Info[] toArray() {
            Info[] array = new Info[size];
            int idx = 0;
            for (int i = 0; i < MAP_CAPACITY; i++) {
                Info current = info[i];
                while (current != null) {
                    array[idx++] = current;
                    current = current.next;
                }
            }
            return array;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            Info[] array = toArray();
            Collator instance = Collator.getInstance();
            Arrays.sort(array, (v1, v2) -> instance.compare(v1.cityStr, v2.cityStr));
            for (Info value : array) {
                sb.append(value).append("\n");
            }

            return sb.toString();
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        long start = System.currentTimeMillis();
        int THREADS = 16;
        SimpleMap[] maps = new SimpleMap[THREADS];

        try (final FileChannel fc = FileChannel.open(Path.of("measurements.txt"), Set.of(StandardOpenOption.READ))) {
            MemorySegment mem = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size(), Arena.global());
            long[] positions = new long[THREADS + 1];
            long[] addresses = new long[THREADS + 1];
            positions[0] = 0;
            positions[THREADS] = fc.size();
            addresses[0] = mem.address();
            addresses[THREADS] = addresses[0] + fc.size();
            for (int i = 1; i < THREADS; i++) {
                long position = fc.size() / THREADS * i;
                long address = addresses[0] + position;
                int j = 0;
                while (UNSAFE.getByte(address + j) != '\n')
                    j++;
                positions[i] = position + j + 1;
                addresses[i] = address + j + 1;
            }
            Thread[] threads = new Thread[THREADS];
            for (int i = 0; i < THREADS; i++) {
                final int finalI = i;
                threads[i] = Thread.ofVirtual().start(() -> {
                    try {
                        System.out.println("Started thread " + finalI);
                        SimpleMap map = maps[finalI] = new SimpleMap();
                        byte[] location = new byte[100];
                        byte currentByte;
                        // int counter = 0;
                        long sliceLength = positions[finalI + 1] - positions[finalI];
                        for (long pos = 0; pos < sliceLength;) {
                            boolean negative = false;
                            int temperature = 0;
                            int j = 0;
                            long city_start = pos;
                            int hash = 0;
                            for (; (currentByte = UNSAFE.getByte(addresses[finalI] + pos)) != ';'; pos++) {
                                hash += 17 * hash + currentByte;
                                location[j++] = currentByte;
                            }
                            long city_stop = pos;
                            pos++; // skip trailing ;
                            while (pos < sliceLength && (currentByte = UNSAFE.getByte(addresses[finalI] + pos)) != '\n') {
                                pos++;
                                switch (currentByte) {
                                    case '-':
                                        negative = true;
                                        break;
                                    case '.':
                                        temperature = (temperature * 10) + (UNSAFE.getByte(addresses[finalI] + pos++) - '0'); // single decimal
                                        break;
                                    default:
                                        temperature = (temperature * 10) + (currentByte - '0');
                                }
                            }
                            pos++; // skip trailing \n
                            map.put(hash, location, (int) (city_stop - city_start), negative ? -temperature : temperature);

                        }
                    }
                    catch (RuntimeException e) {

                        throw new RuntimeException(e);
                    }

                });
            }
            for (int i = 0; i < THREADS; i++) {
                threads[i].join();
            }
            long beforeMerge = System.currentTimeMillis();
            for (int i = 1; i < THREADS; i++) {
                maps[0].merge(maps[i]);
            }

            long beforeprint = System.currentTimeMillis();

            System.out.println(maps[0].toString());
            System.out.printf("Time taken before Merge: %d ms%n", System.currentTimeMillis() - beforeMerge);
            System.out.printf("Time taken before print: %d ms%n", System.currentTimeMillis() - beforeprint);
            System.out.printf("Time taken: %d ms%n", System.currentTimeMillis() - start);

        }
        catch (Exception e) {
            System.out.println(e);
        }
    }
}