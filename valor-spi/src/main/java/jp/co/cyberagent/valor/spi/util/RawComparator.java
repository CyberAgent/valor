/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.cyberagent.valor.spi.util;

import java.util.Comparator;

/**
 * Originated in org.apache.hadoop.io.RawComparator
 * Copied and modified to simplify dependencies.
 *
 * <p>
 * A {@link Comparator} that operates directly on byte representations of
 * objects.
 * </p>
 */
public interface RawComparator<T> extends Comparator<T> {

  /**
   * Compare two objects in binary.
   * b1[s1:l1] is the first object, and b2[s2:l2] is the second object.
   *
   * @param b1 The first byte array.
   * @param s1 The position index in b1. The object under comparison's starting index.
   * @param l1 The length of the object in b1.
   * @param b2 The second byte array.
   * @param s2 The position index in b2. The object under comparison's starting index.
   * @param l2 The length of the object under comparison in b2.
   * @return An integer result of the comparison.
   */
  int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2);
}
