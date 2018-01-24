/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This class provides utilities for working with CRCs.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class CrcUtil {
  public static final int GZIP_POLYNOMIAL = 0xEDB88320;
  public static final int CASTAGNOLI_POLYNOMIAL = 0x82F63B78;

  /**
   * Compute x^({@code lengthBytes} * 8) mod {@code mod}, where {@code mod} is
   * in "reversed" (little-endian) format such that {@code mod & 1} represents
   * x^31 and has an implicit term x^32.
   */
  public static int getMonomial(long lengthBytes, int mod) {
    // TODO(dhuo): Exception on negative degree.
    if (lengthBytes == 0) {
      return 0;
    }

    // Decompose into
    // x^degree == x ^ SUM(bit[i] * 2^i) == PRODUCT(x ^ (bit[i] * 2^i))
    // Generate each x^(2^i) by squaring.
    int multiplier = 0x40000000;  // x^1
    int product = 0x80000000;
    long degree = lengthBytes * 8;
    while (degree > 0) {
      if ((degree & 1) != 0) {
        product = multiply(product, multiplier, mod);
      }
      multiplier = multiply(multiplier, multiplier, mod);
      degree >>= 1;
    }
    return product;
  }

  /**
   * @param monomial Precomputed x^(lengthBInBytes * 8) mod {@code mod}
   */
  public static int composeWithMonomial(int crcA, int crcB, int monomial, int mod) {
    return multiply(crcA, monomial, mod) ^ crcB;
  }

  /**
   * @param lengthB length of content corresponding to {@code crcB}, in bytes.
   */
  public static int compose(int crcA, int crcB, long lengthB, int mod) {
    int monomial = getMonomial(lengthB, mod);
    return composeWithMonomial(crcA, crcB, monomial, mod);
  }

  /**
   * @param m The little-endian polynomial to use as the modulus when multiplying p and q,
   *     with implicit "1" bit beyond the bottom bit.
   */
  private static int multiply(int p, int q, int m) {
    int summation = 0;

    // Top bit is the x^0 place; each right-shift increments the degree of the current term.
    int curTerm = 0x80000000;

    // Iteratively multiply p by x mod m as we go to represent the q[i] term (of degree x^i)
    // times p.
    int px = p;

    while (curTerm != 0) {
      if ((q & curTerm) != 0) {
        summation ^= px;
      }

      // Bottom bit represents highest degree since we're little-endian; before we multiply
      // by "x" for the next term, check bottom bit to know whether the resulting px will
      // thus have a term matching the implicit "1" term of "m" and thus will need to
      // subtract "m" after mutiplying by "x".
      boolean hasMaxDegree = ((px & 1) != 0);
      px >>>= 1;
      if (hasMaxDegree) {
        px ^= m;
      }
      curTerm >>>= 1;
    }
    return summation;
  }
}
