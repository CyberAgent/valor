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


package jp.co.cyberagent.valor.spi.relation.type;

import java.util.Arrays;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

/**
 * Originated in org.apache.hadoop.hbase.util.Bytes
 * Copied and modified to simplify dependencies.
 */
public class Bytes implements Comparable<Bytes> {


  private byte[] bytes;
  private int offset;
  private int length;

  /**
   * Create a zero-size sequence.
   */
  public Bytes() {
    super();
  }

  /**
   * Create a Bytes using the byte array as the initial value.
   *
   * @param bytes This array becomes the backing storage for the object.
   */
  public Bytes(byte[] bytes) {
    this(bytes, 0, bytes.length);
  }

  /**
   * Set the new Bytes to the contents of the passed
   * <code>ibw</code>.
   *
   * @param ibw the value to set this Bytes to.
   */
  public Bytes(final Bytes ibw) {
    this(ibw.get(), ibw.getOffset(), ibw.getLength());
  }

  /**
   * Set the value to a given byte range
   *
   * @param bytes  the new byte range to set to
   * @param offset the offset in newData to start at
   * @param length the number of bytes in the range
   */
  public Bytes(final byte[] bytes, final int offset,
               final int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Get the data from the Bytes.
   *
   * @return The data is only valid between offset and offset+length.
   */
  public byte[] get() {
    if (this.bytes == null) {
      throw new IllegalStateException(
          "Uninitialiized. Null constructor called w/o accompaying readFields invocation");
    }
    return this.bytes;
  }

  /**
   * @param b Use passed bytes as backing array for this instance.
   */
  public void set(final byte[] b) {
    set(b, 0, b.length);
  }

  /**
   * @param b Use passed bytes as backing array for this instance.
   */
  public void set(final byte[] b, final int offset, final int length) {
    this.bytes = b;
    this.offset = offset;
    this.length = length;
  }

  /**
   * @return the number of valid bytes in the buffer
   */
  public int getLength() {
    if (this.bytes == null) {
      throw new IllegalStateException(
          "Uninitialiized. Null constructor called w/o accompaying readFields invocation");
    }
    return this.length;
  }

  /**
   * @return offset
   */
  public int getOffset() {
    return this.offset;
  }

  @Override
  public int hashCode() {
    return ByteUtils.hashCode(bytes, offset, length);
  }

  /**
   * Define the sort order of the Bytes.
   *
   * @param that The other bytes writable
   * @return Positive if left is bigger than right, 0 if they are equal, and
   *     negative if left is smaller than right.
   */
  @Override
  public int compareTo(Bytes that) {
    return ByteUtils.BYTES_RAWCOMPARATOR.compare(
        this.bytes, this.offset, this.length,
        that.bytes, that.offset, that.length);
  }

  /**
   * Compares the bytes in this object to the specified byte array
   *
   * @return Positive if left is bigger than right, 0 if they are equal, and
   *     negative if left is smaller than right.
   */
  public int compareTo(final byte[] that) {
    return ByteUtils.BYTES_RAWCOMPARATOR.compare(
        this.bytes, this.offset, this.length,
        that, 0, that.length);
  }

  /**
   * @see Object#equals(Object)
   */
  @Override
  public boolean equals(Object o) {
    if (o instanceof byte[]) {
      return compareTo((byte[]) o) == 0;
    }
    if (o instanceof Bytes) {
      return compareTo((Bytes) o) == 0;
    }
    return false;
  }

  /**
   * @see Object#toString()
   */
  @Override
  public String toString() {
    return ByteUtils.toString(bytes, offset, length);
  }

  /**
   * Returns a copy of the bytes referred to by this writable
   */
  public byte[] copyBytes() {
    return Arrays.copyOfRange(bytes, offset, offset + length);
  }

}
