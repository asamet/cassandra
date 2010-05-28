/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.cassandra.thrift;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.*;

/**
 * Encapsulate types of conflict resolution.
 * 
 * @param timestamp. User-supplied timestamp. When two columns with this type of clock conflict, the one with the
 *                   highest timestamp is the one whose value the system will converge to. No other assumptions
 *                   are made about what the timestamp represents, but using microseconds-since-epoch is customary.
 */
public class Clock implements TBase<Clock._Fields>, java.io.Serializable, Cloneable, Comparable<Clock> {
  private static final TStruct STRUCT_DESC = new TStruct("Clock");

  private static final TField TIMESTAMP_FIELD_DESC = new TField("timestamp", TType.I64, (short)1);
  private static final TField CONTEXT_FIELD_DESC = new TField("context", TType.STRING, (short)2);

  public long timestamp;
  public byte[] context;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements TFieldIdEnum {
    TIMESTAMP((short)1, "timestamp"),
    CONTEXT((short)2, "context");

    private static final Map<Integer, _Fields> byId = new HashMap<Integer, _Fields>();
    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byId.put((int)field._thriftId, field);
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      return byId.get(fieldId);
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __TIMESTAMP_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);

  public static final Map<_Fields, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new EnumMap<_Fields, FieldMetaData>(_Fields.class) {{
    put(_Fields.TIMESTAMP, new FieldMetaData("timestamp", TFieldRequirementType.OPTIONAL, 
        new FieldValueMetaData(TType.I64)));
    put(_Fields.CONTEXT, new FieldMetaData("context", TFieldRequirementType.OPTIONAL, 
        new FieldValueMetaData(TType.STRING)));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(Clock.class, metaDataMap);
  }

  public Clock() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Clock(Clock other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    this.timestamp = other.timestamp;
    if (other.isSetContext()) {
      this.context = new byte[other.context.length];
      System.arraycopy(other.context, 0, context, 0, other.context.length);
    }
  }

  public Clock deepCopy() {
    return new Clock(this);
  }

  @Deprecated
  public Clock clone() {
    return new Clock(this);
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public Clock setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    setTimestampIsSet(true);
    return this;
  }

  public void unsetTimestamp() {
    __isset_bit_vector.clear(__TIMESTAMP_ISSET_ID);
  }

  /** Returns true if field timestamp is set (has been asigned a value) and false otherwise */
  public boolean isSetTimestamp() {
    return __isset_bit_vector.get(__TIMESTAMP_ISSET_ID);
  }

  public void setTimestampIsSet(boolean value) {
    __isset_bit_vector.set(__TIMESTAMP_ISSET_ID, value);
  }

  public byte[] getContext() {
    return this.context;
  }

  public Clock setContext(byte[] context) {
    this.context = context;
    return this;
  }

  public void unsetContext() {
    this.context = null;
  }

  /** Returns true if field context is set (has been asigned a value) and false otherwise */
  public boolean isSetContext() {
    return this.context != null;
  }

  public void setContextIsSet(boolean value) {
    if (!value) {
      this.context = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TIMESTAMP:
      if (value == null) {
        unsetTimestamp();
      } else {
        setTimestamp((Long)value);
      }
      break;

    case CONTEXT:
      if (value == null) {
        unsetContext();
      } else {
        setContext((byte[])value);
      }
      break;

    }
  }

  public void setFieldValue(int fieldID, Object value) {
    setFieldValue(_Fields.findByThriftIdOrThrow(fieldID), value);
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TIMESTAMP:
      return new Long(getTimestamp());

    case CONTEXT:
      return getContext();

    }
    throw new IllegalStateException();
  }

  public Object getFieldValue(int fieldId) {
    return getFieldValue(_Fields.findByThriftIdOrThrow(fieldId));
  }

  /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    switch (field) {
    case TIMESTAMP:
      return isSetTimestamp();
    case CONTEXT:
      return isSetContext();
    }
    throw new IllegalStateException();
  }

  public boolean isSet(int fieldID) {
    return isSet(_Fields.findByThriftIdOrThrow(fieldID));
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Clock)
      return this.equals((Clock)that);
    return false;
  }

  public boolean equals(Clock that) {
    if (that == null)
      return false;

    boolean this_present_timestamp = true && this.isSetTimestamp();
    boolean that_present_timestamp = true && that.isSetTimestamp();
    if (this_present_timestamp || that_present_timestamp) {
      if (!(this_present_timestamp && that_present_timestamp))
        return false;
      if (this.timestamp != that.timestamp)
        return false;
    }

    boolean this_present_context = true && this.isSetContext();
    boolean that_present_context = true && that.isSetContext();
    if (this_present_context || that_present_context) {
      if (!(this_present_context && that_present_context))
        return false;
      if (!java.util.Arrays.equals(this.context, that.context))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(Clock other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    Clock typedOther = (Clock)other;

    lastComparison = Boolean.valueOf(isSetTimestamp()).compareTo(typedOther.isSetTimestamp());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTimestamp()) {      lastComparison = TBaseHelper.compareTo(timestamp, typedOther.timestamp);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetContext()).compareTo(typedOther.isSetContext());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetContext()) {      lastComparison = TBaseHelper.compareTo(context, typedOther.context);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id) {
        case 1: // TIMESTAMP
          if (field.type == TType.I64) {
            this.timestamp = iprot.readI64();
            setTimestampIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // CONTEXT
          if (field.type == TType.STRING) {
            this.context = iprot.readBinary();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();

    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (isSetTimestamp()) {
      oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
      oprot.writeI64(this.timestamp);
      oprot.writeFieldEnd();
    }
    if (this.context != null) {
      if (isSetContext()) {
        oprot.writeFieldBegin(CONTEXT_FIELD_DESC);
        oprot.writeBinary(this.context);
        oprot.writeFieldEnd();
      }
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Clock(");
    boolean first = true;

    if (isSetTimestamp()) {
      sb.append("timestamp:");
      sb.append(this.timestamp);
      first = false;
    }
    if (isSetContext()) {
      if (!first) sb.append(", ");
      sb.append("context:");
      if (this.context == null) {
        sb.append("null");
      } else {
          int __context_size = Math.min(this.context.length, 128);
          for (int i = 0; i < __context_size; i++) {
            if (i != 0) sb.append(" ");
            sb.append(Integer.toHexString(this.context[i]).length() > 1 ? Integer.toHexString(this.context[i]).substring(Integer.toHexString(this.context[i]).length() - 2).toUpperCase() : "0" + Integer.toHexString(this.context[i]).toUpperCase());
          }
          if (this.context.length > 128) sb.append(" ...");
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
  }

}

