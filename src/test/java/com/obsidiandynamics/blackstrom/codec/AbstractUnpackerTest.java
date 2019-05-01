package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;
import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

public abstract class AbstractUnpackerTest {
  static abstract class BaseSchema {
    private final String type;

    private final String value;

    BaseSchema(String type, String value) {
      this.type = mustExist(type, "Type cannot be null");
      this.value = mustExist(value, "Value cannot be null");
    }

    static final String validateType(String expected, String given) {
      mustBeEqual(expected, given, 
                  withMessage(() -> "Wrong type; expected: " + expected + ", got: " + given, IllegalArgumentException::new));
      return given;
    }

    public final String getType() {
      return type;
    }

    public final String getValue() {
      return value;
    }

    @Override
    public final int hashCode() {
      final var prime = 31;
      var result = 1;
      result = prime * result + Objects.hashCode(type);
      result = prime * result + Objects.hashCode(value);
      return result;
    }

    @Override
    public final boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj instanceof BaseSchema) {
        final var that = (BaseSchema) obj;
        return Objects.equals(type, that.type) && Objects.equals(value, that.value);
      } else {
        return false;
      }
    }

    @Override
    public final String toString() {
      return this.getClass().getSimpleName() + " [type=" + type + ", value=" + value + "]";
    }
  }

  static final class SchemaFoo_v0 extends BaseSchema {
    private static final String TYPE = SchemaFoo_v0.class.getSimpleName();
    
    public SchemaFoo_v0(String type, String value) {
      super(validateType(TYPE, type), value);
    }
    
    public static SchemaFoo_v0 instantiate(String value) {
      return new SchemaFoo_v0(TYPE, value);
    }
  }

  static final class SchemaBar_v0 {}

  static final class SchemaFoo_v1 {}

  static final class SchemaBar_v1 {}

  private Unpacker<?> unpacker;

  @Before
  public final void before() {
    unpacker = getUnpacker();
  }

  protected abstract Unpacker<?> getUnpacker();

  protected abstract <T> T roundTrip(T obj);

  @Test
  public final void testPubSubSameVersion() {
    final var unpacker = getUnpacker();
    final var pubVers = new ContentVersions()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class);
    
    final var pub = SchemaFoo_v0.instantiate("foo version 0");
    final var pubV = pubVers.pack(pub);
    final var subV = roundTrip(pubV);
    
    final var subVers = new ContentVersions()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class);
    final var sub = subVers.unpack(subV);
    assertEquals(pub, sub);
  }
}
