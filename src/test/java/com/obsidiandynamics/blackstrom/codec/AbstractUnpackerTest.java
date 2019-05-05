package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;
import static org.junit.Assert.*;

import java.util.*;

import org.assertj.core.api.*;
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

  static final class SchemaFoo_v1 extends BaseSchema {
    private static final String TYPE = SchemaFoo_v1.class.getSimpleName();
    
    public SchemaFoo_v1(String type, String value) {
      super(validateType(TYPE, type), value);
    }
    
    public static SchemaFoo_v1 instantiate(String value) {
      return new SchemaFoo_v1(TYPE, value);
    }
  }

  static final class SchemaBar_v0 extends BaseSchema {
    private static final String TYPE = SchemaBar_v0.class.getSimpleName();
    
    public SchemaBar_v0(String type, String value) {
      super(validateType(TYPE, type), value);
    }
    
    public static SchemaBar_v0 instantiate(String value) {
      return new SchemaBar_v0(TYPE, value);
    }
  }

  static final class SchemaBar_v1 extends BaseSchema {
    private static final String TYPE = SchemaBar_v1.class.getSimpleName();
    
    public SchemaBar_v1(String type, String value) {
      super(validateType(TYPE, type), value);
    }
    
    public static SchemaBar_v1 instantiate(String value) {
      return new SchemaBar_v1(TYPE, value);
    }
  }

  private Unpacker<?> unpacker;

  @Before
  public final void beforeBase() {
    unpacker = getUnpacker();
  }

  protected abstract Unpacker<?> getUnpacker();

  protected abstract <T> T roundTrip(T obj);

  @Test
  public final void testPubSub_relaxedSingle_match() {
    final var pubConmap = new ContentMapper()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class)
        .withSnapshot("foo", 1, SchemaFoo_v1.class)
        .withSnapshot("bar", 0, SchemaBar_v0.class);
    
    final var pub = SchemaFoo_v0.instantiate("foo version 0");
    final var pubV = pubConmap.relaxed().prepare(pub);
    final var subV = roundTrip(pubV);
    
    final var subConmap = new ContentMapper()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class)
        .withSnapshot("foo", 1, SchemaFoo_v1.class)
        .withSnapshot("bar", 0, SchemaBar_v0.class);
    final var sub = subConmap.map(subV);
    assertEquals(pub, sub);
    
    final var resubV = roundTrip(subV); // tests serialization of a pre-packed variant
    final var resub = subConmap.map(resubV);
    assertEquals(pub, resub);
  }

  @Test
  public final void testPubSub_relaxedSingle_matchWrappedInPayload() {
    final var pubConmap = new ContentMapper()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class)
        .withSnapshot("foo", 1, SchemaFoo_v1.class)
        .withSnapshot("bar", 0, SchemaBar_v0.class);
    
    final var pub = SchemaFoo_v0.instantiate("foo version 0");
    final var pubV = pubConmap.relaxed().prepare(pub);
    final var pubP = Payload.pack(pubV);
    final var subP = roundTrip(pubP);
    final var subV = Payload.<UniVariant>unpack(subP);
    
    final var subConmap = new ContentMapper()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class)
        .withSnapshot("foo", 1, SchemaFoo_v1.class)
        .withSnapshot("bar", 0, SchemaBar_v0.class);
    final var sub = subConmap.map(subV);
    assertEquals(pub, sub);
  }

  @Test
  public final void testPubSub_relaxedSingle_unsupportedVersion() {
    final var pubConmap = new ContentMapper()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class)
        .withSnapshot("foo", 1, SchemaFoo_v1.class)
        .withSnapshot("bar", 0, SchemaBar_v0.class);
    
    final var pub = SchemaFoo_v1.instantiate("foo version 1");
    final var pubV = pubConmap.relaxed().prepare(pub);
    final var subV = roundTrip(pubV);
    
    final var subConmap = new ContentMapper()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class)
        .withSnapshot("bar", 0, SchemaBar_v0.class);
    final var sub = subConmap.map(subV);
    assertNull(sub);
  }
  
  @Test
  public final void testPubSub_strictMultiple_firstMatch() {
    final var pubConmap = new ContentMapper()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class)
        .withSnapshot("foo", 1, SchemaFoo_v1.class)
        .withSnapshot("bar", 0, SchemaBar_v0.class);
    
    final var pub1 = SchemaFoo_v1.instantiate("foo version 1");
    final var pub0 = SchemaFoo_v0.instantiate("foo version 0");
    final var pubVs = pubConmap.strict().prepare(pub1, pub0);
    final var subVs = roundTrip(pubVs);
    
    final var subConmap = new ContentMapper()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class)
        .withSnapshot("foo", 1, SchemaFoo_v1.class)
        .withSnapshot("bar", 0, SchemaBar_v0.class);
    final var sub = subConmap.map(subVs);
    assertEquals(pub1, sub);
    
    final var resubVs = roundTrip(subVs); // tests serialization of a pre-packed variant
    final var resub = subConmap.map(resubVs);
    assertEquals(pub1, resub);
  }
  
  @Test
  public final void testPubSub_strictMultiple_fallbackMatch() {
    final var pubConmap = new ContentMapper()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class)
        .withSnapshot("foo", 1, SchemaFoo_v1.class)
        .withSnapshot("bar", 0, SchemaBar_v0.class);
    
    final var pub1 = SchemaFoo_v1.instantiate("foo version 1");
    final var pub0 = SchemaFoo_v0.instantiate("foo version 0");
    final var pubVs = pubConmap.strict().prepare(pub1, pub0);
    final var subVs = roundTrip(pubVs);
    
    final var subConmap = new ContentMapper()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class)
        .withSnapshot("bar", 0, SchemaBar_v0.class);
    final var sub = subConmap.map(subVs);
    assertEquals(pub0, sub);
    
    final var resubVs = roundTrip(subVs); // tests serialization of a pre-packed variant
    final var resub = subConmap.map(resubVs);
    assertEquals(pub0, resub);
  }
  
  @Test
  public final void testPubSub_compactStrictSingle_match() {
    final var pubConmap = new ContentMapper()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 1, SchemaFoo_v1.class)
        .withSnapshot("bar", 0, SchemaBar_v0.class);
    
    final var pub1 = SchemaFoo_v1.instantiate("foo version 1");
    final var pubVs = pubConmap.compactStrict().prepare(pub1);
    final var subVs = roundTrip(pubVs);
    
    final var subConmap = new ContentMapper()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class)
        .withSnapshot("foo", 1, SchemaFoo_v1.class)
        .withSnapshot("bar", 0, SchemaBar_v0.class);
    final var sub = subConmap.map(subVs);
    assertEquals(pub1, sub);
    
    final var resubVs = roundTrip(subVs); // tests serialization of a pre-packed variant
    final var resub = subConmap.map(resubVs);
    assertEquals(pub1, resub);
  }
  
  @Test
  public final void testPubSub_compactStrictMultiple_fallbackMatch() {
    final var pubConmap = new ContentMapper()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class)
        .withSnapshot("foo", 1, SchemaFoo_v1.class)
        .withSnapshot("bar", 0, SchemaBar_v0.class);
    
    final var pub1 = SchemaFoo_v1.instantiate("foo version 1");
    final var pub0 = SchemaFoo_v0.instantiate("foo version 0");
    final var pubVs = pubConmap.compactStrict().prepare(pub1, pub0);
    Assertions.assertThatObject(pubVs).isInstanceOf(MultiVariant.class);
    final var subVs = roundTrip(pubVs);
    
    final var subConmap = new ContentMapper()
        .withUnpacker(unpacker)
        .withSnapshot("foo", 0, SchemaFoo_v0.class)
        .withSnapshot("bar", 0, SchemaBar_v0.class);
    final var sub = subConmap.map(subVs);
    assertEquals(pub0, sub);
    
    final var resubVs = roundTrip(subVs); // tests serialization of a pre-packed variant
    final var resub = subConmap.map(resubVs);
    assertEquals(pub0, resub);
  }
}
