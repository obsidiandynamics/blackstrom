package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;
import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

import org.assertj.core.api.*;
import org.junit.*;
import org.junit.runners.*;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.esotericsoftware.kryo.util.*;
import com.obsidiandynamics.format.*;
import com.obsidiandynamics.zerolog.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class KryoVariantSerializationTest {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private static Pool<Kryo> createKryoPool() {
    return new Pool<>(true, false) {
      @Override
      protected Kryo create () {
        final var kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);
        kryo.addDefaultSerializer(UniVariant.class, new KryoUniVariantSerializer());
        kryo.addDefaultSerializer(MultiVariant.class, new KryoMultiVariantSerializer());
        return kryo;
      }
    };
  }
  
  private static void logEncoded(byte[] encoded) {
    zlg.t("encoded:\n%s", z -> z.arg(Binary.dump(encoded)));
  }
  
  public static final class TestClass {
    private String a;

    private int b;
    
    public TestClass() {}
    
    TestClass(String a, int b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public int hashCode() {
      final var prime = 31;
      var result = 1;
      result = prime * result + Objects.hashCode(a);
      result = prime * result + b;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof TestClass) {
        final var that = (TestClass) obj;
        return Objects.equals(a, that.a) && b == that.b;
      } else {
        return false;
      }
    }
  }
  
  private UniVariant emulatePacked(Pool<Kryo> kryoPool, String contentType, int contentVersion, Object content) {
    return new UniVariant(new ContentHandle(contentType, contentVersion), new KryoPackedForm(writeBytes(content)), null);
  }
  
  private static UniVariant prepare(String contentType, int contentVersion, Object content) {
    return new UniVariant(new ContentHandle(contentType, contentVersion), null, content);
  }

  private Pool<Kryo> kryoPool;
  
  private void assertUnpacked(Object expected, UniVariant v) {
    final var packed = mustBeSubtype(v.getPacked(), KryoPackedForm.class, AssertionError::new);
    final var unpacked = new KryoUnpacker(kryoPool).unpack(packed, expected.getClass());
    assertTrue(expected + " != " + unpacked, Objects.deepEquals(expected, unpacked));
  }
  
  @Before
  public void before() {
    kryoPool = createKryoPool();
  }
  
  private byte[] writeBytes(Object obj) {
    final var kryo = kryoPool.obtain();
    try {
      try (var buffer = new Output(128, -1)) {
        kryo.writeObject(buffer, obj);
        final var length = buffer.position();
        final var bytes = new byte[length];
        System.arraycopy(buffer.getBuffer(), 0, bytes, 0, length);
        return bytes;
      }
    } finally {
      kryoPool.free(kryo);
    }
  }
  
  private <T> T readBytes(byte[] bytes, Class<T> cls) {
    final var kryo = kryoPool.obtain();
    try {
      try (var buffer = new Input(bytes)) {
        return kryo.readObject(buffer, cls);
      }
    } finally {
      kryoPool.free(kryo);
    }
  }
  
  @Test
  public void testUniVariant_prepackedScalar_failWithUnsupportedPackedForm() throws IOException {
    final var p = new UniVariant(new ContentHandle("test/scalar", 1), new IdentityPackedForm("scalar"), null);
    
    Assertions.assertThatThrownBy(() -> {
      writeBytes(p);
    })
    .isInstanceOf(IllegalStateException.class)
    .hasMessage("Unsupported packed form: IdentityPackedForm");
  }
  
  @Test
  public void testUniVariant_prepackedScalar() throws IOException {
    final var p = emulatePacked(kryoPool, "test/scalar", 1, "scalar");
    
    final var encoded = writeBytes(p);
    logEncoded(encoded);
    
    final var d = readBytes(encoded, UniVariant.class);
    assertUnpacked("scalar", d);
  }
  
  @Test
  public void testUniVariant_serializeScalar() throws IOException {
    final var p = prepare("test/scalar", 1, "scalar");
    
    final var encoded = writeBytes(p);
    logEncoded(encoded);
    
    final var d = readBytes(encoded, UniVariant.class);
    assertUnpacked("scalar", d);
  }

  @Test
  public void testUniVariant_prepackedArray() throws IOException {
    final var array = new int[] {0, 1, 2};
    final var p = emulatePacked(kryoPool, "test/array", 1, array);
    
    final var encoded = writeBytes(p);
    logEncoded(encoded);
    
    final var d = readBytes(encoded, UniVariant.class);
    assertUnpacked(array, d);
  }

  @Test
  public void testUniVariant_serializeArray() throws IOException {
    final var array = new int[] {0, 1, 2};
    final var p = prepare("test/array", 1, array);
    
    final var encoded = writeBytes(p);
    logEncoded(encoded);
    
    final var d = readBytes(encoded, UniVariant.class);
    assertUnpacked(array, d);
  }

  @Test
  public void testUniVariant_prepackedMap() throws IOException {
    final var map = new TreeMap<String, List<String>>();
    map.put("a", Arrays.asList("w", "x"));
    map.put("b", Arrays.asList("y", "z"));
    final var p = emulatePacked(kryoPool, "test/map", 1, map);
    
    final var encoded = writeBytes(p);
    logEncoded(encoded);
    
    final var d = readBytes(encoded, UniVariant.class);
    assertUnpacked(map, d);
  }

  @Test
  public void testUniVariant_serializeMap() throws IOException {
    final var map = new TreeMap<String, List<String>>();
    map.put("a", Arrays.asList("w", "x"));
    map.put("b", Arrays.asList("y", "z"));
    final var p = prepare("test/map", 1, map);
    
    final var encoded = writeBytes(p);
    logEncoded(encoded);
    
    final var d = readBytes(encoded, UniVariant.class);
    assertUnpacked(map, d);
  }

  @Test
  public void testUniVariant_prepackedObject() throws IOException {
    final var obj = new TestClass("someString", 42);
    final var p = emulatePacked(kryoPool, "test/obj", 1, obj);
    
    final var encoded = writeBytes(p);
    logEncoded(encoded);
    
    final var d = readBytes(encoded, UniVariant.class);
    assertUnpacked(obj, d);
  }

  @Test
  public void testUniVariant_serializeObject() throws IOException {
    final var obj = new TestClass("someString", 42);
    final var p = prepare("test/obj", 1, obj);
    
    final var encoded = writeBytes(p);
    logEncoded(encoded);
    
    final var d = readBytes(encoded, UniVariant.class);
    assertUnpacked(obj, d);
  }

  @Test
  public void testMultiVariant_serializeObject() throws IOException {
    final var obj0 = new TestClass("someString", 42);
    final var obj1 = new TestClass("someOtherString", 83);
    final var p0 = prepare("test/obj-0", 1, obj0);
    final var p1 = prepare("test/obj-1", 1, obj1);
    final var mp = new MultiVariant(new UniVariant[] {p0, p1});
    
    final var encoded = writeBytes(mp);
    logEncoded(encoded);
    
    final var md = readBytes(encoded, MultiVariant.class);
    assertEquals(2, md.getVariants().length);
    
    final var d0 = md.getVariants()[0];
    assertUnpacked(obj0, d0);
    
    final var d1 = md.getVariants()[1];
    assertUnpacked(obj1, d1);
  }
}
