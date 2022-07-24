package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;
import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

import org.assertj.core.api.*;
import org.junit.*;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.obsidiandynamics.zerolog.*;

public final class JacksonVariantSerializationTest {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();

  private static ObjectMapper createObjectMapper() {
    return new ObjectMapper().registerModule(new JacksonVariantModule());
  }

  private static void logEncoded(String encoded) {
    zlg.t("encoded %s", z -> z.arg(encoded));
  }

  private static final class TestClass {
    @JsonProperty
    private final String a;

    @JsonProperty
    private final int b;

    TestClass(@JsonProperty("a") String a, @JsonProperty("b") int b) {
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

  private static MonoVariant emulatePacked(ObjectMapper mapper, String contentType, int contentVersion, Object content) {
    final var packed = mapper.<JsonNode>valueToTree(content);
    final var parser = new TreeTraversingParser(packed);
    return new MonoVariant(new ContentHandle(contentType, contentVersion), new JacksonPackedForm(parser, packed), null);
  }

  private static MonoVariant capture(String contentType, int contentVersion, Object content) {
    return new MonoVariant(new ContentHandle(contentType, contentVersion), null, content);
  }

  private static void assertPackedNode(JsonNode expectedNode, MonoVariant v) {
    final var packedForm = mustBeSubtype(v.getPacked(), JacksonPackedForm.class, AssertionError::new);
    assertEquals(expectedNode, packedForm.getNode());
  }

  private static TextNode text(String s) {
    return JsonNodeFactory.instance.textNode(s);
  }

  private static NumericNode number(int v) {
    return JsonNodeFactory.instance.numberNode(v);
  }

  private static ArrayNode array(JsonNode... childNodes) {
    return JsonNodeFactory.instance.arrayNode().addAll(List.of(childNodes));
  }

  private static ObjectNode object(Field... fields) {
    final var node = JsonNodeFactory.instance.objectNode();
    for (var kv : fields) {
      node.set(kv.name, kv.value);
    }
    return node;
  }

  private static Field field(String name, JsonNode value)  {
    return new Field(name, value);
  }

  private static final class Field {
    final String name;
    final JsonNode value;

    Field(String name, JsonNode value) {
      this.name = name;
      this.value = value;
    }
  }

  private static void assertUnpacked(Object expected, MonoVariant v) {
    final var packed = mustBeSubtype(v.getPacked(), JacksonPackedForm.class, AssertionError::new);
    final var unpacked = JacksonUnpacker.getInstance().unpack(packed, expected.getClass());
    assertTrue(expected + " != " + unpacked, Objects.deepEquals(expected, unpacked));
  }

  private static void assertUnpackedSame(Object expected, MonoVariant v) {
    final var packed = mustBeSubtype(v.getPacked(), JacksonPackedForm.class, AssertionError::new);
    final var unpacked = JacksonUnpacker.getInstance().unpack(packed, expected.getClass());
    assertSame(expected, unpacked);
  }

  private ObjectMapper mapper;

  @Before
  public void before() {
    mapper = createObjectMapper();
  }

  @Test
  public void testMonoVariant_prepackedScalar_failWithUnsupportedPackedForm() {
    final var p = new MonoVariant(new ContentHandle("test/scalar", 1), new IdentityPackedForm("scalar"), null);

    Assertions.assertThatThrownBy(() -> {
      mapper.writeValueAsString(p);
    })
    .isInstanceOf(JsonMappingException.class)
    .hasCauseInstanceOf(IllegalStateException.class)
    .hasMessage("Unsupported packed form: IdentityPackedForm");
  }

  @Test
  public void testMonoVariant_prepackedScalar() throws IOException {
    final var p = emulatePacked(mapper, "test/scalar", 1, "scalar");

    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);

    final var d = mapper.readValue(encoded, MonoVariant.class);
    assertPackedNode(text("scalar"), d);
    assertUnpacked("scalar", d);
  }

  @Test
  public void testMonoVariant_serializeScalar() throws IOException {
    final var p = capture("test/scalar", 1, "scalar");

    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);

    final var d = mapper.readValue(encoded, MonoVariant.class);
    assertPackedNode(text("scalar"), d);
    assertUnpacked("scalar", d);
  }

  @Test
  public void testMonoVariant_prepackedArray() throws IOException {
    final var array = new int[] {0, 1, 2};
    final var p = emulatePacked(mapper, "test/array", 1, array);

    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);

    final var d = mapper.readValue(encoded, MonoVariant.class);
    assertPackedNode(array(number(0), number(1), number(2)), d);
    assertUnpacked(array, d);
  }

  @Test
  public void testMonoVariant_serializeArray() throws IOException {
    final var array = new int[] {0, 1, 2};
    final var p = capture("test/array", 1, array);

    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);

    final var d = mapper.readValue(encoded, MonoVariant.class);
    assertPackedNode(array(number(0), number(1), number(2)), d);
    assertUnpacked(array, d);
  }

  @Test
  public void testMonoVariant_prepackedMap() throws IOException {
    final var map = new TreeMap<String, List<String>>();
    map.put("a", List.of("w", "x"));
    map.put("b", List.of("y", "z"));
    final var p = emulatePacked(mapper, "test/map", 1, map);

    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);

    final var d = mapper.readValue(encoded, MonoVariant.class);
    assertPackedNode(object(field("a", array(text("w"), text("x"))), field("b", array(text("y"), text("z")))), d);
    assertUnpacked(map, d);
  }

  @Test
  public void testMonoVariant_serializeMap() throws IOException {
    final var map = new TreeMap<String, List<String>>();
    map.put("a", List.of("w", "x"));
    map.put("b", List.of("y", "z"));
    final var p = capture("test/map", 1, map);

    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);

    final var d = mapper.readValue(encoded, MonoVariant.class);
    assertPackedNode(object(field("a", array(text("w"), text("x"))), field("b", array(text("y"), text("z")))), d);
    assertUnpacked(map, d);
  }

  @Test
  public void testMonoVariant_prepackedObject() throws IOException {
    final var obj = new TestClass("someString", 42);
    final var p = emulatePacked(mapper, "test/obj", 1, obj);

    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);

    final var d = mapper.readValue(encoded, MonoVariant.class);
    assertPackedNode(object(field("a", text("someString")), field("b", number(42))), d);
    assertUnpacked(obj, d);
  }

  @Test
  public void testMonoVariant_serializeObject() throws IOException {
    final var obj = new TestClass("someString", 42);
    final var p = capture("test/obj", 1, obj);

    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);

    final var d = mapper.readValue(encoded, MonoVariant.class);
    assertPackedNode(object(field("a", text("someString")), field("b", number(42))), d);
    assertUnpacked(obj, d);
  }

  @Test
  public void testMonoVariant_serializeObject_readInterfaceType() throws IOException {
    final var obj = new TestClass("someString", 42);
    final var p = capture("test/obj", 1, obj);

    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);

    final var d = (MonoVariant) mapper.readValue(encoded, Variant.class);
    assertPackedNode(object(field("a", text("someString")), field("b", number(42))), d);
    assertUnpacked(obj, d);
  }

  @Test
  public void testMonoVariant_serializeNil() throws IOException {
    final var p = capture("std:nil", 1, Nil.getInstance());

    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);

    final var d = mapper.readValue(encoded, MonoVariant.class);
    assertPackedNode(object(), d);
    assertUnpackedSame(Nil.getInstance(), d);
  }

  @Test
  public void testPolyVariant_serializeObject() throws IOException {
    final var obj0 = new TestClass("someString", 42);
    final var obj1 = new TestClass("someOtherString", 83);
    final var p0 = capture("test/obj-0", 1, obj0);
    final var p1 = capture("test/obj-1", 1, obj1);
    final var pp = new PolyVariant(new MonoVariant[] {p0, p1});

    final var encoded = mapper.writeValueAsString(pp);
    logEncoded(encoded);

    final var pd = mapper.readValue(encoded, PolyVariant.class);
    assertEquals(2, pd.getVariants().length);

    final var d0 = pd.getVariants()[0];
    assertPackedNode(object(field("a", text("someString")), field("b", number(42))), d0);
    assertUnpacked(obj0, d0);

    final var d1 = pd.getVariants()[1];
    assertPackedNode(object(field("a", text("someOtherString")), field("b", number(83))), d1);
    assertUnpacked(obj1, d1);
  }

  @Test
  public void testPolyVariant_serializeObject_readInterfaceType() throws IOException {
    final var obj0 = new TestClass("someString", 42);
    final var p0 = capture("test/obj-0", 1, obj0);
    final var pp = new PolyVariant(new MonoVariant[] {p0});

    final var encoded = mapper.writeValueAsString(pp);
    logEncoded(encoded);

    final var pd = (PolyVariant) mapper.readValue(encoded, Variant.class);
    assertEquals(1, pd.getVariants().length);
    final var d = pd.getVariants()[0];
    assertPackedNode(object(field("a", text("someString")), field("b", number(42))), d);
    assertUnpacked(obj0, d);
  }
}
