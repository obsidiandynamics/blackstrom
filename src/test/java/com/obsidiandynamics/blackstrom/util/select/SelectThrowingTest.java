package com.obsidiandynamics.blackstrom.util.select;

import static com.obsidiandynamics.blackstrom.util.select.Select.*;
import static java.util.function.Predicate.*;
import static org.junit.Assert.*;

import java.util.concurrent.atomic.*;

import org.junit.*;

public final class SelectThrowingTest {
  private static class Once<T> extends AtomicReference<T> {
    private static final long serialVersionUID = 1L;

    void assign(T newValue) {
      assertNull(get());
      super.set(newValue);
    }
  }

  @Test
  public void testWhen() {
    final Once<String> branch = new Once<>();
    Select.from("bar")
    .whenNull().then(() -> branch.assign("null"))
    .when(isEqual("foo")).then(obj -> branch.assign("foo"))
    .when(isEqual("bar")).then(obj -> branch.assign("bar"))
    .otherwise(obj -> branch.assign("otherwise"));

    assertEquals("bar", branch.get());
  }

  @Test
  public void testWhenChecked() throws Exception {
    final Once<String> branch = new Once<>();
    Select.from("bar").checked()
    .whenNull().then(() -> branch.assign("null"))
    .when(isEqual("foo")).then(obj -> branch.assign("foo"))
    .when(isEqual("bar")).then(obj -> branch.assign("bar"))
    .otherwise(obj -> branch.assign("otherwise"));

    assertEquals("bar", branch.get());
  }

  @Test
  public void testOtherwise() {
    final Once<String> branch = new Once<>();
    Select.from("something_else")
    .when(isEqual("foo")).then(obj -> branch.assign("foo"))
    .when(isEqual("bar")).then(obj -> branch.assign("bar"))
    .otherwise(obj -> branch.assign("otherwise"))
    .otherwise(obj -> branch.assign("otherwise_2"));

    assertEquals("otherwise", branch.get());
  }

  @Test
  public void testOtherwiseChecked() throws Exception {
    final Once<String> branch = new Once<>();
    Select.from("something_else").checked()
    .when(isEqual("foo")).then(obj -> branch.assign("foo"))
    .when(isEqual("bar")).then(obj -> branch.assign("bar"))
    .otherwise(obj -> branch.assign("otherwise"))
    .otherwise(obj -> branch.assign("otherwise_2"));

    assertEquals("otherwise", branch.get());
  }

  @Test
  public void testNotEquals() {
    final Once<String> branch = new Once<>();
    Select.from("bar")
    .whenNull().then(() -> branch.assign("null"))
    .when(not(isEqual("bar"))).then(obj -> branch.assign("not_bar"))
    .when(not(isEqual("foo"))).then(obj -> branch.assign("not_foo"))
    .otherwise(obj -> branch.assign("otherwise"));

    assertEquals("not_foo", branch.get());
  }

  @Test
  public void testNotEqualsChecked() throws Exception {
    final Once<String> branch = new Once<>();
    Select.from("bar").checked()
    .whenNull().then(() -> branch.assign("null"))
    .when(not(isEqual("bar"))).then(obj -> branch.assign("not_bar"))
    .when(not(isEqual("foo"))).then(obj -> branch.assign("not_foo"))
    .otherwise(obj -> branch.assign("otherwise"));

    assertEquals("not_foo", branch.get());
  }

  @Test
  public void testNotNull() {
    final Once<String> branch = new Once<>();
    Select.from("bar")
    .whenNull().then(() -> branch.assign("null"))
    .when(isEqual("foo")).then(obj -> branch.assign("foo"))
    .when(isNotNull()).then(obj -> branch.assign("not_null"))
    .otherwise(obj -> branch.assign("otherwise"));

    assertEquals("not_null", branch.get());
  }

  @Test
  public void testNotNullChecked() throws Exception {
    final Once<String> branch = new Once<>();
    Select.from("bar").checked()
    .whenNull().then(() -> branch.assign("null"))
    .when(isEqual("foo")).then(obj -> branch.assign("foo"))
    .when(isNotNull()).then(obj -> branch.assign("not_null"))
    .otherwise(obj -> branch.assign("otherwise"));

    assertEquals("not_null", branch.get());
  }

  @Test
  public void testNull() {
    final Once<String> branch = new Once<>();
    Select.from(null)
    .when(isEqual("foo")).then(obj -> branch.assign("foo"))
    .when(isEqual("bar")).then(obj -> branch.assign("bar"))
    .whenNull().then(() -> branch.assign("null"))
    .otherwise(obj -> branch.assign("otherwise"));

    assertEquals("null", branch.get());
  }

  @Test
  public void testNullChecked() throws Exception {
    final Once<String> branch = new Once<>();
    Select.from(null).checked()
    .when(isEqual("foo")).then(obj -> branch.assign("foo"))
    .when(isEqual("bar")).then(obj -> branch.assign("bar"))
    .whenNull().then(() -> branch.assign("null"))
    .otherwise(obj -> branch.assign("otherwise"));

    assertEquals("null", branch.get());
  }

  @Test
  public void testInstanceOf() {
    final Once<String> branch = new Once<>();
    Select.from(5L)
    .whenNull().then(() -> branch.assign("null"))
    .when(isEqual("foo")).then(obj -> branch.assign("foo"))
    .whenInstanceOf(int.class).then(obj -> branch.assign("int"))
    .whenInstanceOf(Integer.class).then(obj -> branch.assign("Integer"))
    .whenInstanceOf(long.class).then(obj -> branch.assign("long"))
    .whenInstanceOf(Long.class).then(obj -> branch.assign("Long"))
    .whenInstanceOf(Number.class).then(obj -> branch.assign("Number"))
    .otherwise(obj -> branch.assign("otherwise"));

    assertEquals("Long", branch.get());
  }

  @Test
  public void testInstanceOfChecked() throws Exception {
    final Once<String> branch = new Once<>();
    Select.from(5L).checked()
    .whenNull().then(() -> branch.assign("null"))
    .when(isEqual("foo")).then(obj -> branch.assign("foo"))
    .whenInstanceOf(int.class).then(obj -> branch.assign("int"))
    .whenInstanceOf(Integer.class).then(obj -> branch.assign("Integer"))
    .whenInstanceOf(long.class).then(obj -> branch.assign("long"))
    .whenInstanceOf(Long.class).then(obj -> branch.assign("Long"))
    .whenInstanceOf(Number.class).then(obj -> branch.assign("Number"))
    .otherwise(obj -> branch.assign("otherwise"));

    assertEquals("Long", branch.get());
  }

  @Test
  public void testTransform() {
    final Once<String> branch = new Once<>();
    Select.from("5")
    .whenNull().then(() -> branch.assign("null"))
    .when(isEqual("4")).transform(Integer::parseInt).then(obj -> branch.assign("4"))
    .when(isEqual("5")).transform(Integer::parseInt).then(obj -> {
      assertEquals(Integer.class, obj.getClass());
      branch.assign("5");
    })
    .otherwise(obj -> branch.assign("otherwise"));

    assertEquals("5", branch.get());
  }

  @Test
  public void testTransformChecked() throws Exception {
    final Once<String> branch = new Once<>();
    Select.from("5").checked()
    .whenNull().then(() -> branch.assign("null"))
    .when(isEqual("4")).transform(Integer::parseInt).then(obj -> branch.assign("4"))
    .when(isEqual("5")).transform(Integer::parseInt).then(obj -> {
      assertEquals(Integer.class, obj.getClass());
      branch.assign("5");
    })
    .otherwise(obj -> branch.assign("otherwise"));

    assertEquals("5", branch.get());
  }

  @Test
  public void testReturn() {
    final String retVal = Select.withReturn(String.class).from(5L)
        .whenNull().thenReturn(() -> "was null")
        .when(isEqual(1L)).thenReturn(obj -> "was one")
        .when(isEqual(5L)).thenReturn(obj -> "was five")
        .otherwiseReturn(obj -> "was something else")
        .getReturn();

    assertEquals("was five", retVal);
  }

  @Test
  public void testReturnChecked() throws Exception {
    final String retVal = Select.withReturn(String.class).from(5L).checked()
        .whenNull().thenReturn(() -> "was null")
        .when(isEqual(1L)).thenReturn(obj -> "was one")
        .when(isEqual(5L)).thenReturn(obj -> "was five")
        .otherwiseReturn(obj -> "was something else")
        .getReturn();

    assertEquals("was five", retVal);
  }

  @Test
  public void testReturnNull() {
    final String retVal = Select.<String>withReturn().from(10L)
        .whenNull().thenReturn(() -> "was null")
        .when(isEqual(1L)).thenReturn(obj -> "was one")
        .when(isEqual(5L)).thenReturn(obj -> "was five")
        .getReturn();

    assertNull(retVal);
  }

  @Test
  public void testReturnNullChecked() throws Exception {
    final String retVal = Select.<String>withReturn().from(10L).checked()
        .whenNull().thenReturn(() -> "was null")
        .when(isEqual(1L)).thenReturn(obj -> "was one")
        .when(isEqual(5L)).thenReturn(obj -> "was five")
        .getReturn();

    assertNull(retVal);
  }
}
