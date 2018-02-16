package com.obsidiandynamics.blackstrom.util.select;

import static com.obsidiandynamics.blackstrom.util.select.Select.*;
import static java.util.function.Predicate.*;
import static org.junit.Assert.*;

import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.util.throwing.*;

public final class SelectTest {
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
  public void testWhenChecked() {
    final Once<String> branch = new Once<>();
    Select.from("bar").checked()
    .whenNull().then(() -> branch.assign("null"))
    .when(isEqual("foo")).then(obj -> branch.assign("foo"))
    .when(isEqual("bar")).then(obj -> branch.assign("bar"))
    .otherwise(obj -> branch.assign("otherwise"));

    assertEquals("bar", branch.get());
  }
  
  private static class TestCheckedExceptionFoo extends Exception {
    private static final long serialVersionUID = 1L;
  }
  
  private static class TestCheckedExceptionBar extends Exception {
    private static final long serialVersionUID = 1L;
  }
  
  private static <T, X extends Exception> CheckedConsumer<T, X> doThrow(Supplier<X> generator) {
    return t -> {throw generator.get();};
  }

  @Test(expected=TestCheckedExceptionBar.class)
  public void testWhenCheckedWithThrow() throws TestCheckedExceptionBar, TestCheckedExceptionFoo {
    Select.from("bar").checked()
    .whenNull().then(CheckedRunnable::nop)
    .when(isEqual("foo")).then(doThrow(TestCheckedExceptionFoo::new))
    .when(isEqual("bar")).then(doThrow(TestCheckedExceptionBar::new))
    .when(isEqual("baz")).then(CheckedConsumer::nop)
    .otherwise(obj -> {});
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
  public void testOtherwiseChecked() {
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
  public void testNotEqualsChecked() {
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
  public void testNotNullChecked() {
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
  public void testNullChecked() {
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
  public void testInstanceOfChecked() {
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
  public void testTransformChecked() {
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
    final String retVal = Select.returning(String.class).from(5L)
        .whenNull().thenReturn(() -> "was null")
        .when(isEqual(1L)).thenReturn(obj -> "was one")
        .when(isEqual(5L)).thenReturn(obj -> "was five")
        .otherwiseReturn(obj -> "was something else")
        .getReturn();

    assertEquals("was five", retVal);
  }

  @Test
  public void testReturnChecked() {
    final String retVal = Select.returning(String.class).from(5L).checked()
        .whenNull().thenReturn(() -> "was null")
        .when(isEqual(1L)).thenReturn(obj -> "was one")
        .when(isEqual(5L)).thenReturn(obj -> "was five")
        .otherwiseReturn(obj -> "was something else")
        .getReturn();

    assertEquals("was five", retVal);
  }

  @Test
  public void testReturnNull() {
    final String retVal = Select.<String>returning().from(10L)
        .whenNull().thenReturn(() -> "was null")
        .when(isEqual(1L)).thenReturn(obj -> "was one")
        .when(isEqual(5L)).thenReturn(obj -> "was five")
        .getReturn();

    assertNull(retVal);
  }

  @Test
  public void testReturnNullChecked() {
    final String retVal = Select.<String>returning().from(10L).checked()
        .whenNull().thenReturn(() -> "was null")
        .when(isEqual(1L)).thenReturn(obj -> "was one")
        .when(isEqual(5L)).thenReturn(obj -> "was five")
        .getReturn();

    assertNull(retVal);
  }
}
