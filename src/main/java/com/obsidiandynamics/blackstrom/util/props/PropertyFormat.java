package com.obsidiandynamics.blackstrom.util.props;

import static java.lang.String.*;

import java.util.*;
import java.util.function.*;

public final class PropertyFormat {
  private static final int DEF_PAD = 25;
  
  private PropertyFormat() {}
  
  public static int defaultPad() {
    return DEF_PAD;
  }
  
  public static void printTitle(Consumer<String> logLine, String title) {
    if (title != null) logLine.accept(String.format("%s:", title));
  }
  
  public static int measureKeyWidth(Properties props, Predicate<String> keyPredicate) {
    return Collections.list(props.propertyNames()).stream()
        .map(o -> (String) o)
        .filter(keyPredicate)
        .map(key -> key.length())
        .max(Integer::compare).orElse(0);
  }
  
  public static void printStandard(Consumer<String> logLine, 
                                   String title, 
                                   Properties props,
                                   String keyPrefix) {
    printTitle(logLine, title);
    final Predicate<String> keyPredicate = startsWith(keyPrefix);
    final int rightPad = Math.max(defaultPad(), measureKeyWidth(props, keyPredicate));
    printProps(logLine, props, rightPad(rightPad).andThen(prefix("- ")), prefix(" "), keyPredicate);
  }
  
  public static Function<String, String> leftPad() {
    return leftPad(defaultPad());
  }
  
  public static Function<String, String> leftPad(int chars) {
    return s -> format("%" + chars + "s", s);
  }
  
  public static Function<String, String> rightPad() {
    return rightPad(defaultPad());
  }
  
  public static Function<String, String> rightPad(int chars) {
    return s -> format("%-" + chars + "s", s);
  }
  
  public static Function<String, String> prefix(String prefix) {
    return s -> prefix + s;
  }
  
  public static Function<String, String> suffix(String suffix) {
    return s -> s + suffix;
  }
  
  public static Predicate<String> startsWith(String prefix) {
    return s -> s.startsWith(prefix);
  }
  
  public static Predicate<String> any() {
    return s -> true;
  }
  
  public static void printProps(Consumer<String> logLine,
                                Properties props, 
                                Function<String, String> keyFormat,
                                Function<String, String> valueFormat,
                                Predicate<String> keyPredicate) {
    for (Enumeration<?> keys = props.propertyNames(); keys.hasMoreElements();) {
      final String key = (String) keys.nextElement();
      if (keyPredicate.test(key)) {
        logLine.accept(keyFormat.apply(key) + ":" + valueFormat.apply(props.getProperty(key)));
      }
    }
  }
}
