package com.obsidiandynamics.blackstrom.util.props;

import java.util.*;

public final class PropertyManip {
  private PropertyManip() {}
  
  public static Properties mergeProps(Properties... propertiesArray) {
    final Properties merged = new Properties();
    for (Properties props : propertiesArray) {
      Collections.list(props.propertyNames()).stream()
      .map(o -> (String) o)
      .forEach(key -> merged.put(key, props.getProperty(key)));
    }
    return merged;
  }
}
