package com.obsidiandynamics.blackstrom.util;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import com.obsidiandynamics.indigo.util.*;

public final class Launcher {
  public static void main(String[] args) {
    try {
      run(args);
    } catch (Exception e) {
      System.err.format("Error: %s\n", e);
      e.printStackTrace();
    }
  }
  
  private static void run(String[] classes) throws Exception {
    final int packageCompressLevel = PropertyUtils.get("launcher.packageCompressLevel", Integer::parseInt, 0);
    
    Arrays.sort(classes);
    final String partialClassName = PropertyUtils.get("launcher.class", String::valueOf, null);
    if (partialClassName != null) {
      final String className = matchClass(partialClassName, classes);
      if (className != null) {
        run(className);
      } else {
        System.err.format("Error: could not match class '%s'\n", partialClassName);
      }
    } else {
      System.out.println("Select class to run");
      for (int i = 0; i < classes.length; i++) {
        System.out.format("[%2d] %s\n", i + 1, formatClass(classes[i], packageCompressLevel));
      }
      
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
        final String read = reader.readLine();
        if (read != null && ! read.trim().isEmpty()) {
          final int index;
          try {
            index = Integer.parseInt(read.trim()) - 1;
          } catch (NumberFormatException e) {
            System.err.format("Invalid selection '%s'\n", read.trim());
            return;
          }
          if (index < 0 || index >= classes.length) {
            System.err.format("Invalid selection '%s'\n", read.trim());
            return;
          }
          run(classes[index]);
        } else {
          System.out.format("Exiting\n");
        }
      }
    }
  }
  
  private static String matchClass(String partialClassName, String[] classes) {
    return Arrays.stream(classes).filter(c -> c.endsWith(partialClassName)).findAny().orElse(null);
  }
  
  private static CharSequence formatClass(String className, int packageCompressLevel) {
    final String[] frags = className.split("\\.");
    final StringBuilder formatted = new StringBuilder();
    for (int i = 0; i < frags.length; i++) {
      if (formatted.length() != 0) formatted.append('.');
      if (i < packageCompressLevel && i < frags.length - 1) {
        formatted.append(frags[i].charAt(0));
      } else {
        formatted.append(frags[i]);
      }
    }
    return formatted;
  }
  
  private static void run(String className) throws ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    System.out.format("Running %s...\n", className);
    final Class<?> cls = Class.forName(className);
    final Method main = cls.getMethod("main", String[].class);
    main.invoke(null, (Object) new String[0]);
  }
}
