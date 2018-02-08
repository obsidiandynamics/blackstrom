package com.obsidiandynamics.blackstrom.util;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import com.obsidiandynamics.indigo.util.*;

public final class MainRunner {
  public static void main(String[] args) throws IOException {
    final int packageCompressLevel = PropertyUtils.get(MainRunner.class.getSimpleName() + ".packageCompressLevel", Integer::parseInt, 0);
    
    System.out.println("Select class to run");
    Arrays.sort(args);
    for (int i = 0; i < args.length; i++) {
      System.out.format("[%2d] %s\n", i + 1, formatClass(args[i], packageCompressLevel));
    }
    
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
      final String read = reader.readLine();
      if (read != null && ! read.trim().isEmpty()) {
        try {
          final int index = Integer.parseInt(read.trim()) - 1;
          try {
            run(args[index]);
          } catch (Exception e) {
            System.err.println("Error: " + e);
          }
        } catch (NumberFormatException e) {
          System.err.println(e.getMessage());
        }
      } else {
        System.out.format("Exiting\n");
      }
    }
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
