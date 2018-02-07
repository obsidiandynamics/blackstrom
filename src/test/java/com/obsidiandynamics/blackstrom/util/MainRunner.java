package com.obsidiandynamics.blackstrom.util;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

public final class MainRunner {
  public static void main(String[] args) throws IOException {
    System.out.println("Select class to run");
    Arrays.sort(args);
    for (int i = 0; i < args.length; i++) {
      System.out.format("[%2d] %s\n", i, args[i]);
    }
    
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
      final String read = reader.readLine();
      if (read != null && ! read.trim().isEmpty()) {
        try {
          final int index = Integer.parseInt(read.trim());
          try {
            run(args[index]);
          } catch (Exception e) {
            System.err.println("Error running " + args[index] + ": " + e);
          }
        } catch (NumberFormatException e) {
          System.err.println(e.getMessage());
        }
      } else {
        System.out.format("Existing\n");
      }
    }
  }
  
  private static void run(String className) throws ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    System.out.format("Running %s...\n", className);
    final Class<?> cls = Class.forName(className);
    final Method main = cls.getMethod("main", String[].class);
    main.invoke(null, (Object) new String[0]);
  }
}
