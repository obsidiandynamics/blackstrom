package com.obsidiandynamics.blackstrom.util;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import com.obsidiandynamics.indigo.util.*;

public final class Launcher {
  @FunctionalInterface
  public interface ConsoleWriter {
    void printf(String format, Object ... args);
  }
  
  @FunctionalInterface
  public interface ConsoleReader {
    String readLine() throws IOException;
  }
  
  @FunctionalInterface
  public interface ClassRunner {
    void run(String className) throws Exception;
  }
  
  private int packageCompressLevel = PropertyUtils.get("launcher.package.compress.level", Integer::parseInt, 0);
  
  private String partialClassName = PropertyUtils.get("launcher.class", String::valueOf, null);
  
  public Launcher withPackageCompressLevel(int packageCompressLevel) {
    this.packageCompressLevel = packageCompressLevel;
    return this;
  }

  public Launcher withPartialClassName(String partialClassName) {
    this.partialClassName = partialClassName;
    return this;
  }

  public void run(String[] classes, ConsoleWriter out, ConsoleWriter err, ConsoleReader in, ClassRunner runner) throws Exception {
    Arrays.sort(classes);
    if (partialClassName != null) {
      final String className = matchClass(partialClassName, classes);
      if (className != null) {
        out.printf("Running %s...\n", className);
        runner.run(className);
      } else {
        err.printf("Error: could not match class '%s'\n", partialClassName);
      }
    } else {
      out.printf("Select class to run\n");
      for (int i = 0; i < classes.length; i++) {
        out.printf("[%2d] %s\n", i + 1, formatClass(classes[i], packageCompressLevel));
      }
      
      final String read = in.readLine();
      if (read != null && ! read.trim().isEmpty()) {
        final int index;
        try {
          index = Integer.parseInt(read.trim()) - 1;
        } catch (NumberFormatException e) {
          err.printf("Invalid selection '%s'\n", read.trim());
          return;
        }
        if (index < 0 || index >= classes.length) {
          err.printf("Invalid selection '%s'\n", read.trim());
          return;
        }
        out.printf("Running %s...\n", classes[index]);
        runner.run(classes[index]);
      } else {
        out.printf("Exiting\n");
      }
    }
  }
  
  static String matchClass(String partialClassName, String[] classes) {
    return Arrays.stream(classes).filter(c -> c.endsWith(partialClassName)).findAny().orElse(null);
  }
  
  static CharSequence formatClass(String className, int packageCompressLevel) {
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
  
  static void run(String className) throws Exception {
    final Class<?> cls = Class.forName(className);
    final Method main = cls.getMethod("main", String[].class);
    main.invoke(null, (Object) new String[0]);
  }
  
  static BufferedReader getSystemInReader() {
    return new BufferedReader(new InputStreamReader(System.in));
  }
  
  public static void main(String[] args) {
    final ConsoleWriter out = System.out::printf;
    final ConsoleWriter err = System.err::printf;
    final ConsoleReader in = getSystemInReader()::readLine;
    final ClassRunner runner = Launcher::run;
    try {
      new Launcher().run(args, out, err, in, runner);
    } catch (Exception e) {
      err.printf("Error: %s\n", e);
      e.printStackTrace();
    }
  }
}
