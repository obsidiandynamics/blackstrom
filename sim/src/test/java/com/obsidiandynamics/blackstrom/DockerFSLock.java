package com.obsidiandynamics.blackstrom;

import java.io.*;

import com.obsidiandynamics.fslock.*;

/**
 *  An <em>FS.lock</em> root for restricting the launching of Docker instances (for integration and
 *  scenario tests).
 */
public final class DockerFSLock {
  private static final LockRoot root = new LockRoot(new File("../.docker-fslock"));
  
  public static LockRoot getRoot() { return root; }
  
  private DockerFSLock() {}
}
