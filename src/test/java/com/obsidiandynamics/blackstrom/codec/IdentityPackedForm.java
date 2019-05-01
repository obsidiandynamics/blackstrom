package com.obsidiandynamics.blackstrom.codec;

final class IdentityPackedForm implements PackedForm {
  final Object content;

  IdentityPackedForm(Object content) {
    this.content = content;
  }
}
