package com.obsidiandynamics.blackstrom.codec;

final class IdentityUnpacker implements Unpacker<IdentityPackedForm> {
  @Override
  public Class<? extends IdentityPackedForm> getPackedType() {
    return IdentityPackedForm.class;
  }
  
  @Override
  public Object unpack(IdentityPackedForm packed, Class<?> contentClass) {
    return packed.content;
  }

  @Override
  public String toString() {
    return IdentityUnpacker.class.getSimpleName();
  }
}
