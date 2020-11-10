package com.google.cloud.datastore;

import com.google.protobuf.ByteString;
import java.util.Optional;

public final class ReflectionHacks {
  private ReflectionHacks() {}

  public static Optional<ByteString> getTransactionId(DatastoreReaderWriter drw) {
    if (drw instanceof TransactionImpl) {
      TransactionImpl tx = (TransactionImpl) drw;
      return Optional.ofNullable(tx.getTransactionId());
    } else {
      return Optional.empty();
    }
  }

  public static Optional<String> getTransactionIdAsString(DatastoreReaderWriter drw) {
    return getTransactionId(drw)
        // .map(bs -> Base64.getEncoder().encodeToString(bs.toByteArray()));
        .map(ByteString::toStringUtf8);
  }

}
