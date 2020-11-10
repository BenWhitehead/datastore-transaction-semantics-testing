package io.github.benwhitehead.datastore;

import static com.google.common.collect.Sets.newHashSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.ReflectionHacks;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.datastore.v1.TransactionOptions;
import com.google.datastore.v1.TransactionOptions.ReadOnly;
import com.google.datastore.v1.TransactionOptions.ReadWrite;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public final class TransactionReadSemanticTest {
  static {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionReadSemanticTest.class);
  private static final Marker test = MarkerFactory.getMarker("test");
  private static final Marker tx1 = MarkerFactory.getMarker("tx1");
  private static final Marker tx2 = MarkerFactory.getMarker("tx2");

  private static void doTest(final DatastoreOptions datastoreOptions, TransactionOptions tx1Options) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2, new DefaultThreadFactory("tx", true));
    final ListeningExecutorService es = MoreExecutors.listeningDecorator(pool);
    final Datastore ds = datastoreOptions.getService();
    LOGGER.debug(test, "ds = {}", ds);

    KeyFactory txTesting = ds.newKeyFactory().setKind("txTesting");
    final Key aKey = txTesting.newKey("A");

    // expected progression
    // Transaction 1    | Transaction 2
    // -----------------------------------------------------
    // Get A            | ...
    // A.foo == 10      | ...
    // ...              | set A.foo = 42
    // ...              | Put A
    // Get A            | ...
    // A.foo == 10      | ...

    Entity a = Entity.newBuilder(aKey).set("foo", 10).build();
    LOGGER.debug(test, "Creating A...");
    Entity aPrime = ds.put(a);
    LOGGER.debug(test, "Creating A complete ({})", aPrime);

    final CountDownLatch tx1begin = new CountDownLatch(1);
    final CountDownLatch tx2begin = new CountDownLatch(1);
    final CountDownLatch tx1Read1Complete = new CountDownLatch(1);
    final CountDownLatch tx2WriteComplete = new CountDownLatch(1);
    final CountDownLatch tx1Read2Complete = new CountDownLatch(1);

    final Callable<String> txCallable1 = () -> ds.runInTransaction(tx -> {
      ReflectionHacks.getTransactionIdAsString(tx)
          .ifPresent(id ->
              LOGGER.debug(tx1, "transactionId = {}", id)
          );
      LOGGER.debug(tx1, ">>> runInTransaction");
      try {
        LOGGER.debug(tx1, "tx1begin.countDown()");
        tx1begin.countDown();

        LOGGER.debug(tx1, "await(tx2begin) = {}", await(tx2begin));
        Entity aa = tx.get(aKey);
        LOGGER.debug(tx1, "aa.foo = {}", aa.getLong("foo"));
        LOGGER.debug(tx1, "tx1Read1Complete.countDown()");
        tx1Read1Complete.countDown();

        LOGGER.debug(tx1, "await(tx2WriteComplete) = {}", await(tx2WriteComplete));
        Entity aaPrime = tx.get(aKey);
        long aaFoo = aaPrime.getLong("foo");
        LOGGER.debug(tx1, "aa.foo = {}", aaFoo);
        LOGGER.debug(tx1, "tx1Read2Complete.countDown()");
        tx1Read2Complete.countDown();

        return String.format("Tx1 > A.foo = %d", aaFoo);
      } finally {
        LOGGER.debug(tx1, "<<< runInTransaction");
      }
    }, tx1Options);
    final Callable<String> txCallable2 = () -> ds.runInTransaction(tx -> {
      ReflectionHacks.getTransactionIdAsString(tx)
          .ifPresent(id ->
              LOGGER.debug(tx2, "transactionId = {}", id)
          );
      LOGGER.debug(tx2, ">>> runInTransaction");
      try {
        LOGGER.debug(tx2, "tx2begin.countDown()");
        tx2begin.countDown();

        LOGGER.debug(tx2, "await(tx1Read1Complete) = {}", await(tx1Read1Complete));

        Entity aV2 = Entity.newBuilder(aPrime).set("foo", 42).build();
        ds.update(aV2);
        LOGGER.debug(tx2, "tx2writeComplete.countDown()");
        tx2WriteComplete.countDown();

        return String.format("Tx2 > A.foo = %d", 42);
      } finally {
        LOGGER.debug(tx2, "<<< runInTransaction");
      }
    });

    final ListenableFuture<String> f1 = es.submit(txCallable1);
    final ListenableFuture<String> f2 = es.submit(txCallable2);

    //noinspection UnstableApiUsage
    final ListenableFuture<List<String>> all = Futures.allAsList(f1, f2);
    LOGGER.debug(test, "Awaiting transactions...");
    final List<String> txResults = all.get();
    LOGGER.debug(test, "Awaiting transactions complete");
    assertThat(newHashSet(txResults)).isEqualTo(newHashSet(
        "Tx1 > A.foo = 10",
        "Tx2 > A.foo = 42"
    ));
  }

  private static boolean await(CountDownLatch cdl) throws InterruptedException {
    final int timeout = 5;
    final TimeUnit unit = TimeUnit.SECONDS;
    final boolean await = cdl.await(timeout, unit);
    assertTrue(await, String.format("Await timed out after %d %s", timeout, unit));
    return await;
  }

  @Nested
  class CloudDatastore extends TestCases {
    private static final String ENV_VAR_ACCT = "CLOUD_DATASTORE_SERVICE_ACCOUNT_JSON";
    private static final String ENV_VAR_PROJ = "CLOUD_DATASTORE_PROJECT_ID";
    @BeforeEach
    void setUp() throws IOException {
      final String mode = "Cloud Datastore";
      assumeEnvCreds(ENV_VAR_ACCT, ENV_VAR_PROJ, mode);
      datastoreOptions = DatastoreOptions.newBuilder()
          .setProjectId(System.getenv(ENV_VAR_PROJ))
          .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(System.getenv(ENV_VAR_ACCT))))
          .build();
    }
  }

  @Nested
  class FirestoreInDatastoreMode extends TestCases {
    private static final String ENV_VAR_ACCT = "DATASTORE_MODE_SERVICE_ACCOUNT_JSON";
    private static final String ENV_VAR_PROJ = "DATASTORE_MODE_PROJECT_ID";
    @BeforeEach
    void setUp() throws IOException {
      final String mode = "Cloud Firestore in Datastore Mode";
      assumeEnvCreds(ENV_VAR_ACCT, ENV_VAR_PROJ, mode);
      datastoreOptions = DatastoreOptions.newBuilder()
          .setProjectId(System.getenv(ENV_VAR_PROJ))
          .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(System.getenv(ENV_VAR_ACCT))))
          .build();
    }
  }

  static abstract class TestCases {
    protected DatastoreOptions datastoreOptions;

    @Test
    void runSimultaneousTransactions_tx1ReadOnly() throws Exception {
      doTest(datastoreOptions,
          TransactionOptions.newBuilder().setReadOnly(ReadOnly.getDefaultInstance()).build());
    }

    @Test
    void runSimultaneousTransactions_tx1ReadWrite() throws Exception {
      doTest(datastoreOptions,
          TransactionOptions.newBuilder().setReadWrite(ReadWrite.getDefaultInstance()).build());
    }
  }

  private static void assumeEnvCreds(String acctVar, String projVar, String mode) {
    final String acctValue = System.getenv(acctVar);
    final String acctMessage = String.format(
        "%s environment variable not set, skipping tests for " + mode,
        acctVar);
    final boolean acctSkip = acctValue == null || acctValue.isEmpty();
    if (acctSkip) {
      LOGGER.warn(acctMessage);
    }
    final String projValue = System.getenv(projVar);
    final String projMessage = String.format(
        "%s environment variable not set, skipping tests for " + mode,
        projVar);
    final boolean projSkip = projValue == null || projValue.isEmpty();
    if (projSkip) {
      LOGGER.warn(projMessage);
    }
    assumeFalse(acctSkip || projSkip, acctMessage);
  }

}
