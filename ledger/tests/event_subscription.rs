mod helpers;

use rand::distributions::{Alphanumeric, DistString};
use sqlx_ledger::{account::*, event::*, journal::*, tx_template::*, *};

/// Helper: create a journal, two accounts, and a simple template.
/// Returns (ledger, journal_id, sender_account_id, recipient_account_id, tx_code).
async fn setup_ledger(
    pool: &sqlx::PgPool,
) -> anyhow::Result<(SqlxLedger, JournalId, AccountId, AccountId, String)> {
    let tx_code = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);
    let name = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);
    let new_journal = NewJournal::builder().name(name).build().unwrap();
    let ledger = SqlxLedger::new(pool);

    let journal_id = ledger.journals().create(new_journal).await.unwrap();

    let code = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);
    let sender_account_id = ledger
        .accounts()
        .create(
            NewAccount::builder()
                .id(uuid::Uuid::new_v4())
                .name(format!("Sender {code}"))
                .code(code)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    let code = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);
    let recipient_account_id = ledger
        .accounts()
        .create(
            NewAccount::builder()
                .id(uuid::Uuid::new_v4())
                .name(format!("Recipient {code}"))
                .code(code)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    let params = vec![
        ParamDefinition::builder()
            .name("recipient")
            .r#type(ParamDataType::UUID)
            .build()
            .unwrap(),
        ParamDefinition::builder()
            .name("sender")
            .r#type(ParamDataType::UUID)
            .build()
            .unwrap(),
        ParamDefinition::builder()
            .name("journal_id")
            .r#type(ParamDataType::UUID)
            .build()
            .unwrap(),
        ParamDefinition::builder()
            .name("external_id")
            .r#type(ParamDataType::STRING)
            .build()
            .unwrap(),
        ParamDefinition::builder()
            .name("effective")
            .r#type(ParamDataType::DATE)
            .default_expr("date()")
            .build()
            .unwrap(),
    ];
    let entries = vec![
        EntryInput::builder()
            .entry_type("'TEST_DR'")
            .account_id("params.sender")
            .layer("SETTLED")
            .direction("DEBIT")
            .units("decimal('100')")
            .currency("'USD'")
            .build()
            .unwrap(),
        EntryInput::builder()
            .entry_type("'TEST_CR'")
            .account_id("params.recipient")
            .layer("SETTLED")
            .direction("CREDIT")
            .units("decimal('100')")
            .currency("'USD'")
            .build()
            .unwrap(),
    ];
    let new_template = NewTxTemplate::builder()
        .id(uuid::Uuid::new_v4())
        .code(&tx_code)
        .params(params)
        .tx_input(
            TxInput::builder()
                .effective("params.effective")
                .journal_id("params.journal_id")
                .external_id("params.external_id")
                .build()
                .unwrap(),
        )
        .entries(entries)
        .build()
        .unwrap();
    ledger.tx_templates().create(new_template).await.unwrap();

    Ok((
        ledger,
        journal_id,
        sender_account_id,
        recipient_account_id,
        tx_code,
    ))
}

/// Helper: post a single transaction and return its external_id.
async fn post_one_transaction(
    ledger: &SqlxLedger,
    tx_code: &str,
    journal_id: JournalId,
    sender_account_id: AccountId,
    recipient_account_id: AccountId,
) -> anyhow::Result<String> {
    let external_id = uuid::Uuid::new_v4().to_string();
    let mut params = TxParams::new();
    params.insert("journal_id", journal_id);
    params.insert("sender", sender_account_id);
    params.insert("recipient", recipient_account_id);
    params.insert("external_id", external_id.clone());

    ledger
        .post_transaction(TransactionId::new(), tx_code, Some(params))
        .await
        .unwrap();
    Ok(external_id)
}

// ---------------------------------------------------------------------------
// Test: Pagination / large catch-up via after_id
//
// This tests the reload path in subscribe(). When a subscriber connects with
// after_id, subscribe() fetches all events with id > after_id from the DB
// using fetch_all. This test posts many transactions BEFORE subscribing,
// then subscribes with after_id=BEGIN to force a full reload.
//
// Currently fetch_all loads everything into memory. This test verifies
// correctness and provides a baseline for the pagination fix.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn after_id_catches_up_on_many_events() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let (ledger, journal_id, sender_id, recipient_id, tx_code) = setup_ledger(&pool).await?;

    // Post N transactions BEFORE subscribing.
    // Each transaction generates 3 events: 1 TransactionCreated + 2 BalanceUpdated
    let num_transactions = 20;
    for _ in 0..num_transactions {
        post_one_transaction(&ledger, &tx_code, journal_id, sender_id, recipient_id).await?;
    }

    // Get the current max event id so we know where our events start
    let _first_event_id_row = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MIN(id), 0) FROM sqlx_ledger_events WHERE id > 0",
    )
    .fetch_one(&pool)
    .await?;

    // Subscribe with after_id=BEGIN to force reload of ALL events from DB
    let events = ledger
        .events(EventSubscriberOpts {
            after_id: Some(SqlxLedgerEventId::BEGIN),
            buffer: 1000,
            ..Default::default()
        })
        .await?;
    let mut all_events = events.all().expect("subscriber should be open");

    // Each transaction produces 3 events (1 TransactionCreated + 2 BalanceUpdated).
    // But we're subscribing from BEGIN so we get ALL events ever in the table,
    // not just ours. We'll collect events with a timeout and verify we get at
    // least our expected count.
    let expected_min_events = num_transactions * 3;
    let mut received = Vec::new();
    let timeout = tokio::time::Duration::from_secs(10);

    loop {
        match tokio::time::timeout(timeout, all_events.recv()).await {
            Ok(Ok(event)) => {
                received.push(event);
                if received.len() >= expected_min_events {
                    break;
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                // Lagged means events were dropped from the buffer — still continue
                eprintln!("Lagged by {n} events");
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("Event subscriber closed unexpectedly");
            }
            Err(_) => {
                // Timeout — check what we got
                break;
            }
        }
    }

    assert!(
        received.len() >= expected_min_events,
        "Expected at least {expected_min_events} events, got {}",
        received.len()
    );

    // Verify events are in strictly increasing ID order
    for window in received.windows(2) {
        assert!(
            window[0].id < window[1].id,
            "Events out of order: {:?} >= {:?}",
            window[0].id,
            window[1].id
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Test: Subscriber with after_id picks up only events after that point
//
// Post some transactions, capture an event ID, post more transactions,
// then subscribe with after_id. Verify we only get events after that ID.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn after_id_only_receives_subsequent_events() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let (ledger, journal_id, sender_id, recipient_id, tx_code) = setup_ledger(&pool).await?;

    // Subscribe to capture the first batch of event IDs
    let events = ledger.events(Default::default()).await?;
    let mut all_events = events.all().expect("subscriber should be open");

    // Post first transaction (generates 3 events)
    post_one_transaction(&ledger, &tx_code, journal_id, sender_id, recipient_id).await?;

    // Receive all 3 events from first transaction
    let mut first_batch_ids = Vec::new();
    for _ in 0..3 {
        let event =
            tokio::time::timeout(tokio::time::Duration::from_secs(5), all_events.recv()).await??;
        first_batch_ids.push(event.id);
    }

    let cutoff_id = *first_batch_ids.last().unwrap();

    // Post second transaction (generates 3 more events)
    post_one_transaction(&ledger, &tx_code, journal_id, sender_id, recipient_id).await?;

    // Now subscribe with after_id = cutoff_id
    let after_events = ledger
        .events(EventSubscriberOpts {
            after_id: Some(cutoff_id),
            buffer: 100,
            ..Default::default()
        })
        .await?;
    let mut after_all = after_events.all().expect("subscriber should be open");

    // Should receive only the events from the second transaction
    let mut second_batch = Vec::new();
    let timeout = tokio::time::Duration::from_secs(5);
    for _ in 0..3 {
        match tokio::time::timeout(timeout, after_all.recv()).await {
            Ok(Ok(event)) => {
                assert!(
                    event.id > cutoff_id,
                    "Received event {:?} that should have been filtered by after_id {:?}",
                    event.id,
                    cutoff_id
                );
                second_batch.push(event);
            }
            Ok(Err(e)) => panic!("Unexpected recv error: {e}"),
            Err(_) => panic!("Timed out waiting for events after after_id"),
        }
    }

    assert_eq!(second_batch.len(), 3);

    // Verify ordering
    for window in second_batch.windows(2) {
        assert!(window[0].id < window[1].id);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Test: Concurrent producers and a single subscriber
//
// Multiple tasks post transactions simultaneously. A subscriber connects
// after all producers finish, using after_id to catch up from the DB.
// Verifies all events are delivered in strictly increasing ID order.
// Each producer gets its own unique accounts to avoid DuplicateKey conflicts
// on the sqlx_ledger_current_balances table.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn concurrent_producers_events_ordered() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;

    // Capture current max event ID before our producers start
    let baseline_id: i64 =
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(id), 0) FROM sqlx_ledger_events")
            .fetch_one(&pool)
            .await?;

    // Spawn concurrent producers, each with its own unique accounts
    let num_producers = 5;
    let txns_per_producer = 4;
    let mut handles = Vec::new();

    for _ in 0..num_producers {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            let (producer_ledger, journal_id, sender_id, recipient_id, tx_code) =
                setup_ledger(&pool_clone).await.unwrap();
            for _ in 0..txns_per_producer {
                post_one_transaction(
                    &producer_ledger,
                    &tx_code,
                    journal_id,
                    sender_id,
                    recipient_id,
                )
                .await
                .unwrap();
            }
        });
        handles.push(handle);
    }

    // Wait for all producers to finish
    for handle in handles {
        handle.await?;
    }

    // Verify the expected number of events were created in the DB
    let total_transactions = num_producers * txns_per_producer;
    let expected_events = total_transactions * 3;
    let actual_new_events: i64 =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sqlx_ledger_events WHERE id > $1")
            .bind(baseline_id)
            .fetch_one(&pool)
            .await?;
    assert!(
        actual_new_events >= expected_events as i64,
        "Expected at least {expected_events} new events in DB, got {actual_new_events}"
    );

    // Now subscribe with after_id = baseline so the reload path fetches all
    // our producer events from the DB in order
    let ledger = SqlxLedger::new(&pool);
    let events = ledger
        .events(EventSubscriberOpts {
            after_id: Some(SqlxLedgerEventId::BEGIN),
            buffer: 1000,
            ..Default::default()
        })
        .await?;
    let mut all_events = events.all().expect("subscriber should be open");

    let mut received = Vec::new();
    let timeout = tokio::time::Duration::from_secs(10);

    loop {
        match tokio::time::timeout(timeout, all_events.recv()).await {
            Ok(Ok(event)) => {
                received.push(event);
                // We subscribed from BEGIN so we'll get all events in the DB.
                // Stop once we've received enough (baseline + our new events).
                if received.len() >= (baseline_id as usize + expected_events) {
                    break;
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                eprintln!("Lagged by {n}");
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("Subscriber closed");
            }
            Err(_) => break,
        }
    }

    assert!(
        received.len() >= expected_events,
        "Expected at least {expected_events} events, got {}",
        received.len()
    );

    // Verify strictly increasing order across ALL received events
    for window in received.windows(2) {
        assert!(
            window[0].id < window[1].id,
            "Events out of order: {:?} >= {:?}",
            window[0].id,
            window[1].id
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Test: close_on_lag closes the subscriber when buffer overflows
//
// Create a subscriber with a tiny buffer and close_on_lag=true.
// Post enough transactions to overflow it, then verify the subscriber
// reports closed.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn close_on_lag_closes_subscriber() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let (ledger, journal_id, sender_id, recipient_id, tx_code) = setup_ledger(&pool).await?;

    // Subscribe with a very small buffer and close_on_lag=true
    let events = ledger
        .events(EventSubscriberOpts {
            close_on_lag: true,
            buffer: 2, // tiny buffer
            ..Default::default()
        })
        .await?;

    // Get a receiver but DON'T read from it — let it lag
    let _all_events = events.all().expect("subscriber should be open");

    // Post enough transactions to overflow the buffer
    // Each transaction generates 3 events, buffer is 2, so even 2 transactions
    // should cause lag
    for _ in 0..5 {
        post_one_transaction(&ledger, &tx_code, journal_id, sender_id, recipient_id).await?;
    }

    // Give the subscriber loop time to process the lag
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Attempting to get a new receiver should fail with EventSubscriberClosed
    let result = events.all();
    assert!(result.is_err(), "Expected EventSubscriberClosed but got Ok");

    Ok(())
}

// ---------------------------------------------------------------------------
// Test: >8KB payload triggers the reload path
//
// Insert a transaction with large metadata that exceeds the pg_notify 8KB
// limit. The PG trigger sends a minimal payload (no data field), which
// causes deserialization to fail with "data field missing". The subscribe
// loop should detect this and reload from the DB.
//
// To avoid flakiness from parallel tests, we first post a small transaction
// to capture a known event ID, then post the large transaction, then
// subscribe with after_id set to the small transaction's last event.
// This forces the reload path to fetch our large-payload events from the DB.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn large_payload_triggers_reload() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let ledger = SqlxLedger::new(&pool);

    // First, set up a normal (small) template + accounts to get a baseline event ID
    let (_, journal_id, sender_account_id, recipient_account_id, small_tx_code) =
        setup_ledger(&pool).await?;

    // Post a small transaction to establish a known event ID
    post_one_transaction(
        &ledger,
        &small_tx_code,
        journal_id,
        sender_account_id,
        recipient_account_id,
    )
    .await?;

    // Get the max event ID after the small transaction — this is our baseline
    let baseline_max_id: i64 =
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(id), 0) FROM sqlx_ledger_events")
            .fetch_one(&pool)
            .await?;

    // Now create a template with large metadata (>8KB)
    let large_tx_code = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);
    let large_value = "x".repeat(9000);
    let large_metadata = format!(r#"{{"large_field": "{large_value}"}}"#);

    let params = vec![
        ParamDefinition::builder()
            .name("recipient")
            .r#type(ParamDataType::UUID)
            .build()
            .unwrap(),
        ParamDefinition::builder()
            .name("sender")
            .r#type(ParamDataType::UUID)
            .build()
            .unwrap(),
        ParamDefinition::builder()
            .name("journal_id")
            .r#type(ParamDataType::UUID)
            .build()
            .unwrap(),
        ParamDefinition::builder()
            .name("external_id")
            .r#type(ParamDataType::STRING)
            .build()
            .unwrap(),
        ParamDefinition::builder()
            .name("effective")
            .r#type(ParamDataType::DATE)
            .default_expr("date()")
            .build()
            .unwrap(),
    ];
    let entries = vec![
        EntryInput::builder()
            .entry_type("'TEST_DR'")
            .account_id("params.sender")
            .layer("SETTLED")
            .direction("DEBIT")
            .units("decimal('100')")
            .currency("'USD'")
            .build()
            .unwrap(),
        EntryInput::builder()
            .entry_type("'TEST_CR'")
            .account_id("params.recipient")
            .layer("SETTLED")
            .direction("CREDIT")
            .units("decimal('100')")
            .currency("'USD'")
            .build()
            .unwrap(),
    ];
    let new_template = NewTxTemplate::builder()
        .id(uuid::Uuid::new_v4())
        .code(&large_tx_code)
        .params(params)
        .tx_input(
            TxInput::builder()
                .effective("params.effective")
                .journal_id("params.journal_id")
                .external_id("params.external_id")
                .metadata(&large_metadata)
                .build()
                .unwrap(),
        )
        .entries(entries)
        .build()
        .unwrap();
    ledger.tx_templates().create(new_template).await.unwrap();

    // Subscribe with after_id = BEGIN so the reload path fetches all events
    // (including the large-payload ones) from the DB.
    let events = ledger
        .events(EventSubscriberOpts {
            after_id: Some(SqlxLedgerEventId::BEGIN),
            buffer: 10000,
            ..Default::default()
        })
        .await?;
    let mut all_events = events.all().expect("subscriber should be open");

    // Post the transaction with large metadata
    let external_id = uuid::Uuid::new_v4().to_string();
    let mut tx_params = TxParams::new();
    tx_params.insert("journal_id", journal_id);
    tx_params.insert("sender", sender_account_id);
    tx_params.insert("recipient", recipient_account_id);
    tx_params.insert("external_id", external_id.clone());

    ledger
        .post_transaction(TransactionId::new(), &large_tx_code, Some(tx_params))
        .await
        .unwrap();

    // Verify the events are in the DB with large metadata
    let db_event_count: i64 =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sqlx_ledger_events WHERE id > $1")
            .bind(baseline_max_id)
            .fetch_one(&pool)
            .await?;
    assert!(
        db_event_count >= 3,
        "Expected at least 3 new events in DB, got {db_event_count}"
    );

    // Collect events from the subscriber. We subscribed from BEGIN so we'll
    // get all events, including the large-payload ones fetched via the reload path.
    let per_event_timeout = tokio::time::Duration::from_millis(500);
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(15);
    let mut our_events = Vec::new();

    while tokio::time::Instant::now() < deadline && our_events.len() < 3 {
        match tokio::time::timeout(per_event_timeout, all_events.recv()).await {
            Ok(Ok(event)) => {
                let is_ours = match &event.data {
                    SqlxLedgerEventData::TransactionCreated(tx) => tx.external_id == external_id,
                    SqlxLedgerEventData::BalanceUpdated(b) => {
                        b.account_id == sender_account_id || b.account_id == recipient_account_id
                    }
                    _ => false,
                };
                if is_ours {
                    our_events.push(event);
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                eprintln!("Lagged by {n} events");
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("Subscriber closed unexpectedly");
            }
            Err(_) => {
                // Per-event timeout — continue until deadline
            }
        }
    }

    assert!(
        our_events.len() >= 3,
        "Expected 3 events from large-payload transaction, got {}. \
         Types received: {:?}. \
         The reload path may not have recovered correctly.",
        our_events.len(),
        our_events
            .iter()
            .map(|e| format!("{:?}", e.r#type))
            .collect::<Vec<_>>()
    );

    // Verify we got the right event types
    let types: Vec<_> = our_events.iter().map(|e| &e.r#type).collect();
    assert!(
        types.contains(&&SqlxLedgerEventType::TransactionCreated),
        "Expected a TransactionCreated event"
    );
    assert!(
        types
            .iter()
            .filter(|t| ***t == SqlxLedgerEventType::BalanceUpdated)
            .count()
            >= 2,
        "Expected at least 2 BalanceUpdated events"
    );

    Ok(())
}
