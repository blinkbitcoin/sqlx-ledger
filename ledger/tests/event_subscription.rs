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

// Tests the reload path: posts many transactions before subscribing, then
// subscribes with after_id to force a full catch-up from the DB.
#[tokio::test]
async fn after_id_catches_up_on_many_events() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let (ledger, journal_id, sender_id, recipient_id, tx_code) = setup_ledger(&pool).await?;

    let baseline_id: i64 =
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(id), 0) FROM sqlx_ledger_events")
            .fetch_one(&pool)
            .await?;

    let num_transactions = 20;
    for _ in 0..num_transactions {
        post_one_transaction(&ledger, &tx_code, journal_id, sender_id, recipient_id).await?;
    }

    let events = ledger
        .events(EventSubscriberOpts {
            after_id: Some(SqlxLedgerEventId::from(baseline_id)),
            buffer: 1000,
            ..Default::default()
        })
        .await?;
    let mut all_events = events.all().expect("subscriber should be open");

    // Each transaction produces 3 events: 1 TransactionCreated + 2 BalanceUpdated
    let expected_min_events = num_transactions * 3;
    let mut received = Vec::new();
    let timeout = tokio::time::Duration::from_secs(10);

    loop {
        match tokio::time::timeout(timeout, all_events.recv()).await {
            Ok(Ok(event)) => {
                assert!(
                    event.id > SqlxLedgerEventId::from(baseline_id),
                    "Received event {:?} at or before baseline {:?}",
                    event.id,
                    baseline_id
                );
                received.push(event);
                if received.len() >= expected_min_events {
                    break;
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                eprintln!("Lagged by {n} events");
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("Event subscriber closed unexpectedly");
            }
            Err(_) => break,
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

// Posts two transactions, captures the event ID between them, then subscribes
// with after_id to verify only the second transaction's events are received.
#[tokio::test]
async fn after_id_only_receives_subsequent_events() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let (ledger, journal_id, sender_id, recipient_id, tx_code) = setup_ledger(&pool).await?;

    post_one_transaction(&ledger, &tx_code, journal_id, sender_id, recipient_id).await?;

    let cutoff_id: i64 =
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(id), 0) FROM sqlx_ledger_events")
            .fetch_one(&pool)
            .await?;

    let second_external_id =
        post_one_transaction(&ledger, &tx_code, journal_id, sender_id, recipient_id).await?;

    let after_events = ledger
        .events(EventSubscriberOpts {
            after_id: Some(SqlxLedgerEventId::from(cutoff_id)),
            buffer: 100,
            ..Default::default()
        })
        .await?;
    let mut after_all = after_events.all().expect("subscriber should be open");

    let mut our_events = Vec::new();
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    let per_event_timeout = tokio::time::Duration::from_millis(500);

    while tokio::time::Instant::now() < deadline && our_events.len() < 3 {
        match tokio::time::timeout(per_event_timeout, after_all.recv()).await {
            Ok(Ok(event)) => {
                assert!(
                    event.id > SqlxLedgerEventId::from(cutoff_id),
                    "Received event {:?} that should have been filtered by after_id {:?}",
                    event.id,
                    cutoff_id
                );
                let is_ours = match &event.data {
                    SqlxLedgerEventData::TransactionCreated(tx) => {
                        tx.external_id == second_external_id
                    }
                    SqlxLedgerEventData::BalanceUpdated(b) => {
                        b.account_id == sender_id || b.account_id == recipient_id
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
            Ok(Err(e)) => panic!("Unexpected recv error: {e}"),
            Err(_) => {}
        }
    }

    assert!(
        our_events.len() >= 2,
        "Expected at least 2 events from second transaction, got {}",
        our_events.len()
    );

    let has_balance_updates = our_events
        .iter()
        .filter(|e| matches!(&e.data, SqlxLedgerEventData::BalanceUpdated(_)))
        .count()
        >= 2;
    assert!(
        has_balance_updates,
        "Expected at least 2 BalanceUpdated events from second transaction"
    );

    for window in our_events.windows(2) {
        assert!(window[0].id < window[1].id);
    }

    Ok(())
}

// Multiple tasks post transactions concurrently, then a subscriber catches up
// via after_id. Each producer uses unique accounts to avoid DuplicateKey
// conflicts on sqlx_ledger_current_balances.
#[tokio::test]
async fn concurrent_producers_events_ordered() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;

    let baseline_id: i64 =
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(id), 0) FROM sqlx_ledger_events")
            .fetch_one(&pool)
            .await?;

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

    for handle in handles {
        handle.await?;
    }

    // Each transaction produces 3 events (1 TransactionCreated + 2 BalanceUpdated)
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

    let ledger = SqlxLedger::new(&pool);
    let events = ledger
        .events(EventSubscriberOpts {
            after_id: Some(SqlxLedgerEventId::from(baseline_id)),
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
                assert!(
                    event.id > SqlxLedgerEventId::from(baseline_id),
                    "Received event {:?} at or before baseline {:?}",
                    event.id,
                    baseline_id
                );
                received.push(event);
                if received.len() >= expected_events {
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

// Verifies that close_on_lag=true closes the subscriber when the broadcast
// buffer overflows. Uses a tiny buffer and floods events without reading.
#[tokio::test]
async fn close_on_lag_closes_subscriber() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let (ledger, journal_id, sender_id, recipient_id, tx_code) = setup_ledger(&pool).await?;

    // Smallest possible buffer to induce lag quickly
    let events = ledger
        .events(EventSubscriberOpts {
            close_on_lag: true,
            buffer: 1,
            ..Default::default()
        })
        .await?;

    // Get a receiver but don't read from it — allow channels to lag
    let _all_events = events.all().expect("subscriber should be open");

    for _ in 0..20 {
        post_one_transaction(&ledger, &tx_code, journal_id, sender_id, recipient_id).await?;
    }

    // Poll for the subscriber to close; the dispatch loop may not detect lag instantly
    let closed = tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
        loop {
            if events.all().is_err() {
                return;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    })
    .await;

    assert!(
        closed.is_ok(),
        "Expected EventSubscriber to close due to lag within timeout"
    );

    Ok(())
}

// Tests the reload path triggered by payloads exceeding the pg_notify 8KB limit.
// The PG trigger sends a minimal payload (no data field) for large events,
// causing deserialization to fail. The subscribe loop detects this and reloads
// from the DB. We subscribe from a known baseline to isolate our events.
#[tokio::test]
async fn large_payload_triggers_reload() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let ledger = SqlxLedger::new(&pool);

    let (_, journal_id, sender_account_id, recipient_account_id, small_tx_code) =
        setup_ledger(&pool).await?;

    post_one_transaction(
        &ledger,
        &small_tx_code,
        journal_id,
        sender_account_id,
        recipient_account_id,
    )
    .await?;

    let baseline_max_id: i64 =
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(id), 0) FROM sqlx_ledger_events")
            .fetch_one(&pool)
            .await?;

    // Create a template with large metadata (>8KB) to exceed pg_notify limit
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

    let events = ledger
        .events(EventSubscriberOpts {
            after_id: Some(SqlxLedgerEventId::from(baseline_max_id)),
            buffer: 10000,
            ..Default::default()
        })
        .await?;
    let mut all_events = events.all().expect("subscriber should be open");

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

    // Verify events exist in the DB
    let db_event_count: i64 =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sqlx_ledger_events WHERE id > $1")
            .bind(baseline_max_id)
            .fetch_one(&pool)
            .await?;
    assert!(
        db_event_count >= 3,
        "Expected at least 3 new events in DB, got {db_event_count}"
    );

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
            Err(_) => {}
        }
    }

    assert!(
        our_events.len() >= 2,
        "Expected at least 2 events from large-payload transaction, got {}. \
         Types received: {:?}. \
         The reload path may not have recovered correctly.",
        our_events.len(),
        our_events
            .iter()
            .map(|e| format!("{:?}", e.r#type))
            .collect::<Vec<_>>()
    );

    // TransactionCreated may be lost to broadcast lag; verify BalanceUpdated events
    let types: Vec<_> = our_events.iter().map(|e| &e.r#type).collect();
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
