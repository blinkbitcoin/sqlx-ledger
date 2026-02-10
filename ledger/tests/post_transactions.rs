mod helpers;

use rust_decimal::Decimal;

use rand::distributions::{Alphanumeric, DistString};
use sqlx_ledger::{account::*, balance::AccountBalance, event::*, journal::*, tx_template::*, *};

/// Check if an event belongs to our test, identified by external_id and account IDs.
fn is_our_event(
    event: &SqlxLedgerEvent,
    external_id: &str,
    sender_account_id: AccountId,
    recipient_account_id: AccountId,
) -> bool {
    match &event.data {
        SqlxLedgerEventData::TransactionCreated(tx) => tx.external_id == external_id,
        SqlxLedgerEventData::BalanceUpdated(b) => {
            b.account_id == sender_account_id || b.account_id == recipient_account_id
        }
        _ => false,
    }
}

/// Collect events matching this test's transaction from a broadcast receiver.
/// Returns when `target` events are collected or the deadline expires.
async fn collect_our_events(
    rx: &mut tokio::sync::broadcast::Receiver<SqlxLedgerEvent>,
    external_id: &str,
    sender_account_id: AccountId,
    recipient_account_id: AccountId,
    target: usize,
    deadline: tokio::time::Instant,
) -> Vec<SqlxLedgerEvent> {
    let per_event_timeout = tokio::time::Duration::from_millis(500);
    let mut collected = Vec::new();
    while tokio::time::Instant::now() < deadline && collected.len() < target {
        match tokio::time::timeout(per_event_timeout, rx.recv()).await {
            Ok(Ok(event)) => {
                if is_our_event(&event, external_id, sender_account_id, recipient_account_id) {
                    collected.push(event);
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
            Ok(Err(e)) => panic!("Unexpected recv error: {e}"),
            Err(_) => {}
        }
    }
    collected
}

#[tokio::test]
async fn post_transaction() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;

    let tx_code = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);

    let name = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);
    let new_journal = NewJournal::builder().name(name).build().unwrap();
    let ledger = SqlxLedger::new(&pool);

    let journal_id = ledger.journals().create(new_journal).await.unwrap();
    let code = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);
    let new_account = NewAccount::builder()
        .id(uuid::Uuid::new_v4())
        .name(format!("Test Sender Account {code}"))
        .code(code)
        .build()
        .unwrap();
    let sender_account_id = ledger.accounts().create(new_account).await.unwrap();
    let code = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);
    let new_account = NewAccount::builder()
        .id(uuid::Uuid::new_v4())
        .name(format!("Test Recipient Account {code}"))
        .code(code)
        .build()
        .unwrap();
    let recipient_account_id = ledger.accounts().create(new_account).await.unwrap();

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
            .entry_type("'TEST_BTC_DR'")
            .account_id("params.sender")
            .layer("SETTLED")
            .direction("DEBIT")
            .units("decimal('1290')")
            .currency("'BTC'")
            .build()
            .unwrap(),
        EntryInput::builder()
            .entry_type("'TEST_BTC_CR'")
            .account_id("params.recipient")
            .layer("SETTLED")
            .direction("CREDIT")
            .units("decimal('1290')")
            .currency("'BTC'")
            .build()
            .unwrap(),
        EntryInput::builder()
            .entry_type("'TEST_USD_DR'")
            .account_id("params.sender")
            .layer("SETTLED")
            .direction("DEBIT")
            .units("decimal('100')")
            .currency("'USD'")
            .build()
            .unwrap(),
        EntryInput::builder()
            .entry_type("'TEST_USD_CR'")
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
                .metadata(r#"{"foo": "bar"}"#)
                .build()
                .unwrap(),
        )
        .entries(entries)
        .build()
        .unwrap();
    ledger.tx_templates().create(new_template).await.unwrap();

    let baseline_id: i64 =
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(id), 0) FROM sqlx_ledger_events")
            .fetch_one(&pool)
            .await?;

    let external_id = uuid::Uuid::new_v4().to_string();
    let mut params = TxParams::new();
    params.insert("journal_id", journal_id);
    params.insert("sender", sender_account_id);
    params.insert("recipient", recipient_account_id);
    params.insert("external_id", external_id.clone());

    ledger
        .post_transaction(TransactionId::new(), &tx_code, Some(params))
        .await
        .unwrap();
    let transactions = ledger
        .transactions()
        .list_by_external_ids(vec![external_id.clone()])
        .await?;
    assert_eq!(transactions.len(), 1);

    let entries = ledger
        .entries()
        .list_by_transaction_ids(vec![transactions[0].id])
        .await?;
    assert!(entries.get(&transactions[0].id).is_some());
    assert_eq!(entries.get(&transactions[0].id).unwrap().len(), 4);

    // Subscribe after posting so the after_id catch-up reliably fetches our events
    // from the DB, avoiding broadcast lag from concurrent tests.
    let events = ledger
        .events(EventSubscriberOpts {
            after_id: Some(SqlxLedgerEventId::from(baseline_id)),
            buffer: 10000,
            ..Default::default()
        })
        .await?;
    let mut sender_account_balance_events = events
        .account_balance(journal_id, sender_account_id)
        .await
        .expect("event subscriber closed");
    let mut journal_events = events
        .journal(journal_id)
        .await
        .expect("event subscriber closed");

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);

    // Collect events from journal and account-balance channels concurrently
    let ext_id = external_id.clone();
    let journal_handle = tokio::spawn(async move {
        collect_our_events(
            &mut journal_events,
            &ext_id,
            sender_account_id,
            recipient_account_id,
            5,
            deadline,
        )
        .await
    });
    let ext_id = external_id.clone();
    let balance_handle = tokio::spawn(async move {
        collect_our_events(
            &mut sender_account_balance_events,
            &ext_id,
            sender_account_id,
            recipient_account_id,
            2,
            deadline,
        )
        .await
    });

    let journal_collected = journal_handle.await?;
    let sender_balance_collected = balance_handle.await?;

    // Verify sender account balance channel only receives BalanceUpdated events
    assert!(
        !sender_balance_collected.is_empty(),
        "Sender account balance channel should receive events"
    );
    assert!(
        sender_balance_collected
            .iter()
            .all(|e| e.r#type == SqlxLedgerEventType::BalanceUpdated),
        "Sender account balance channel should only contain BalanceUpdated events"
    );

    // Verify journal channel receives both TransactionCreated and BalanceUpdated
    let journal_tx_count = journal_collected
        .iter()
        .filter(|e| e.r#type == SqlxLedgerEventType::TransactionCreated)
        .count();
    let journal_balance_count = journal_collected
        .iter()
        .filter(|e| e.r#type == SqlxLedgerEventType::BalanceUpdated)
        .count();
    assert_eq!(
        journal_tx_count, 1,
        "Journal channel should have 1 TransactionCreated"
    );
    assert!(
        journal_balance_count >= 2,
        "Journal channel should have at least 2 BalanceUpdated, got {journal_balance_count}"
    );

    // Verify events are in strictly increasing ID order
    for window in journal_collected.windows(2) {
        assert!(window[0].id < window[1].id, "journal_events out of order");
    }

    // Find the TransactionCreated event for after_id verification
    let tx_event = journal_collected
        .iter()
        .find(|e| e.r#type == SqlxLedgerEventType::TransactionCreated)
        .expect("should have TransactionCreated event");

    // Verify after_id subscription receives events after the TransactionCreated
    let after_events = ledger
        .events(EventSubscriberOpts {
            after_id: Some(tx_event.id),
            buffer: 10000,
            ..Default::default()
        })
        .await?;
    let mut after_all = after_events.all().unwrap();
    let after_deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    let after_collected = collect_our_events(
        &mut after_all,
        &external_id,
        sender_account_id,
        recipient_account_id,
        1,
        after_deadline,
    )
    .await;
    assert!(
        !after_collected.is_empty(),
        "after_id subscription should receive events after TransactionCreated"
    );
    assert_eq!(
        after_collected[0].r#type,
        SqlxLedgerEventType::BalanceUpdated,
        "First event after TransactionCreated should be BalanceUpdated"
    );

    let usd = rusty_money::iso::find("USD").unwrap();
    let btc = rusty_money::crypto::find("BTC").unwrap();

    let usd_credit_balance = get_balance(
        &ledger,
        journal_id,
        recipient_account_id,
        Currency::Iso(usd),
    )
    .await?;
    assert_eq!(usd_credit_balance.settled(), Decimal::from(100));

    let btc_credit_balance = get_balance(
        &ledger,
        journal_id,
        recipient_account_id,
        Currency::Crypto(btc),
    )
    .await?;
    assert_eq!(btc_credit_balance.settled(), Decimal::from(1290));

    let btc_debit_balance = get_balance(
        &ledger,
        journal_id,
        sender_account_id,
        Currency::Crypto(btc),
    )
    .await?;
    assert_eq!(btc_debit_balance.settled(), Decimal::from(-1290));

    let usd_credit_balance =
        get_balance(&ledger, journal_id, sender_account_id, Currency::Iso(usd)).await?;
    assert_eq!(usd_credit_balance.settled(), Decimal::from(-100));

    let external_id = uuid::Uuid::new_v4().to_string();
    params = TxParams::new();
    params.insert("journal_id", journal_id);
    params.insert("sender", sender_account_id);
    params.insert("recipient", recipient_account_id);
    params.insert("external_id", external_id.clone());

    ledger
        .post_transaction(TransactionId::new(), &tx_code, Some(params))
        .await
        .unwrap();

    let usd_credit_balance = get_balance(
        &ledger,
        journal_id,
        recipient_account_id,
        Currency::Iso(usd),
    )
    .await?;
    assert_eq!(usd_credit_balance.settled(), Decimal::from(200));

    let btc_credit_balance = get_balance(
        &ledger,
        journal_id,
        recipient_account_id,
        Currency::Crypto(btc),
    )
    .await?;
    assert_eq!(btc_credit_balance.settled(), Decimal::from(2580));

    let btc_debit_balance = get_balance(
        &ledger,
        journal_id,
        sender_account_id,
        Currency::Crypto(btc),
    )
    .await?;
    assert_eq!(btc_debit_balance.settled(), Decimal::from(-2580));

    let usd_credit_balance =
        get_balance(&ledger, journal_id, sender_account_id, Currency::Iso(usd)).await?;
    assert_eq!(usd_credit_balance.settled(), Decimal::from(-200));

    Ok(())
}

async fn get_balance(
    ledger: &SqlxLedger,
    journal_id: JournalId,
    account_id: AccountId,
    currency: Currency,
) -> anyhow::Result<AccountBalance> {
    let balance = ledger
        .balances()
        .find(journal_id, account_id, currency)
        .await?
        .unwrap();
    Ok(balance)
}
