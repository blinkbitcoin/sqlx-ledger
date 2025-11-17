//! Use [ledger.events()](crate::SqlxLedger::events()) to subscribe to events triggered by changes to the ledger.
use chrono::{DateTime, Utc};
use serde::{de::Error as _, Deserialize, Serialize};
use sqlx::{postgres::PgListener, PgPool};
use tokio::{
    sync::{
        broadcast::{self, error::RecvError},
        RwLock,
    },
    task,
};
use tracing::instrument;
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetrySpanExt;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crate::{
    balance::BalanceDetails, transaction::Transaction, AccountId, JournalId, SqlxLedgerError,
};

/// Options when initializing the EventSubscriber
pub struct EventSubscriberOpts {
    pub close_on_lag: bool,
    pub buffer: usize,
    pub after_id: Option<SqlxLedgerEventId>,
}
impl Default for EventSubscriberOpts {
    fn default() -> Self {
        Self {
            close_on_lag: false,
            buffer: 100,
            after_id: None,
        }
    }
}

/// Contains fields to store & manage various ledger-related `SqlxLedgerEvent` event receivers.
#[derive(Debug, Clone)]
pub struct EventSubscriber {
    buffer: usize,
    closed: Arc<AtomicBool>,
    #[allow(clippy::type_complexity)]
    balance_receivers:
        Arc<RwLock<HashMap<(JournalId, AccountId), broadcast::Sender<SqlxLedgerEvent>>>>,
    journal_receivers: Arc<RwLock<HashMap<JournalId, broadcast::Sender<SqlxLedgerEvent>>>>,
    all: Arc<broadcast::Receiver<SqlxLedgerEvent>>,
}

impl EventSubscriber {
    pub(crate) async fn connect(
        pool: &PgPool,
        EventSubscriberOpts {
            close_on_lag,
            buffer,
            after_id: start_id,
        }: EventSubscriberOpts,
    ) -> Result<Self, SqlxLedgerError> {
        let closed = Arc::new(AtomicBool::new(false));
        let mut incoming = subscribe(pool.clone(), Arc::clone(&closed), buffer, start_id).await?;
        let all = Arc::new(incoming.resubscribe());
        let balance_receivers = Arc::new(RwLock::new(HashMap::<
            (JournalId, AccountId),
            broadcast::Sender<SqlxLedgerEvent>,
        >::new()));
        let journal_receivers = Arc::new(RwLock::new(HashMap::<
            JournalId,
            broadcast::Sender<SqlxLedgerEvent>,
        >::new()));
        let inner_balance_receivers = Arc::clone(&balance_receivers);
        let inner_journal_receivers = Arc::clone(&journal_receivers);
        let inner_closed = Arc::clone(&closed);
        tokio::spawn(async move {
            loop {
                match incoming.recv().await {
                    Ok(event) => {
                        let journal_id = event.journal_id();
                        if let Some(journal_receivers) =
                            inner_journal_receivers.read().await.get(&journal_id)
                        {
                            let _ = journal_receivers.send(event.clone());
                        }
                        if let Some(account_id) = event.account_id() {
                            let receivers = inner_balance_receivers.read().await;
                            if let Some(receiver) = receivers.get(&(journal_id, account_id)) {
                                let _ = receiver.send(event);
                            }
                        }
                    }
                    Err(RecvError::Lagged(_)) => {
                        if close_on_lag {
                            inner_closed.store(true, Ordering::SeqCst);
                            inner_balance_receivers.write().await.clear();
                            inner_journal_receivers.write().await.clear();
                        }
                    }
                    Err(RecvError::Closed) => {
                        tracing::warn!("Event subscriber closed");
                        inner_closed.store(true, Ordering::SeqCst);
                        inner_balance_receivers.write().await.clear();
                        inner_journal_receivers.write().await.clear();
                        break;
                    }
                }
            }
        });
        Ok(Self {
            buffer,
            closed,
            balance_receivers,
            journal_receivers,
            all,
        })
    }

    pub fn all(&self) -> Result<broadcast::Receiver<SqlxLedgerEvent>, SqlxLedgerError> {
        let recv = self.all.resubscribe();
        if self.closed.load(Ordering::SeqCst) {
            return Err(SqlxLedgerError::EventSubscriberClosed);
        }
        Ok(recv)
    }

    pub async fn account_balance(
        &self,
        journal_id: JournalId,
        account_id: AccountId,
    ) -> Result<broadcast::Receiver<SqlxLedgerEvent>, SqlxLedgerError> {
        let mut listeners = self.balance_receivers.write().await;
        let mut ret = None;
        let sender = listeners
            .entry((journal_id, account_id))
            .or_insert_with(|| {
                let (sender, recv) = broadcast::channel(self.buffer);
                ret = Some(recv);
                sender
            });
        let ret = ret.unwrap_or_else(|| sender.subscribe());
        if self.closed.load(Ordering::SeqCst) {
            listeners.remove(&(journal_id, account_id));
            return Err(SqlxLedgerError::EventSubscriberClosed);
        }
        Ok(ret)
    }

    pub async fn journal(
        &self,
        journal_id: JournalId,
    ) -> Result<broadcast::Receiver<SqlxLedgerEvent>, SqlxLedgerError> {
        let mut listeners = self.journal_receivers.write().await;
        let mut ret = None;
        let sender = listeners.entry(journal_id).or_insert_with(|| {
            let (sender, recv) = broadcast::channel(self.buffer);
            ret = Some(recv);
            sender
        });
        let ret = ret.unwrap_or_else(|| sender.subscribe());
        if self.closed.load(Ordering::SeqCst) {
            listeners.remove(&journal_id);
            return Err(SqlxLedgerError::EventSubscriberClosed);
        }
        Ok(ret)
    }
}

#[derive(
    sqlx::Type, Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Hash, Copy,
)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct SqlxLedgerEventId(i64);
impl SqlxLedgerEventId {
    pub const BEGIN: Self = Self(0);
}

/// Representation of a ledger event.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "EventRaw")]
pub struct SqlxLedgerEvent {
    pub id: SqlxLedgerEventId,
    pub data: SqlxLedgerEventData,
    pub r#type: SqlxLedgerEventType,
    pub recorded_at: DateTime<Utc>,
    #[cfg(feature = "otel")]
    pub otel_context: opentelemetry::Context,
}

impl SqlxLedgerEvent {
    #[cfg(feature = "otel")]
    fn record_otel_context(&mut self) {
        self.otel_context = tracing::Span::current().context();
    }

    #[cfg(not(feature = "otel"))]
    fn record_otel_context(&mut self) {}
}

impl SqlxLedgerEvent {
    pub fn journal_id(&self) -> JournalId {
        match &self.data {
            SqlxLedgerEventData::BalanceUpdated(b) => b.journal_id,
            SqlxLedgerEventData::TransactionCreated(t) => t.journal_id,
            SqlxLedgerEventData::TransactionUpdated(t) => t.journal_id,
        }
    }

    pub fn account_id(&self) -> Option<AccountId> {
        match &self.data {
            SqlxLedgerEventData::BalanceUpdated(b) => Some(b.account_id),
            _ => None,
        }
    }
}

/// Represents the different kinds of data that can be included in an `SqlxLedgerEvent` event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum SqlxLedgerEventData {
    BalanceUpdated(BalanceDetails),
    TransactionCreated(Transaction),
    TransactionUpdated(Transaction),
}

/// Defines possible event types for `SqlxLedgerEvent`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlxLedgerEventType {
    BalanceUpdated,
    TransactionCreated,
    TransactionUpdated,
}

pub(crate) async fn subscribe(
    pool: PgPool,
    closed: Arc<AtomicBool>,
    buffer: usize,
    after_id: Option<SqlxLedgerEventId>,
) -> Result<broadcast::Receiver<SqlxLedgerEvent>, SqlxLedgerError> {
    let mut listener = PgListener::connect_with(&pool).await?;
    listener.listen("sqlx_ledger_events").await?;
    let (snd, recv) = broadcast::channel(buffer);
    let mut reload = after_id.is_some();
    task::spawn(async move {
        let mut num_errors = 0;
        let mut last_id = after_id.unwrap_or(SqlxLedgerEventId(0));
        loop {
            if reload {
                match sqlx::query!(
                    r#"SELECT json_build_object(
                      'id', id,
                      'type', type,
                      'data', data,
                      'recorded_at', recorded_at
                    ) AS "payload!" FROM sqlx_ledger_events WHERE id > $1 ORDER BY id"#,
                    last_id.0
                )
                .fetch_all(&pool)
                .await
                {
                    Ok(rows) => {
                        num_errors = 0;
                        for row in rows {
                            let event: Result<SqlxLedgerEvent, _> =
                                serde_json::from_value(row.payload);
                            if sqlx_ledger_notification_received(event, &snd, &mut last_id, true)
                                .is_err()
                            {
                                closed.store(true, Ordering::SeqCst);
                                break;
                            }
                        }
                    }
                    Err(e) if num_errors == 0 => {
                        tracing::error!("Error fetching events: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(1 << num_errors)).await;
                        num_errors += 1;
                        continue;
                    }
                    _ => {
                        num_errors = 0;
                        continue;
                    }
                }
            }
            if closed.load(Ordering::Relaxed) {
                break;
            }
            while let Ok(notification) = listener.recv().await {
                let event: Result<SqlxLedgerEvent, _> =
                    serde_json::from_str(notification.payload());
                if let Err(e) = &event {
                    if e.to_string().contains("data field missing") {
                        reload = true;
                        break;
                    }
                }
                match sqlx_ledger_notification_received(event, &snd, &mut last_id, !reload) {
                    Ok(false) => break,
                    Ok(_) => num_errors = 0,
                    Err(_) => {
                        closed.store(true, Ordering::SeqCst);
                    }
                }
                reload = true;
            }
        }
    });
    Ok(recv)
}

#[instrument(name = "sqlx_ledger.notification_received", skip(sender), err)]
fn sqlx_ledger_notification_received(
    event: Result<SqlxLedgerEvent, serde_json::Error>,
    sender: &broadcast::Sender<SqlxLedgerEvent>,
    last_id: &mut SqlxLedgerEventId,
    ignore_gap: bool,
) -> Result<bool, SqlxLedgerError> {
    let mut event = event?;
    event.record_otel_context();
    let id = event.id;
    if id <= *last_id {
        return Ok(true);
    }
    if !ignore_gap && last_id.0 + 1 != id.0 {
        return Ok(false);
    }
    sender.send(event)?;
    *last_id = id;
    Ok(true)
}

#[derive(Deserialize)]
struct EventRaw {
    id: SqlxLedgerEventId,
    #[serde(default)]
    data: Option<serde_json::Value>,
    r#type: SqlxLedgerEventType,
    recorded_at: DateTime<Utc>,
}

impl TryFrom<EventRaw> for SqlxLedgerEvent {
    type Error = serde_json::Error;

    fn try_from(value: EventRaw) -> Result<Self, Self::Error> {
        let data_value = value
            .data
            .ok_or_else(|| serde_json::Error::custom("data field missing"))?;

        let data = match value.r#type {
            SqlxLedgerEventType::BalanceUpdated => {
                SqlxLedgerEventData::BalanceUpdated(serde_json::from_value(data_value)?)
            }
            SqlxLedgerEventType::TransactionCreated => {
                SqlxLedgerEventData::TransactionCreated(serde_json::from_value(data_value)?)
            }
            SqlxLedgerEventType::TransactionUpdated => {
                SqlxLedgerEventData::TransactionUpdated(serde_json::from_value(data_value)?)
            }
        };

        Ok(SqlxLedgerEvent {
            id: value.id,
            data,
            r#type: value.r#type,
            recorded_at: value.recorded_at,
            #[cfg(feature = "otel")]
            otel_context: tracing::Span::current().context(),
        })
    }
}
