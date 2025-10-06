use sqlx::{PgPool, Postgres, QueryBuilder, Row, Transaction};
use tracing::instrument;
use uuid::Uuid;

use std::{collections::HashMap, str::FromStr};

use super::entity::*;
use crate::{error::*, primitives::*};

/// Repository for working with `AccountBalance` entities.
#[derive(Debug, Clone)]
pub struct Balances {
    pool: PgPool,
}

impl Balances {
    pub fn new(pool: &PgPool) -> Self {
        Self { pool: pool.clone() }
    }

    #[instrument(name = "sqlx_ledger.balances.find", skip(self))]
    pub async fn find(
        &self,
        journal_id: JournalId,
        account_id: AccountId,
        currency: Currency,
    ) -> Result<Option<AccountBalance>, SqlxLedgerError> {
        let record = sqlx::query!(
            r#"SELECT
              a.normal_balance_type as "normal_balance_type: DebitOrCredit", b.journal_id, b.account_id, entry_id, b.currency,
              settled_dr_balance, settled_cr_balance, settled_entry_id, settled_modified_at,
              pending_dr_balance, pending_cr_balance, pending_entry_id, pending_modified_at,
              encumbered_dr_balance, encumbered_cr_balance, encumbered_entry_id, encumbered_modified_at,
              c.version, modified_at, created_at
                FROM sqlx_ledger_balances b JOIN (
                  SELECT * FROM sqlx_ledger_current_balances WHERE journal_id = $1 AND account_id = $2 AND currency = $3 ) c
                ON b.journal_id = c.journal_id AND b.account_id = c.account_id AND b.currency = c.currency AND b.version = c.version
                JOIN ( SELECT id, normal_balance_type FROM sqlx_ledger_accounts WHERE id = $2 LIMIT 1 ) a
                  ON a.id = b.account_id"#,
            journal_id as JournalId,
            account_id as AccountId,
            currency.code()
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(record.map(|record| AccountBalance {
            balance_type: record.normal_balance_type,
            details: BalanceDetails {
                journal_id,
                account_id,
                entry_id: EntryId::from(record.entry_id),
                currency,
                settled_dr_balance: record.settled_dr_balance,
                settled_cr_balance: record.settled_cr_balance,
                settled_entry_id: EntryId::from(record.settled_entry_id),
                settled_modified_at: record.settled_modified_at,
                pending_dr_balance: record.pending_dr_balance,
                pending_cr_balance: record.pending_cr_balance,
                pending_entry_id: EntryId::from(record.pending_entry_id),
                pending_modified_at: record.pending_modified_at,
                encumbered_dr_balance: record.encumbered_dr_balance,
                encumbered_cr_balance: record.encumbered_cr_balance,
                encumbered_entry_id: EntryId::from(record.encumbered_entry_id),
                encumbered_modified_at: record.encumbered_modified_at,
                version: record.version,
                modified_at: record.modified_at,
                created_at: record.created_at,
            },
        }))
    }

    #[instrument(name = "sqlx_ledger.balances.find_all", skip(self, accounts))]
    pub async fn find_all(
        &self,
        journal_id: JournalId,
        accounts: impl IntoIterator<Item = AccountId>,
    ) -> Result<HashMap<AccountId, HashMap<Currency, AccountBalance>>, SqlxLedgerError> {
        let account_ids: Vec<Uuid> = accounts.into_iter().map(Uuid::from).collect();
        let rows = sqlx::query!(
            r#"SELECT
              a.normal_balance_type as "normal_balance_type: DebitOrCredit", b.journal_id, b.account_id, entry_id, b.currency,
              settled_dr_balance, settled_cr_balance, settled_entry_id, settled_modified_at,
              pending_dr_balance, pending_cr_balance, pending_entry_id, pending_modified_at,
              encumbered_dr_balance, encumbered_cr_balance, encumbered_entry_id, encumbered_modified_at,
              c.version, modified_at, created_at
                FROM sqlx_ledger_balances b JOIN (
                  SELECT * FROM sqlx_ledger_current_balances WHERE journal_id = $1 AND account_id = ANY($2)) c
                ON b.journal_id = c.journal_id AND b.account_id = c.account_id AND b.currency = c.currency AND b.version = c.version
                JOIN ( SELECT DISTINCT(id), normal_balance_type FROM sqlx_ledger_accounts WHERE id = ANY($2)) a
                  ON a.id = b.account_id"#,
            journal_id as JournalId,
            &account_ids[..]
        )
        .fetch_all(&self.pool)
        .await?;
        let mut ret = HashMap::new();
        for row in rows {
            ret.entry(AccountId::from(row.account_id))
                .or_insert_with(HashMap::new)
                .insert(
                    row.currency.parse().expect("Currency code is invalid"),
                    AccountBalance {
                        balance_type: row.normal_balance_type,
                        details: BalanceDetails {
                            journal_id,
                            account_id: AccountId::from(row.account_id),
                            entry_id: EntryId::from(row.entry_id),
                            currency: row.currency.parse().unwrap(),
                            settled_dr_balance: row.settled_dr_balance,
                            settled_cr_balance: row.settled_cr_balance,
                            settled_entry_id: EntryId::from(row.settled_entry_id),
                            settled_modified_at: row.settled_modified_at,
                            pending_dr_balance: row.pending_dr_balance,
                            pending_cr_balance: row.pending_cr_balance,
                            pending_entry_id: EntryId::from(row.pending_entry_id),
                            pending_modified_at: row.pending_modified_at,
                            encumbered_dr_balance: row.encumbered_dr_balance,
                            encumbered_cr_balance: row.encumbered_cr_balance,
                            encumbered_entry_id: EntryId::from(row.encumbered_entry_id),
                            encumbered_modified_at: row.encumbered_modified_at,
                            version: row.version,
                            modified_at: row.modified_at,
                            created_at: row.created_at,
                        },
                    },
                );
        }
        Ok(ret)
    }

    #[instrument(
        level = "trace",
        name = "sqlx_ledger.balances.find_for_update",
        skip(self, tx)
    )]
    pub(crate) async fn find_for_update<'a>(
        &self,
        journal_id: JournalId,
        ids: Vec<(AccountId, &Currency)>,
        tx: &mut Transaction<'a, Postgres>,
    ) -> Result<HashMap<(AccountId, Currency), BalanceDetails>, SqlxLedgerError> {
        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            r#"SELECT
              b.journal_id, b.account_id, entry_id, b.currency,
              settled_dr_balance, settled_cr_balance, settled_entry_id, settled_modified_at,
              pending_dr_balance, pending_cr_balance, pending_entry_id, pending_modified_at,
              encumbered_dr_balance, encumbered_cr_balance, encumbered_entry_id, encumbered_modified_at,
              c.version, modified_at, created_at
                FROM sqlx_ledger_balances b JOIN (
                    SELECT * FROM sqlx_ledger_current_balances WHERE journal_id = "#,
        );
        query_builder.push_bind(journal_id);
        query_builder.push(r#" AND (account_id, currency) IN"#);
        query_builder.push_tuples(ids, |mut builder, (id, currency)| {
            builder.push_bind(id);
            builder.push_bind(currency.code());
        });
        query_builder.push(
            r#"FOR UPDATE ) c ON
                b.journal_id = c.journal_id AND b.account_id = c.account_id AND b.currency = c.currency AND b.version = c.version"#,
        );

        let query = query_builder.build();
        let records = query.fetch_all(&mut **tx).await?;
        let mut ret = HashMap::new();
        for r in records {
            let account_id = AccountId::from(r.get::<Uuid, _>("account_id"));
            let currency =
                Currency::from_str(r.get("currency")).expect("currency code should be valid");
            ret.insert(
                (account_id, currency),
                BalanceDetails {
                    account_id,
                    journal_id: JournalId::from(r.get::<Uuid, _>("journal_id")),
                    entry_id: EntryId::from(r.get::<Uuid, _>("entry_id")),
                    currency: r.get::<&str, _>("currency").parse()?,
                    settled_dr_balance: r.get("settled_dr_balance"),
                    settled_cr_balance: r.get("settled_cr_balance"),
                    settled_entry_id: EntryId::from(r.get::<Uuid, _>("settled_entry_id")),
                    settled_modified_at: r.get("settled_modified_at"),
                    pending_dr_balance: r.get("pending_dr_balance"),
                    pending_cr_balance: r.get("pending_cr_balance"),
                    pending_entry_id: EntryId::from(r.get::<Uuid, _>("pending_entry_id")),
                    pending_modified_at: r.get("pending_modified_at"),
                    encumbered_dr_balance: r.get("encumbered_dr_balance"),
                    encumbered_cr_balance: r.get("encumbered_cr_balance"),
                    encumbered_entry_id: EntryId::from(r.get::<Uuid, _>("encumbered_entry_id")),
                    encumbered_modified_at: r.get("encumbered_modified_at"),
                    version: r.get("version"),
                    modified_at: r.get("modified_at"),
                    created_at: r.get("created_at"),
                },
            );
        }
        Ok(ret)
    }

    #[instrument(
        level = "trace",
        name = "sqlx_ledger.balances.update_balances",
        skip(self, tx)
    )]
    pub(crate) async fn update_balances<'a>(
        &self,
        journal_id: JournalId,
        new_balances: Vec<BalanceDetails>,
        tx: &mut Transaction<'a, Postgres>,
    ) -> Result<(), SqlxLedgerError> {
        let mut latest_versions = HashMap::new();
        let mut previous_versions = HashMap::new();
        for BalanceDetails {
            account_id,
            version,
            currency,
            ..
        } in new_balances.iter()
        {
            latest_versions.insert((account_id, currency), version);
            if previous_versions.contains_key(&(account_id, currency)) {
                continue;
            }
            previous_versions.insert((account_id, currency), version - 1);
        }
        let expected_accounts_effected = latest_versions.len();
        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            r#"INSERT INTO sqlx_ledger_current_balances
                  (journal_id, account_id, currency, version)"#,
        );
        let new_accounts: Vec<_> = previous_versions.iter().filter(|(_, v)| **v == 0).collect();
        let any_new = !new_accounts.is_empty();

        if any_new {
            query_builder.push_values(
                new_accounts,
                |mut builder, ((account_id, currency), version)| {
                    builder.push_bind(journal_id);
                    builder.push_bind(**account_id);
                    builder.push_bind(currency.code());
                    builder.push_bind(version);
                },
            );
            query_builder.build().execute(&mut **tx).await?;
        }
        let mut query_builder: QueryBuilder<Postgres> =
            QueryBuilder::new(r#"UPDATE sqlx_ledger_current_balances SET version = CASE"#);
        let mut bind_numbers = HashMap::new();
        let mut next_bind_number = 1;
        for ((account_id, currency), version) in latest_versions {
            bind_numbers.insert((account_id, currency), next_bind_number);
            next_bind_number += 3;
            query_builder.push(" WHEN account_id = ");
            query_builder.push_bind(*account_id);
            query_builder.push(" AND currency = ");
            query_builder.push_bind(currency.code());
            query_builder.push(" THEN ");
            query_builder.push_bind(version);
        }
        query_builder.push(" END WHERE journal_id = ");
        query_builder.push_bind(journal_id);
        query_builder.push(" AND (account_id, currency, version) IN");
        query_builder.push_tuples(
            previous_versions,
            |mut builder, ((account_id, currency), version)| {
                let n = bind_numbers.remove(&(account_id, currency)).unwrap();
                builder.push(format!("${}, ${}", n, n + 1));
                builder.push_bind(version);
            },
        );
        let result = query_builder.build().execute(&mut **tx).await?;
        if result.rows_affected() != (expected_accounts_effected as u64) {
            return Err(SqlxLedgerError::OptimisticLockingError);
        }

        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            r#"INSERT INTO sqlx_ledger_balances (
                 journal_id, account_id, entry_id, currency,
                 settled_dr_balance, settled_cr_balance, settled_entry_id, settled_modified_at,
                 pending_dr_balance, pending_cr_balance, pending_entry_id, pending_modified_at,
                 encumbered_dr_balance, encumbered_cr_balance, encumbered_entry_id, encumbered_modified_at,
                 version, modified_at, created_at)
            "#,
        );
        query_builder.push_values(new_balances, |mut builder, b| {
            builder.push_bind(b.journal_id);
            builder.push_bind(b.account_id);
            builder.push_bind(b.entry_id);
            builder.push_bind(b.currency.code());
            builder.push_bind(b.settled_dr_balance);
            builder.push_bind(b.settled_cr_balance);
            builder.push_bind(b.settled_entry_id);
            builder.push_bind(b.settled_modified_at);
            builder.push_bind(b.pending_dr_balance);
            builder.push_bind(b.pending_cr_balance);
            builder.push_bind(b.pending_entry_id);
            builder.push_bind(b.pending_modified_at);
            builder.push_bind(b.encumbered_dr_balance);
            builder.push_bind(b.encumbered_cr_balance);
            builder.push_bind(b.encumbered_entry_id);
            builder.push_bind(b.encumbered_modified_at);
            builder.push_bind(b.version);
            builder.push_bind(b.modified_at);
            builder.push_bind(b.created_at);
        });
        query_builder.build().execute(&mut **tx).await?;
        Ok(())
    }
}
