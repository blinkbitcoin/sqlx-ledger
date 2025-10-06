//! # sqlx-ledger
//!
//! This crate builds on the sqlx crate to provide a set of primitives for
//! implementing an SQL-compatible double-entry accounting system. This system
//! is engineered specifically for dealing with money and building financial
//! products.
//!
//! ## Quick Start
//!
//! Add and execute the migrations from the migrations directory before usage.
//! ```bash,ignore
//! cp ./migrations/* <path/to/your/projects/migrations>
//! # in your project
//! cargo sqlx migrate
//! ```
//!
//! Here is how to initialize a ledger create a primitive template and post a transaction.
//! This is a toy example that brings all pieces together end-to-end.
//! Not recommended for real use.
//! ```rust
//! use uuid::uuid;
//! use rust_decimal::Decimal;
//! use sqlx_ledger::{*, journal::*, account::*, tx_template::*};
//!
//! async fn init_ledger(journal_id: JournalId) -> SqlxLedger {
//!     let pg_con =
//!         std::env::var("PG_CON").unwrap_or(format!("postgres://user:password@localhost:5432/pg"));
//!     let pool = sqlx::PgPool::connect(&pg_con).await.unwrap();
//!     let ledger = SqlxLedger::new(&pool);
//!
//!     // Initialize the journal - all entities are constructed via builders
//!     let new_journal = NewJournal::builder()
//!         .id(journal_id)
//!         .description("General ledger".to_string())
//!         .name("Ledger")
//!         .build()
//!         .expect("Couldn't build NewJournal");
//!
//!     let _ = ledger.journals().create(new_journal).await;
//!
//!     // Initialize an income omnibus account
//!     let main_account_id = uuid!("00000000-0000-0000-0000-000000000001");
//!     let new_account = NewAccount::builder()
//!         .id(main_account_id)
//!         .name("Income")
//!         .code("Income")
//!         .build()
//!         .unwrap();
//!
//!     let _ = ledger.accounts().create(new_account).await;
//!
//!     // Create the trivial 'income' template
//!     //
//!     // Here are the 'parameters' that the template will require as inputs.
//!     let params = vec![
//!         ParamDefinition::builder()
//!             .name("sender_account_id")
//!             .r#type(ParamDataType::UUID)
//!             .build()
//!             .unwrap(),
//!         ParamDefinition::builder()
//!             .name("units")
//!             .r#type(ParamDataType::DECIMAL)
//!             .build()
//!             .unwrap()
//!     ];
//!
//!     // The templates for the Entries that will be created as part of the transaction.
//!     let entries = vec![
//!         EntryInput::builder()
//!             .entry_type("'INCOME_DR'")
//!             // Reference the input parameters via CEL syntax
//!             .account_id("params.sender_account_id")
//!             .layer("SETTLED")
//!             .direction("DEBIT")
//!             .units("params.units")
//!             .currency("'BTC'")
//!             .build()
//!             .unwrap(),
//!         EntryInput::builder()
//!             .entry_type("'INCOME_CR'")
//!             .account_id(format!("uuid('{main_account_id}')"))
//!             .layer("SETTLED")
//!             .direction("CREDIT")
//!             .units("params.units")
//!             .currency("'BTC'")
//!             .build()
//!             .unwrap(),
//!     ];
//!     let tx_code = "GENERAL_INCOME";
//!     let new_template = NewTxTemplate::builder()
//!         .id(uuid::Uuid::new_v4())
//!         .code(tx_code)
//!         .params(params)
//!         .tx_input(
//!             // Template for the Transaction metadata.
//!             TxInput::builder()
//!                 .effective("date()")
//!                 .journal_id(format!("uuid('{journal_id}')"))
//!                 .build()
//!                 .unwrap(),
//!         )
//!         .entries(entries)
//!         .build()
//!         .unwrap();
//!
//!     let _ = ledger.tx_templates().create(new_template).await;
//!
//!     ledger
//! }
//!
//! tokio_test::block_on(async {
//!     let journal_id = JournalId::from(uuid!("00000000-0000-0000-0000-000000000001"));
//!     let ledger = init_ledger(journal_id).await;
//!
//!     // The account that is sending to the general income account
//!     let sender_account_id = AccountId::new();
//!     let sender_account = NewAccount::builder()
//!         .id(sender_account_id)
//!         .name(format!("Sender-{sender_account_id}"))
//!         .code(format!("Sender-{sender_account_id}"))
//!         .build()
//!         .unwrap();
//!     ledger.accounts().create(sender_account).await.unwrap();
//!
//!     // Prepare the input parameters that the template requires
//!     let mut params = TxParams::new();
//!     params.insert("sender_account_id", sender_account_id);
//!     params.insert("units", Decimal::ONE);
//!
//!     // Create the transaction via the template
//!     ledger
//!         .post_transaction(TransactionId::new(), "GENERAL_INCOME", Some(params))
//!         .await
//!         .unwrap();
//!
//!     // Check the resulting balance
//!     let account_balance = ledger
//!         .balances()
//!         .find(journal_id, sender_account_id, "BTC".parse().unwrap())
//!         .await
//!         .unwrap();
//!
//!     assert_eq!(account_balance.unwrap().settled(), Decimal::NEGATIVE_ONE);
//! });
//! ```

#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![cfg_attr(feature = "fail-on-warnings", deny(clippy::all))]
#![allow(clippy::result_large_err)]
#![allow(clippy::large_enum_variant)]

pub mod account;
pub mod balance;
pub mod entry;
pub mod event;
pub mod journal;
pub mod transaction;
pub mod tx_template;

mod error;
mod ledger;
mod macros;
mod primitives;

pub use error::*;
pub use ledger::*;
pub use primitives::*;
