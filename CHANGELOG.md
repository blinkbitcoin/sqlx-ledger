# [sqlx-ledger release v0.11.11](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.11.11)


### Bug Fixes

- Update sqlx cache queries (#109)

# [sqlx-ledger release v0.11.10](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.11.10)


### Bug Fixes

- Refactor events subscription (#108)

# [sqlx-ledger release v0.11.9](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.11.9)


### Bug Fixes

- Add unlisten after events loop (#105)

# [sqlx-ledger release v0.11.8](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.11.8)


### Bug Fixes

- Pg_notify payload limit (#101)

### Miscellaneous Tasks

- Update rust version (#103)

# [sqlx-ledger release v0.11.7](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.11.7)



# [sqlx-ledger release v0.11.3](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.11.3)


### Bug Fixes

- Var naming (#65)

### Miscellaneous Tasks

- Bump otel tracing, derive-builder and cached (#73)

# [sqlx-ledger release v0.11.2](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.11.2)


### Miscellaneous Tasks

- Update opentelemetry and tracing-opentelemetry (#64)

# [sqlx-ledger release v0.11.1](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.11.1)


### Bug Fixes

- Current_balance error when executing template (#58)
- Check-code error on concourse (#57)
- Account can have multiple currencies (#56)

### Miscellaneous Tasks

- Add flake
- Update tracing-opentelemetry requirement from 0.20 to 0.21 (#53)

# [sqlx-ledger release v0.11.0](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.11.0)


### Bug Fixes

- Formatting in Cargo.toml

### Miscellaneous Tasks

- Add async feature for cached crate
- Update cached requirement from 0.44 to 0.46

# [sqlx-ledger release v0.10.0](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.10.0)


### Miscellaneous Tasks

- [**breaking**] Update opentelemetry and tracing-opentelemetry (#47)

# [sqlx-ledger release v0.9.0](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.9.0)


### Miscellaneous Tasks

- [**breaking**] Update sqlx to 0.7.1 (#45)

# [sqlx-ledger release v0.8.5](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.8.5)


### Features

- Accept IntoIterator for tx_ids

# [sqlx-ledger release v0.8.4](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.8.4)


### Features

- Add transactions.list_by_template_id

# [sqlx-ledger release v0.8.3](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.8.3)


### Miscellaneous Tasks

- Bump dependencies

# [sqlx-ledger release v0.8.2](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.8.2)


### Bug Fixes

- Typo in error.rs

### Miscellaneous Tasks

- Bump lalrpop
- Update cached requirement from 0.43.0 to 0.44.0

# [sqlx-ledger release v0.8.1](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.8.1)


### Miscellaneous Tasks

- Update opentelemetry and tracing-opentelemetry (#35)

# [sqlx-ledger release v0.8.0](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.8.0)


### Bug Fixes

- Line up ParamDataType with new Decimal
- [**breaking**] Correct numeric types

### Refactor

- Array->List
- Remove ledger specific literals from cel-interpreter

# [sqlx-ledger release v0.7.7](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.7.7)


### Miscellaneous Tasks

- Bump lalrpop

# [sqlx-ledger release v0.7.6](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.7.6)


### Bug Fixes

- Add CelArray type

### Miscellaneous Tasks

- Missing Default for CelArray

# [sqlx-ledger release v0.7.5](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.7.5)


### Miscellaneous Tasks

- Impl Type on entity ids

# [sqlx-ledger release v0.7.4](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.7.4)


### Bug Fixes

- Pass reload to sqlx_ledger_notfification_received

# [sqlx-ledger release v0.7.3](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.7.3)


### Bug Fixes

- Make BEGIN marker pub

# [sqlx-ledger release v0.7.2](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.7.2)


### Bug Fixes

- Only reload if after_id is set

# [sqlx-ledger release v0.7.1](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.7.1)


### Miscellaneous Tasks

- Type safe SqlxLedgerEventId

# [sqlx-ledger release v0.7.0](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.7.0)


### Miscellaneous Tasks

- Add otel feature

# [sqlx-ledger release v0.6.1](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.6.1)


### Miscellaneous Tasks

- Rename idx -> id

# [sqlx-ledger release v0.6.0](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.6.0)


### Features

- [**breaking**] EventSubscriberOpts to configure event listening

### Miscellaneous Tasks

- Try to use working lalrpop
- Attempt to pin lalrpop to a ref
- Temporarily pin lalrpop

# [sqlx-ledger release v0.5.12](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.5.12)


### Bug Fixes

- Spelling

### Miscellaneous Tasks

- Bump cached crate

# [sqlx-ledger release v0.5.11](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.5.11)


### Miscellaneous Tasks

- Include dec in builtins

# [sqlx-ledger release v0.5.10](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.5.10)


### Bug Fixes

- Error output in relation

### Miscellaneous Tasks

- Implement interpretation of some Relations

# [sqlx-ledger release v0.5.9](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.5.9)


### Miscellaneous Tasks

- Better bool handling

# [sqlx-ledger release v0.5.8](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.5.8)


### Features

- Transactions.list_by_ids

# [sqlx-ledger release v0.5.7](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.5.7)



# [sqlx-ledger release v0.5.5](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.5.5)


### Documentation

- Link to readme in ledger/Cargo.toml

# [sqlx-ledger release v0.5.4](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.5.4)


### Documentation

- Readme + small improvements

# [sqlx-ledger release v0.5.3](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.5.3)


### Documentation

- No-deps to cargo doc
- Add quick-start section
- Move module docs to repos
- 1st attempt at documenting tx_template module
- Adds documentation for 'transaction' module
- Adds documentation for 'journal' module
- Adds documentation for 'entry' module
- Adds documentation for 'balance' module
- Adds documentation for 'account' module
- Adds documentation for events module
- Adds documentation for sqlx-ledger crate

# [sqlx-ledger release v0.5.2](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.5.2)


### Features

- Add balances.find_all

### Miscellaneous Tasks

- Simplify as TxTemplateCore is now Send
- Less cloning within TxTemplateCore

# [sqlx-ledger release v0.5.1](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.5.1)


### Miscellaneous Tasks

- Add some trace spans
- Cache tx_templates.find_core
- Include current span in SqlxLedgerEvent

# [sqlx-ledger release v0.5.0](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.5.0)


### Bug Fixes

- Sqlx_ledger.notification_received tracing name

### Miscellaneous Tasks

- [**breaking**] Make ids mandatory in tx_template + account

### Testing

- Remove bitfinex from hedging test

# [sqlx-ledger release v0.4.0](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.4.0)


### Features

- [**breaking**] Better event interface

### Miscellaneous Tasks

- Derive Clone for EventSubscriber

# [sqlx-ledger release v0.3.0](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.3.0)


### Features

- [**breaking**] Post_transaction requires tx_id for idempotency

# [sqlx-ledger release v0.2.1](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.2.1)


### Features

- Use entry entity outside of crate (#11)

# [sqlx-ledger release v0.2.0](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.2.0)


### Features

- List entries by external id (#9)
- [**breaking**] Expose ledger.event_stream

### Miscellaneous Tasks

- Clippy
- Use DESC LIMIT 1 to get current balance
- Exclude CHANGELOG in typos

# [sqlx-ledger release v0.1.1](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.1.1)


### Miscellaneous Tasks

- Expose deserialized metadata on Transaction
- List_by_external_tx_ids

# [sqlx-ledger release v0.1.0](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.1.0)


### Bug Fixes

- Referenced dev versions
- Balance is unique accross journal_id
- Updating existing balances
- Null params should become None
- .gitignore
- Unique index on balances

### Features

- Add op to cel
- Balances e2e
- Journals
- Create account

### Miscellaneous Tasks

- Descriptions
- Fix release of worspace versions
- Update derive_builder requirement from 0.11.2 to 0.12.0
- Remove --locked when testing
- Typos
- Deps and check-code
- Expose balance.encumbered
- Clippy
- Report original expression in CelError
- Add ledger/sqlx-data.json
- Implement Multiply op
- Add settled to AccountBalance
- Add From<string> for CelValue
- Improve tracing
- Add a bunch of tracing
- Fix TxTemplateCore not being Send
- Improve metadata handling
- Expose AccountBalance
- Fix timestamp TZ
- Enable post_transaction in tx
- Small fixes
- Add From<Decimal> for CelValue
- Better error output
- Support DuplicateKey error
- Add find_by_code to Accounts
- Optional settes for tx_input
- Make SqlxLedger Clone
- Switch pg settings
- Update sqlx-data
- Add create_in_tx to account / journal
- Add id to journal/account builder
- Add update-account
- Remove (n) constraint on columns
- Rename tables with sqlx_ledger prefix
- Remove unused make commands
- Currency from CelValue
- Clippy
- OptimisticLockingError
- Return StagedEntries
- Persist entries
- Add EntryInput
- Validate params against defs
- E2e post-transaction kind of working
- Remove ledger/cel module
- Interpreter can lookup values
- Interpreter wip
- Cleanup grammar
- Workspace
- Prep TxTemplate.prep_tx
- Transaction wip
- Params is optional
- Tx_template pt 1
- Cel wip
- Some account scaffolding
- Initial commit

### Refactor

- External_id is String
- Expose BalanceDetails
- Accept impl Into<TxParams> in post_transaction
- &str for post_transaction tx_template_code
- More efficient balance selection
- Fix parser
- Rename perm -> core
- Remove current / history tables

# [sqlx-ledger release v0.0.5](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.0.5)


### Miscellaneous Tasks

- Descriptions

# [sqlx-ledger release v0.0.4](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.0.4)


### Bug Fixes

- Referenced dev versions

### Miscellaneous Tasks

- Fix release of worspace versions

# [sqlx-ledger release v0.0.3](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.0.3)



# [sqlx-ledger release v0.0.2](https://github.com/GaloyMoney/sqlx-ledger/releases/tag/v0.0.2)
