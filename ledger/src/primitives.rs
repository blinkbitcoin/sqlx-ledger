use crate::error::*;
use rusty_money::{crypto, iso};

use cel_interpreter::{CelResult, CelValue};
use serde::{Deserialize, Serialize};

crate::entity_id! { AccountId }
crate::entity_id! { JournalId }
crate::entity_id! { TransactionId }
crate::entity_id! { EntryId }
crate::entity_id! { TxTemplateId }
crate::entity_id! { CorrelationId }

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "Layer", rename_all = "snake_case")]
pub enum Layer {
    Settled,
    Pending,
    Encumbered,
}

impl<'a> TryFrom<CelResult<'a>> for Layer {
    type Error = SqlxLedgerError;

    fn try_from(CelResult { val, .. }: CelResult) -> Result<Self, Self::Error> {
        match val {
            CelValue::String(v) if v.as_ref() == "SETTLED" => Ok(Layer::Settled),
            CelValue::String(v) if v.as_ref() == "PENDING" => Ok(Layer::Pending),
            CelValue::String(v) if v.as_ref() == "ENCUMBERED" => Ok(Layer::Encumbered),
            v => Err(SqlxLedgerError::UnknownLayer(format!("{v:?}"))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, sqlx::Type)]
#[sqlx(type_name = "DebitOrCredit", rename_all = "snake_case")]
pub enum DebitOrCredit {
    Debit,
    #[default]
    Credit,
}

impl<'a> TryFrom<CelResult<'a>> for DebitOrCredit {
    type Error = SqlxLedgerError;

    fn try_from(CelResult { val, .. }: CelResult) -> Result<Self, Self::Error> {
        match val {
            CelValue::String(v) if v.as_ref() == "DEBIT" => Ok(DebitOrCredit::Debit),
            CelValue::String(v) if v.as_ref() == "CREDIT" => Ok(DebitOrCredit::Credit),
            v => Err(SqlxLedgerError::UnknownDebitOrCredit(format!("{v:?}"))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, sqlx::Type)]
#[sqlx(type_name = "Status", rename_all = "snake_case")]
pub enum Status {
    #[default]
    Active,
}

#[derive(Debug, Clone, Copy, Eq, Serialize, Deserialize)]
#[serde(try_from = "String")]
#[serde(into = "&str")]
pub enum Currency {
    Iso(&'static iso::Currency),
    Crypto(&'static crypto::Currency),
}

impl Currency {
    pub fn code(&self) -> &'static str {
        match self {
            Currency::Iso(c) => c.iso_alpha_code,
            Currency::Crypto(c) => c.code,
        }
    }
}

impl std::fmt::Display for Currency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.code())
    }
}

impl std::hash::Hash for Currency {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.code().hash(state);
    }
}

impl PartialEq for Currency {
    fn eq(&self, other: &Self) -> bool {
        self.code() == other.code()
    }
}

impl std::str::FromStr for Currency {
    type Err = SqlxLedgerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match iso::find(s) {
            Some(c) => Ok(Currency::Iso(c)),
            _ => match crypto::find(s) {
                Some(c) => Ok(Currency::Crypto(c)),
                _ => Err(SqlxLedgerError::UnknownCurrency(s.to_string())),
            },
        }
    }
}

impl TryFrom<String> for Currency {
    type Error = SqlxLedgerError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl From<Currency> for &'static str {
    fn from(c: Currency) -> Self {
        c.code()
    }
}

impl<'a> TryFrom<CelResult<'a>> for Currency {
    type Error = SqlxLedgerError;

    fn try_from(CelResult { val, .. }: CelResult) -> Result<Self, Self::Error> {
        match val {
            CelValue::String(v) => v.as_ref().parse(),
            v => Err(SqlxLedgerError::UnknownCurrency(format!("{v:?}"))),
        }
    }
}

impl<'r, DB: sqlx::Database> sqlx::Decode<'r, DB> for Currency
where
    &'r str: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: <DB as sqlx::Database>::ValueRef<'r>,
    ) -> Result<Currency, Box<dyn std::error::Error + 'static + Send + Sync>> {
        let value = <&str as sqlx::Decode<DB>>::decode(value)?;

        Ok(value.parse().map_err(Box::new)?)
    }
}
