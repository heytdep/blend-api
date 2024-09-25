use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use zephyr_sdk::{prelude::*, DatabaseDerive, EnvClient};

#[derive(DatabaseDerive, Clone)]
#[with_name("clateral")]
#[external("35")]
pub struct Collateral {
    pub timestamp: u64,
    pub ledger: u32,
    pub pool: String,
    pub asset: String,
    pub clateral: i128,
    pub delta: i128,
    pub source: String,
}

#[derive(DatabaseDerive, Clone)]
#[with_name("borrowed")]
#[external("35")]
pub struct Borrowed {
    pub timestamp: u64,
    pub ledger: u32,
    pub pool: String,
    pub asset: String,
    pub borrowed: i128,
    pub delta: i128,
    pub source: String,
}

#[derive(Deserialize)]
pub struct Request {
    pub addresses: Vec<String>,
}

#[derive(Serialize)]
pub enum ActionKind {
    Borrow,
    Collateral,
}

#[derive(Serialize)]
pub struct Action {
    pub kind: ActionKind,
    pub timestamp: u64,
    pub ledger: u32,
    pub pool: String,
    pub asset: String,
    pub tvl: i64,
    pub delta: i64,
    pub source: String,
}

#[no_mangle]
pub extern "C" fn api() {
    let env = EnvClient::empty();
    let request: Request = env.read_request_body();

    env.log().debug("Got request", None);

    let searched: HashMap<String, Vec<Action>> = request
        .addresses
        .into_iter()
        .map(|address| {
            env.log().debug("Getting collaterals for address", None);
            let collaterals: Vec<Collateral> = env
                .read_filter()
                .column_equal_to("source", address.clone())
                .read()
                .unwrap();
            env.log().debug("Getting borrows for address", None);
            let borrows: Vec<Borrowed> = env
                .read_filter()
                .column_equal_to("source", address.clone())
                .read()
                .unwrap();

            env.log().debug("got all data from indexes", None);

            let mut joined = Vec::new();

            joined.extend(collaterals.into_iter().map(|collateral| Action {
                kind: ActionKind::Collateral,
                timestamp: collateral.timestamp,
                ledger: collateral.ledger,
                pool: collateral.pool,
                asset: collateral.asset,
                tvl: collateral.clateral as i64,
                delta: collateral.delta as i64,
                source: collateral.source,
            }));

            joined.extend(borrows.into_iter().map(|borrow| Action {
                kind: ActionKind::Borrow,
                timestamp: borrow.timestamp,
                ledger: borrow.ledger,
                pool: borrow.pool,
                asset: borrow.asset,
                tvl: borrow.borrowed as i64,
                delta: borrow.delta as i64,
                source: borrow.source,
            }));

            (address.clone(), joined)
        })
        .collect();

    env.log().debug("Returning response", None);
    env.conclude(&searched);
}
