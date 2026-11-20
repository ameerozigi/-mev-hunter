//! MEV Hunter — Lightweight MEV bot simulation framework
//!
//! Monitors Ethereum mempool for arbitrage opportunities between Uniswap V2
//! and Uniswap V3 pools, calculating price discrepancies and potential profit.
//!
//! Usage:
//!   Set ALCHEMY_WS_URL environment variable to your Alchemy/Infura WebSocket URL
//!   cargo run

use futures_util::SinkExt;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::env;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolTx {
    pub hash: String,
    pub from: String,
    pub to: Option<String>,
    pub input: String,
    pub value: String,
    pub gas_price: Option<String>,
    pub gas: String,
}

#[derive(Debug, Clone)]
pub struct PoolState {
    pub reserve0: Decimal,
    pub reserve1: Decimal,
    pub fee_bps: u32,
    pub dex: DexType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DexType {
    UniswapV2,
    UniswapV3,
}

#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub token_in: String,
    pub token_out: String,
    pub dex_buy: DexType,
    pub dex_sell: DexType,
    pub amount_in: Decimal,
    pub amount_out: Decimal,
    pub profit: Decimal,
    pub profit_pct: Decimal,
}

/// Uniswap V2 price impact calculation
/// amount_out = (amount_in * reserve_out * 997) / (reserve_in * 1000 + amount_in * 997)
pub fn v2_price_impact(pool: &PoolState, amount_in: Decimal) -> Decimal {
    if pool.reserve0 == Decimal::ZERO || pool.reserve1 == Decimal::ZERO {
        return Decimal::ZERO;
    }
    let fee = Decimal::from(1000 - pool.fee_bps);
    let numerator = amount_in * pool.reserve1 * fee;
    let denominator = pool.reserve0 * Decimal::from(1000) + amount_in * fee;
    if denominator == Decimal::ZERO {
        return Decimal::ZERO;
    }
    numerator / denominator
}

/// Uniswap V3 price impact (simplified concentrated liquidity)
/// amount_out = (amount_in * reserve1 * (1 - fee)) / (reserve0 + amount_in * (1 - fee))
pub fn v3_price_impact(pool: &PoolState, amount_in: Decimal) -> Decimal {
    if pool.reserve0 == Decimal::ZERO || pool.reserve1 == Decimal::ZERO {
        return Decimal::ZERO;
    }
    let fee = dec!(1) - Decimal::from(pool.fee_bps) / dec!(10000);
    let numerator = amount_in * pool.reserve1 * fee;
    let denominator = pool.reserve0 + amount_in * fee;
    if denominator == Decimal::ZERO {
        return Decimal::ZERO;
    }
    numerator / denominator
}

/// Check arbitrage opportunity between two pools
pub fn check_arbitrage(
    pool_a: &PoolState,
    pool_b: &PoolState,
    token_in: &str,
    token_out: &str,
    amount_in: Decimal,
) -> Option<ArbitrageOpportunity> {
    let out_a = match pool_a.dex {
        DexType::UniswapV2 => v2_price_impact(pool_a, amount_in),
        DexType::UniswapV3 => v3_price_impact(pool_a, amount_in),
    };
    let out_b = match pool_b.dex {
        DexType::UniswapV2 => v2_price_impact(pool_b, amount_in),
        DexType::UniswapV3 => v3_price_impact(pool_b, amount_in),
    };

    // Buy on pool with more output (lower price), sell on pool with less output (higher price)
    if out_a > out_b {
        let profit = out_a - out_b;
        let profit_pct = if out_b > Decimal::ZERO {
            (profit / out_b) * dec!(100)
        } else {
            Decimal::ZERO
        };
        Some(ArbitrageOpportunity {
            token_in: token_in.to_string(),
            token_out: token_out.to_string(),
            dex_buy: pool_a.dex,
            dex_sell: pool_b.dex,
            amount_in,
            amount_out: out_a,
            profit,
            profit_pct,
        })
    } else if out_b > out_a {
        let profit = out_b - out_a;
        let profit_pct = if out_a > Decimal::ZERO {
            (profit / out_a) * dec!(100)
        } else {
            Decimal::ZERO
        };
        Some(ArbitrageOpportunity {
            token_in: token_in.to_string(),
            token_out: token_out.to_string(),
            dex_buy: pool_b.dex,
            dex_sell: pool_a.dex,
            amount_in,
            amount_out: out_b,
            profit,
            profit_pct,
        })
    } else {
        None
    }
}

/// Parse a pending transaction from mempool JSON
pub fn parse_pending_tx(json: &str) -> Option<MempoolTx> {
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    let params = value.get("params")?;
    let result = params.get(0)?;
    serde_json::from_value(result.clone()).ok()
}

/// Check if transaction is a swap (method signature 0x7ff36ab5 = swapExactETHForTokens,
/// 0x38ed1739 = swapExactTokensForTokens, etc.)
pub fn is_swap_tx(input: &str) -> bool {
    if input.len() < 10 {
        return false;
    }
    let method_sig = &input[0..10];
    matches!(
        method_sig,
        "0x7ff36ab5" | "0x38ed1739" | "0x18cbafe5" | "0x70a08231" | "0x414bf389"
    )
}

fn dex_name(dex: DexType) -> &'static str {
    match dex {
        DexType::UniswapV2 => "Uniswap V2",
        DexType::UniswapV3 => "Uniswap V3",
    }
}

fn log_opportunity(opp: &ArbitrageOpportunity) {
    println!(
        "[ARB] {} -> {} | Buy on {} | Sell on {} | In: {} | Out: {} | Profit: {} ({}%)",
        opp.token_in,
        opp.token_out,
        dex_name(opp.dex_buy),
        dex_name(opp.dex_sell),
        opp.amount_in,
        opp.amount_out,
        opp.profit,
        opp.profit_pct
    );
}

async fn run_simulation() {
    // Simulate mempool monitoring with sample data
    println!("=== MEV Hunter — Simulation Mode ===");
    println!("Monitoring mempool for arbitrage opportunities...\n");

    // Sample pool states (ETH/USDC)
    let pool_v2 = PoolState {
        reserve0: dec!(1000),   // ETH
        reserve1: dec!(2000000), // USDC
        fee_bps: 30,            // 0.3%
        dex: DexType::UniswapV2,
    };
    let pool_v3 = PoolState {
        reserve0: dec!(950),    // ETH
        reserve1: dec!(1950000), // USDC
        fee_bps: 5,             // 0.05%
        dex: DexType::UniswapV3,
    };

    // Check arbitrage for various trade sizes
    for amount in &[dec!(1), dec!(5), dec!(10), dec!(50)] {
        if let Some(opp) = check_arbitrage(&pool_v2, &pool_v3, "WETH", "USDC", *amount) {
            if opp.profit_pct > dec!(0.01) {
                log_opportunity(&opp);
            }
        }
    }

    println!("\n=== Simulation Complete ===");
}

#[tokio::main]
async fn main() {
    let ws_url = env::var("ALCHEMY_WS_URL").unwrap_or_else(|_| {
        println!("No ALCHEMY_WS_URL set — running in simulation mode");
        String::new()
    });

    if ws_url.is_empty() {
        run_simulation().await;
        return;
    }

    println!("Connecting to WebSocket: {}", ws_url);
    let url = url::Url::parse(&ws_url).expect("Invalid WebSocket URL");
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    // Subscribe to pending transactions
    let subscribe_msg = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["newPendingTransactions"]
    });
    ws_stream
        .send(Message::Text(subscribe_msg.to_string()))
        .await
        .expect("Failed to subscribe");

    println!("Subscribed to pending transactions. Monitoring...\n");

    while let Some(msg) = ws_stream.next().await {
        if let Ok(Message::Text(text)) = msg {
            if let Some(tx) = parse_pending_tx(&text) {
                if is_swap_tx(&tx.input) {
                    println!("Swap tx detected: {}", tx.hash);
                    // In production: fetch pool states, calculate arbitrage
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_v2_price_impact() {
        let pool = PoolState {
            reserve0: dec!(1000),
            reserve1: dec!(2000000),
            fee_bps: 30,
            dex: DexType::UniswapV2,
        };
        let out = v2_price_impact(&pool, dec!(1));
        assert!(out > Decimal::ZERO);
        assert!(out < dec!(2000));
    }

    #[test]
    fn test_v3_price_impact() {
        let pool = PoolState {
            reserve0: dec!(950),
            reserve1: dec!(1950000),
            fee_bps: 5,
            dex: DexType::UniswapV3,
        };
        let out = v3_price_impact(&pool, dec!(1));
        assert!(out > Decimal::ZERO);
    }

    #[test]
    fn test_arbitrage_detection() {
        let pool_a = PoolState {
            reserve0: dec!(1000),
            reserve1: dec!(2000000),
            fee_bps: 30,
            dex: DexType::UniswapV2,
        };
        let pool_b = PoolState {
            reserve0: dec!(950),
            reserve1: dec!(1950000),
            fee_bps: 5,
            dex: DexType::UniswapV3,
        };
        let opp = check_arbitrage(&pool_a, &pool_b, "WETH", "USDC", dec!(10));
        assert!(opp.is_some());
        let opp = opp.unwrap();
        assert!(opp.profit > Decimal::ZERO);
    }

    #[test]
    fn test_no_arbitrage_equal_pools() {
        let pool = PoolState {
            reserve0: dec!(1000),
            reserve1: dec!(2000000),
            fee_bps: 30,
            dex: DexType::UniswapV2,
        };
        let opp = check_arbitrage(&pool, &pool, "WETH", "USDC", dec!(10));
        // Equal pools should yield no arbitrage (or zero profit)
        if let Some(o) = opp {
            assert!(o.profit == Decimal::ZERO || o.profit_pct < dec!(0.001));
        }
    }

    #[test]
    fn test_is_swap_tx() {
        assert!(is_swap_tx("0x38ed173900000000000000000000000000000000000000000000000000000000"));
        assert!(!is_swap_tx("0x1234"));
        assert!(!is_swap_tx("0x"));
    }

    #[test]
    fn test_zero_reserves() {
        let pool = PoolState {
            reserve0: Decimal::ZERO,
            reserve1: dec!(2000000),
            fee_bps: 30,
            dex: DexType::UniswapV2,
        };
        let out = v2_price_impact(&pool, dec!(1));
        assert_eq!(out, Decimal::ZERO);
    }
}