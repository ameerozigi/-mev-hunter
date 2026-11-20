# MEV Hunter

A lightweight MEV bot simulation framework written in Rust, designed to monitor mempool data and calculate cross-DEX arbitrage opportunities.

## Features

- **Mempool Monitoring**: Connects to Ethereum nodes via WebSocket (Alchemy/Infura) to listen for pending transactions
- **Arbitrage Detection**: Calculates price discrepancies between Uniswap V2 and Uniswap V3 pools
- **Price Impact Modeling**: Implements V2 (constant product) and V3 (concentrated liquidity) price impact formulas
- **Simulation Mode**: Run without a node connection using sample pool data
- **Swap Detection**: Identifies swap transactions by method signature

## Quick Start

```bash
# Simulation mode (no node required)
cargo run

# Live mode (requires Alchemy/Infura WebSocket URL)
ALCHEMY_WS_URL=wss://eth-mainnet.g.alchemy.com/v2/YOUR_KEY cargo run
```

## How It Works

1. **Connect** to an Ethereum node via WebSocket
2. **Subscribe** to pending transactions via `eth_subscribe`
3. **Parse** incoming transactions and detect swap patterns
4. **Calculate** price impact on both Uniswap V2 and V3
5. **Log** arbitrage opportunities with profit estimates

## Tech Stack

- **Rust** — High-performance systems programming
- **Tokio** — Async runtime
- **tokio-tungstenite** — WebSocket client
- **rust_decimal** — Precise decimal arithmetic for financial calculations

## Tests

```bash
cargo test
```