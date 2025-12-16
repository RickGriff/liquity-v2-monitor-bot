# Liquity V2 Slack Monitoring bot

A lightweight TypeScript bot that monitors Liquity V2 activity on Ethereum mainnet and posts real-time, daily summaries to Slack.

## What it does

- Polls on-chain events for Trove operations (open/adjust/close, rate changes), liquidations, and redemptions across ETH, WSTETH, and RETH branches.
- Tracks Stability Pool deposit/withdraw activity and BOLD minting from borrower ops and interest.
- Computes simple derived metrics like LTV using Chainlink feeds. WSTETH uses a fixed exchange-rate multiplier.
- Posts formatted updates to Slack via an incoming webhook.

## How it works

- Uses viem + an Alchemy mainnet RPC to fetch logs in 1000-block chunks
- Maintains a small `state.json` for last processed blocks, daily stats, and cached oracle price logs
- Runs continuously on an interval, only processing new blocks since the last poll

## Run

- Install deps: npm install
- Configure .env (see below)
- Start: npm start
  
Env vars:
- `ALCHEMY_API_KEY` – Ethereum mainnet RPC key
- `SLACK_WEBHOOK_URL` – Slack incoming webhook URL
