import axios from 'axios';
import dotenv from 'dotenv';
import { createPublicClient, http, parseAbi } from 'viem';
import { mainnet } from 'viem/chains';
import fs from 'fs';

type Address = `0x${string}`;

const STATE_FILE = 'state.json';
const POST_TO_SLACK = true // toggle to only log to console for testing
dotenv.config();
	 
const BOLD_TOKEN_ADDRESS: Address = "0x6440f144b7e50d6a8439336510312d2f54beb01d"
const ZERO_ADDRESS: Address = "0x0000000000000000000000000000000000000000"

const DEPLOYMENT_BLOCK = 22516078n
// --- First blocks with trove ops ---
const FIRST_OPEN_TROVE_BLOCK = 22516627n
const FIRST_ADJUST_TROVE_BLOCK = 22516819n
const FIRST_CLOSE_TROVE_BLOCK = 22517875n
const FIRST_ADJUST_TROVE_RATE_BLOCK = 22524725n
const FIRST_LIQUIDATION_BLOCK = 22525888n
const FIRST_REDEMPTION_BLOCK = 22516536n

const BLOCKS_IN_ONE_HOUR = 3600n / 12n
const BLOCKS_IN_ONE_DAY = BLOCKS_IN_ONE_HOUR * 24n

const GETLOGS_MAX_INTERVAL = 1000n;

const MILLISECONDS = 1000;

const DECIMAL_PRECISION: bigint = 1_000_000_000_000_000_000n;
// const MIN_REDEMPTION: bigint = 50_000n * DECIMAL_PRECISION; // 50k at 18 decimal precision
const MIN_REDEMPTION: bigint = 0n;

// const MIN_LIQUIDATION: bigint = 50_000n * DECIMAL_PRECISION; 
const MIN_LIQUIDATION: bigint = 0n

//  Exchange rate on 16/12/2025 = 1.22458 STETH per WSTETH
// https://www.coingecko.com/en/coins/wrapped-steth/eth
// TODO: replace hardcoded val with the actual val emitted in event on WSTETH contract (it changes very slowly over time)
const WSTETH_EXCHANGE_RATE = 1_22_458_000_000_000_000n

// --- Chainlink data structures ---

interface PriceEntry {
  timestamp: bigint;
  answer: bigint;
  roundId: bigint;
  blockNumber: bigint;
}

// --- Collaterals ---

interface Coll {
	  label: string,
	  troveManager: Address,
	  sp: Address,
	  feed: Address,
	  priceDecimals: bigint,
	  priceCache: PriceEntry[]
	  cacheInterval: bigint
}

const ETH: Coll = {
  label: "ETH",
  troveManager: "0x7bcb64b2c9206a5b699ed43363f6f98d4776cf5a",
  sp: "0x5721cbbd64fc7ae3ef44a0a3f9a790a9264cf9bf",
  feed: "0x7d4E742018fb52E48b08BE73d041C18B21de6Fb5", // ETH-USD aggregator
  priceDecimals: 8n,
  priceCache: [],
  cacheInterval: BLOCKS_IN_ONE_HOUR * 3n // cover 2x oracle heartbeat, + 1hr buffer
}

const WSTETH: Coll = {
    label: "WSTETH",
  troveManager: "0xa2895d6a3bf110561dfe4b71ca539d84e1928b22",
  sp: "0x9502b7c397e9aa22fe9db7ef7daf21cd2aebe56b",
  feed: "0x26f196806f43E88FD27798C9e3fb8fdF4618240f", // STETH-USD aggregator
  priceDecimals: 8n,
  priceCache: [], 
  cacheInterval: BLOCKS_IN_ONE_HOUR * 3n // cover 2x oracle heartbeat, + 1hr buffer
}

const RETH: Coll = {
  label: "RETH",
  troveManager: "0xb2b2abeb5c357a234363ff5d180912d319e3e19e",
  sp: "0xd442e41019b7f5c4dd78f50dc03726c446148695",
  feed: "0xc77904CD2CA0806CC3DB0819E9630FF3e2f6093d", // RETH-ETH aggregator
  priceDecimals: 18n,
  priceCache: [],
  cacheInterval: BLOCKS_IN_ONE_HOUR * 49n // cover 2x oracle heartbeat, + 1hr buffer
}

enum TroveOperations {
  OpenTrove = 0,
  CloseTrove = 1,
  AdjustTrove = 2,
  AdjustTroveInterestRate = 3,
  ApplyPendingDebt = 4,
  Liquidate = 5,
  RedeemCollateral = 6,
  // Batch management
  OpenTroveAndJoinBatch = 7,
  SetInterestBatchManager = 8,
  RemoveFromBatch = 9  
}

enum BatchOperations {
  registerBatchManager = 0,
  lowerBatchManagerAnnualFee = 1,
  setBatchManagerAnnualInterestRate = 2,
  applyBatchInterestAndFee = 3,
  joinBatch = 4, 
  exitBatch = 5,
  troveChange = 6
}

enum DepositOperations {
  provideToSP = 0,
  withdrawFromSP = 1,
  claimAllCollGains = 2
}

const chainlinkEventSig = 'event AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt)';

const BOLDEventSigs = {
  Transfer: 'event Transfer(address indexed from, address indexed to, uint256 value)'
}

const troveEventSigs = {
  Liquidation: 'event Liquidation(uint256 _debtOffsetBySP, uint256 _debtRedistributed, uint256 _boldGasCompensation, uint256 _collGasCompensation, uint256 _collSentToSP, uint256 _collRedistributed, uint256 _collSurplus, uint256 _L_ETH, uint256 _L_boldDebt, uint256 _price)',
  Redemption: 'event Redemption(uint256 _attemptedBoldAmount, uint256 _actualBoldAmount, uint256 _ETHSent, uint256 _ETHFee, uint256 _price, uint256 _redemptionPrice)',
  TroveUpdated: 'event TroveUpdated(uint256 indexed _troveId, uint256 _debt, uint256 _coll, uint256 _stake, uint256 _annualInterestRate, uint256 _snapshotOfTotalCollRedist, uint256 _snapshotOfTotalDebtRedist)',
  TroveOperation: 'event TroveOperation(uint256 indexed _troveId, uint8 _operation, uint256 _annualInterestRate, uint256 _debtIncreaseFromRedist, uint256 _debtIncreaseFromUpfrontFee, int256 _debtChangeFromOperation, uint256 _collIncreaseFromRedist, int256 _collChangeFromOperation)',
  // RedemptionFeePaidToTrove: 'event RedemptionFeePaidToTrove(uint256 indexed _troveId, uint256 _ETHFee)',
  BatchUpdated: 'event BatchUpdated(address indexed _interestBatchManager, uint8 _operation, uint256 _debt, uint256 _coll, uint256 _annualInterestRate, uint256 _annualManagementFee, uint256 _totalDebtShares, uint256 _debtIncreaseFromUpfrontFee)',
  // BatchedTroveUpdated: 'event BatchedTroveUpdated(uint256 indexed _troveId, address _interestBatchManager, uint256 _batchDebtShares, uint256 _coll, uint256 _stake, uint256 _snapshotOfTotalCollRedist, uint256 _snapshotOfTotalDebtRedist)',
}

const spEventSigs = {
  // StabilityPoolCollBalanceUpdated: 'event StabilityPoolCollBalanceUpdated(uint256 _newBalance)',
  // StabilityPoolBoldBalanceUpdated: 'event StabilityPoolBoldBalanceUpdated(uint256 _newBalance)',
  // P_Updated: 'event P_Updated(uint256 _P)',
  // S_Updated: 'event S_Updated(uint256 _S, uint256 _scale)',
  // B_Updated: 'event B_Updated(uint256 _B, uint256 _scale)',
  // ScaleUpdated: 'event ScaleUpdated(uint256 _currentScale)',
  DepositUpdated: 'event DepositUpdated(address indexed _depositor, uint256 _newDeposit, uint256 _stashedColl, uint256 _snapshotP, uint256 _snapshotS, uint256 _snapshotB, uint256 _snapshotScale)',
  DepositOperation: 'event DepositOperation(address indexed _depositor, uint8 _operation, uint256 _depositLossSinceLastOperation, int256 _topUpOrWithdrawal, uint256 _yieldGainSinceLastOperation, uint256 _yieldGainClaimed, uint256 _ethGainSinceLastOperation, uint256 _ethGainClaimed)'
};

// --- Price functions ---

function getLTV(
  coll: Coll, 
  collVal: bigint, 
  debt: bigint, 
  rawPrice: bigint | null, 
  log: any, 
  wstethExRate: bigint = WSTETH_EXCHANGE_RATE
): string { 
  if (rawPrice === null) return "n/a";
  let price: bigint;
  
  // Scale to 18 decimal digits
  price = rawPrice * (10n ** (18n - coll.priceDecimals));

  // Convert STETH-USD to WSTETH-USD: USD_per_WSTETH = USD_per_STETH * STETH_per_WSTETH
  if (coll.label === "WSTETH") {price = price * wstethExRate / DECIMAL_PRECISION;}

  // Convert RETH-ETH to ETH-USD: USD_per_RETH = USD_per_ETH * ETH_per_RETH
  if (coll.label === "RETH") { 
    const ethUsdPrice = findClosestPrior(ETH.priceCache, log);
    if (ethUsdPrice === null) return "n/a";
    price = price * ethUsdPrice * (10n ** 10n) / DECIMAL_PRECISION;
  }

  const denom = collVal * price;
  if (denom === 0n) return "n/a";
  const ltv = debt * DECIMAL_PRECISION * DECIMAL_PRECISION / denom
  
  return percent(ltv);
}

async function fetchPriceEntries(coll: Coll, fromBlock: bigint, toBlock: bigint): Promise<PriceEntry[]> {
  let entries: PriceEntry[] = [];
 
  for (let from = fromBlock; from <= toBlock;) {
  const span = (from + GETLOGS_MAX_INTERVAL - 1n <= toBlock) ? GETLOGS_MAX_INTERVAL : (toBlock - from + 1n);
    const logs = await client.getLogs({
       address: coll.feed,
      events: parseAbi([chainlinkEventSig]),
      fromBlock: from,
      toBlock: from + span - 1n
    });
    
    for (const log of logs) {
      entries.push({
        timestamp: log.args.updatedAt,
        answer: log.args.current,
        roundId: log.args.roundId,
        blockNumber: log.blockNumber
      });
    }
    from += span;
  }
  entries.sort((a, b) => Number(a.timestamp - b.timestamp));
  return entries;
}

async function updateCache(coll: Coll, cacheFromBlock: bigint, toBlock: bigint) {
  if (coll.priceCache.length === 0) {
    coll.priceCache = await refillCache(coll, cacheFromBlock, toBlock);
    savePriceCaches();
    return;
  }
  
  const lastBlock = coll.priceCache.at(-1)!.blockNumber;

  // If cache is too new (processing historical data) or too old, refill completely
  if (lastBlock > toBlock || lastBlock < cacheFromBlock) {
    coll.priceCache = await refillCache(coll, cacheFromBlock, toBlock)
  // Otherwise fetch prices since the last cached and trim off stale prices
  } else {
    const fetched = await fetchPriceEntries(coll, lastBlock +1n, toBlock);
    coll.priceCache.push(...fetched);

    coll.priceCache = trimCache(coll, cacheFromBlock);
  }

  // Save after any cache modification
  savePriceCaches();
}

async function refillCache(coll: Coll, fromBlock: bigint, toBlock: bigint) {
  console.log("refill cache")
    coll.priceCache.length = 0;
    console.log("fetching price...");
    const fetched = await fetchPriceEntries(coll, fromBlock, toBlock);
    console.log(fetched.length, "refillCache::fetched.length")
    coll.priceCache.push(...fetched);

    return coll.priceCache;
}

function trimCache(coll: Coll, fromBlock: bigint) {
  return coll.priceCache.filter(entry => entry.blockNumber >= fromBlock)
}

function findClosestPrior(priceCache: PriceEntry[], troveLog: any): bigint | null {
  const troveBlockNumber = BigInt(troveLog.blockNumber);
  const priorEntries = priceCache.filter(entry => {
    return entry.blockNumber < troveBlockNumber});

  return priorEntries.at(-1)?.answer ?? null;
}

function savePriceCaches(): void {
  const state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  state.ethPriceCache = ETH.priceCache.map(entry => ({
    timestamp: entry.timestamp.toString(),
    answer: entry.answer.toString(),
    roundId: entry.roundId.toString(),
    blockNumber: entry.blockNumber.toString()
  }));
  state.wstethPriceCache = WSTETH.priceCache.map(entry => ({
    timestamp: entry.timestamp.toString(),
    answer: entry.answer.toString(),
    roundId: entry.roundId.toString(),
    blockNumber: entry.blockNumber.toString()
  }));
  state.rethPriceCache = RETH.priceCache.map(entry => ({
    timestamp: entry.timestamp.toString(),
    answer: entry.answer.toString(),
    roundId: entry.roundId.toString(),
    blockNumber: entry.blockNumber.toString()
  }));
  fs.writeFileSync(STATE_FILE, JSON.stringify(state));
}

// --- Formatting functions ---

function formatNum(val: bigint | number): string {
  const scaledVal = (typeof val === "bigint" ? Number(val) : val) / 1e18;

  const absScaledVal = Math.abs(scaledVal);
  if (absScaledVal < 1) {
    return scaledVal.toLocaleString('en-US', { 
      minimumFractionDigits: 2, 
      maximumFractionDigits: 2 
    });  
  } else if (absScaledVal < 100) {
    return scaledVal.toLocaleString('en-US', { 
      minimumFractionDigits: 1, 
      maximumFractionDigits: 1 
    });  
  } else {
    return scaledVal.toLocaleString('en-US', { 
      minimumFractionDigits: 0, 
      maximumFractionDigits: 0 
    }); 
  }
}

function dateAndTime(timestamp: number) {
  return (new Date(timestamp)).toISOString().slice(0,-5).split('T').join(' ')
}

// function getTimeOrPastDateAndTime(timestamp: number) {
//   if timestamp > 
//   return dateAndTime(timestamp).split(" ")[1]
// }

function percent(amount: bigint): string {
  return `${(Number(amount) * 100 / 1e18).toFixed(2)}%`
}

function addCircles(val: bigint, scale: number = 1): string {
  const absScaledVal = Math.abs(Number(val) / 1e18);
  
  const circle = val >= 0n ? "🟢" : "🔴"

  if (absScaledVal < 100_000 / scale) {
    return "";
  } else if (absScaledVal < 1_000_000 / scale) {
    return circle;
  } else {
    return circle.concat(circle);
  }
}

function txHashLink(log: any): string {
  return  `<https://etherscan.io/tx/${log.transactionHash}|Tx hash>`
}

function addressLink(address: string, linkText: string): string {
  return `<https://etherscan.io/address/${address}|${linkText}>`
}

// --- Posting functions ---

async function postToSlack(message: string): Promise<void> {
  console.log(`Posting to slack: ${POST_TO_SLACK}`)
  if (POST_TO_SLACK) {
    console.log(message)
    await axios.post(process.env.SLACK_WEBHOOK_URL!, {
      text: message
    });
  } else {
    console.log(message);
  }
}

// Ethereum client
// const client = createPublicClient({
//   chain: mainnet,
//   transport: http(), // Uses free Cloudflare endpoint, for 1000 block range
// });

// TODO: Use Liquity AG's paid alchemy plan
const client = createPublicClient({
  chain: mainnet,
  transport: http(`https://eth-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`)
});

// --- Individual event posting functions ---

async function postSPDeposits(logs: any[]) {
  console.log(` --- SP deposits ---`)
  const deposits = logs.filter(log => log.eventName === 'DepositUpdated')
  for (const deposit of deposits) {
    const {_depositor, _newDeposit} = deposit.args
    await postToSlack(`Deposit updated. address: ${_depositor} \n 
      new deposit: ${_newDeposit}`)
  }
}

async function postOpenTrove(logs: any[], coll: Coll) {
  console.log(` --- openTroves ---`)
  const opened = logs.filter(log => 
    log.eventName === "TroveOperation" && 
    log.args['_operation'] === TroveOperations.OpenTrove
  )

  opened.sort((a, b) => a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0)

  for (const trove of opened) {
    const debt = trove.args._debtChangeFromOperation + trove.args._debtIncreaseFromUpfrontFee;
    const collVal = trove.args._collChangeFromOperation;
    const price = findClosestPrior(coll.priceCache, trove);
    const ltv = getLTV(coll, collVal, debt, price, trove);

    await postToSlack(`
      ${addCircles(debt)} *Trove opened* (${coll.label}) - Loan: ${formatNum(debt)}, Coll: ${formatNum(collVal)}, LTV: ${ltv}, Rate: ${percent(trove.args._annualInterestRate)} [${dateAndTime(trove.timestamp)}, ${txHashLink(trove)}]` 
    )
  }
}

async function postClosedTrove(logs: any[], coll: Coll) {
  const closed = logs.filter(log => 
    log.eventName === "TroveOperation" && 
    log.args['_operation'] === TroveOperations.CloseTrove
  )

  closed.sort((a, b) => a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0)

  for (const trove of closed) {
    const debt = trove.args._debtChangeFromOperation * -1n;
    const collVal = trove.args._collChangeFromOperation * -1n;
    const price = findClosestPrior(coll.priceCache, trove);
    const ltv = getLTV(coll, collVal, debt, price, trove);

    await postToSlack(
      `${addCircles(trove.args._debtChangeFromOperation)} *Trove closed* (${coll.label}) - Debt: ${formatNum(debt)}, Coll: ${formatNum(collVal)}, LTV: ${ltv}, [${dateAndTime(trove.timestamp)}, ${txHashLink(trove)}]`
    )
  }
}

async function postAdjustTrove(logs: any[], coll: Coll): Promise<void> {
  const txHashToEvents = new Map<bigint, { adjusted: any, updated: any }>();

  const sortedLogs = [...logs].sort((a, b) => a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0);
  
  // Catch each pair of (AdjustTrove, TroveUpdated) events with same Tx hash.
  for (const log of sortedLogs) {
    const txHash = log.transactionHash;

    if (!txHashToEvents.has(txHash)) {
      txHashToEvents.set(txHash, { adjusted: null, updated: null });
    }

    if (log.eventName === "TroveOperation" && 
        log.args._operation === TroveOperations.AdjustTrove) {
      txHashToEvents.get(txHash)!.adjusted = log;
    } else if (log.eventName === "TroveUpdated") {
      txHashToEvents.get(txHash)!.updated = log;
    }
  }

  for (const { adjusted, updated } of txHashToEvents.values()) {
    if (adjusted && updated && adjusted.args._troveId === updated.args._troveId) {
      const debtChange = 
        adjusted.args._debtIncreaseFromUpfrontFee + 
        adjusted.args._debtChangeFromOperation +
        adjusted.args._debtIncreaseFromRedist
      
      const prevDebt = updated.args._debt - debtChange;
        
      const prevColl = updated.args._coll - 
        adjusted.args._collIncreaseFromRedist - 
        adjusted.args._collChangeFromOperation;
      
      const debtCircles = addCircles(debtChange);
      // TODO: incorporate coll change in circle logic, for all ops - opened, adjusted, closed. 
      // Big changes of either debt or coll (or both) should post circles.

      const debtChangeStr = 
        updated.args._debt === prevDebt  ? 
          `Debt: ${formatNum(prevDebt)}` :
          `Debt: ${formatNum(prevDebt)} => ${formatNum(updated.args._debt)}`

      const collChangeStr: string = 
        updated.args._coll === prevColl ?
         `Coll: ${formatNum(prevColl)}` :
        `Coll: ${formatNum(prevColl)} => ${formatNum(updated.args._coll)}`

      const price = findClosestPrior(coll.priceCache, adjusted)
      const ltv = getLTV(coll, updated.args._coll, updated.args._debt, price, adjusted);

      await postToSlack(
        `${debtCircles} *Trove adjusted* (${coll.label}) - ${debtChangeStr}, ${collChangeStr}, LTV: ${ltv}, Rate: ${percent(adjusted.args._annualInterestRate)} [${dateAndTime(adjusted.timestamp)}, ${txHashLink(adjusted)}]`
      )
    }
  }
}

async function postAdjustRate(logs: any[], coll: Coll): Promise<void> {
  console.log(` --- adjustRates ---`)

  const sortedLogs = [...logs].sort((a, b) => a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0);

  const txHashToEvents = new Map<bigint, { adjusted: any, updated: any }>();
  
  // Pair TroveOperation (rate adjustment) with TroveUpdated events
  for (const log of sortedLogs) {
    const txHash = log.transactionHash;

    if (!txHashToEvents.has(txHash)) {
      txHashToEvents.set(txHash, { adjusted: null, updated: null });
    }
    
    if (log.eventName === "TroveOperation" && 
        log.args._operation === TroveOperations.AdjustTroveInterestRate) {
      txHashToEvents.get(txHash)!.adjusted = log;
    } else if (log.eventName === "TroveUpdated") {
      txHashToEvents.get(txHash)!.updated = log;
    }
  }

  // Process pairs and post the rate adjustment
  for (const { adjusted, updated } of txHashToEvents.values()) {
    if (adjusted && updated && adjusted.args._troveId === updated.args._troveId) {
      const debt = formatNum(updated.args._debt);
      const collVal = formatNum(updated.args._coll);

      const price = findClosestPrior(coll.priceCache, adjusted)
      const ltv = getLTV(coll, updated.args._coll, updated.args._debt, price, adjusted);
      
      // Format interest rate
      const interestRate = percent(adjusted.args._annualInterestRate)
      
      await postToSlack(
        `*Rate Adjusted* (${coll.label}) - Loan: ${debt}, Coll: ${collVal}, New rate: ${interestRate}, LTV: ${ltv}, [${dateAndTime(adjusted.timestamp)}, ${txHashLink(adjusted)}]`
      );
    }
  }
}

async function postBatchAdjustRate(logs: any[], coll: Coll): Promise<void> {
  console.log(` --- batchAdjustRate ---`)
  const batchAdjusted = logs.filter(log => 
  log.eventName === "BatchUpdated" &&
  log.args._operation == BatchOperations.setBatchManagerAnnualInterestRate)
  
  batchAdjusted.sort((a, b) => a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0)

  for (const log of batchAdjusted) {
    await postToSlack( 
      `*Batch rate adjusted* (${coll.label}) - New rate: ${percent(log.args._annualInterestRate)}, Batch debt: ${formatNum(log.args._debt)} [${dateAndTime(log.timestamp)}, ${txHashLink(log)}, ${addressLink(log.args._interestBatchManager, "Manager")}]`
    )
  }    
};

async function postBatchAdjustFee(logs: any[], coll: Coll): Promise<void> {
  const batchAdjusted = logs.filter(log => 
  log.eventName === "BatchUpdated" &&
  log.args._operation == BatchOperations.lowerBatchManagerAnnualFee)

  batchAdjusted.sort((a, b) => a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0)
  
  for (const log of batchAdjusted) {
    await postToSlack( 
      `*Batch fee lowered* (${coll.label}) - New fee: ${log.args._annualManagementFee}, Batch debt: ${log.args._debt} [${dateAndTime(log.timestamp)}, ${txHashLink(log)}, ${addressLink(log.args._interestBatchManager, "Manager")}]`
    )
  } 
};

async function postRedemption(logs: any[], coll: Coll): Promise<void> {
  const redemptions = logs.filter(log => 
    log.eventName === "Redemption" && 
    log.args._actualBoldAmount > MIN_REDEMPTION)

  redemptions.sort((a, b) => a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0)
  /* 
  * Effective redemption price:
  * USD_per_BOLD =
  * USD_value_received / BOLD_redeemed =
  * LST_received * USD_per_LST  / BOLD_redeemed 
  */
  for (const log of redemptions) {
    if (log.args._ETHSent === 0n) {continue;}

    const effectiveBOLDPrice = (log.args._ETHSent + log.args._ETHFee) * log.args._redemptionPrice / log.args._actualBoldAmount;

    const feePct = percent(log.args._ETHFee * DECIMAL_PRECISION  / (log.args._ETHFee + log.args._ETHSent));

    await postToSlack(` *Redemption* (${coll.label}) - Amount: ${formatNum(log.args._actualBoldAmount)}, Fee rate: ${(feePct)}, BOLD price: ${formatNum(effectiveBOLDPrice)} USD, ${coll.label} price: ${formatNum(log.args._redemptionPrice)} USD [${dateAndTime(log.timestamp)}, ${txHashLink(log)}]`
    )
  }
}

async function postLiquidation(logs: any[], coll: Coll): Promise<void> {
  const liqs = logs.filter(
    (log) =>
      log.eventName === "Liquidation"
    //  &&
    //   (log.args._debtOffsetBySP + log.args._debtRedistributed) > MIN_LIQUIDATION
  );

  liqs.sort((a, b) => a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0)

  for (const log of liqs) {
    const boldAmount =
      log.args._debtOffsetBySP + log.args._debtRedistributed;
    // const collAmount =
    //   log.args._collSentToSP +
    //   log.args._collRedistributed +
    //   log.args._collGasCompensation +
    //   log.args._collSurplus;
    const price = log.args._price;

    await postToSlack(
      `*Liquidation* (${coll.label}) ${dateAndTime(log.timestamp)} - Amount: ${formatNum(boldAmount)} BOLD, price: ${formatNum(price)} USD ${txHashLink(log)}`
    );
  }
}

// --- Posting functions for all branches ---

async function postAllOpenTrove(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postOpenTrove(wethTroveLogs, ETH)
  await postOpenTrove(wstethTroveLogs, WSTETH)
  await postOpenTrove(rethTroveLogs, RETH)
}

async function postAllClosedTrove(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postClosedTrove(wethTroveLogs, ETH)
  await postClosedTrove(wstethTroveLogs, WSTETH)
  await postClosedTrove(rethTroveLogs, RETH)
}

async function postAllAdjustTrove(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postAdjustTrove(wethTroveLogs, ETH)
  await postAdjustTrove(wstethTroveLogs, WSTETH)
  await postAdjustTrove(rethTroveLogs, RETH)
}

async function postAllAdjustRate(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postAdjustRate(wethTroveLogs, ETH)
  await postAdjustRate(wstethTroveLogs, WSTETH)
  await postAdjustRate(rethTroveLogs, RETH)
}

async function postAllBatchAdjustRate(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postBatchAdjustRate(wethTroveLogs, ETH)
  await postBatchAdjustRate(wstethTroveLogs, WSTETH)
  await postBatchAdjustRate(rethTroveLogs, RETH)
}

async function postAllBatchAdjustFee(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postBatchAdjustFee(wethTroveLogs, ETH)
  await postBatchAdjustFee(wstethTroveLogs, WSTETH)
  await postBatchAdjustFee(rethTroveLogs, RETH)
}

async function postAllRedemption(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postRedemption(wethTroveLogs, ETH)
  await postRedemption(wstethTroveLogs, WSTETH)
  await postRedemption(rethTroveLogs, RETH)
}

async function postAllLiquidation(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postLiquidation(wethTroveLogs, ETH)
  await postLiquidation(wstethTroveLogs, WSTETH)
  await postLiquidation(rethTroveLogs, RETH)
}

// --- Daily update functionality ---

async function postDailyUpdate(
  colls: Coll[],
  boldTokenAddress,
  dayStartBlock: bigint,
  dayEndBlock: bigint,
  troveEvents,
  boldEvents,
  spEvents,
  interval: bigint = GETLOGS_MAX_INTERVAL
): Promise<DailyStats>  {
  let total = 0n;

  let troveLogs: any[] = []
  let boldLogs: any[] = []
  const spLogs = {
    "ETH": [],
    "WSTETH": [],
    "RETH": []
  }

  // Get all Trove logs for the day
  for (let from = dayStartBlock; from <= dayEndBlock; ) {
    const span = (from + interval - 1n <= dayEndBlock) ? interval : (dayEndBlock - from + 1n);

    for (const coll of colls) {
      troveLogs.push(...
        (await client.getLogs({
          address: coll.troveManager,
          events: troveEvents,
          fromBlock: from,
          toBlock: from + span - 1n
        }))
      )

      spLogs[coll.label].push(...
        (await client.getLogs({
        address: coll.sp,
          events: spEvents,
          fromBlock: from,
          toBlock: from + span - 1n
        }))
      )
      await addTimestamps(spLogs[coll.label])
    }

      boldLogs.push(...
        (await client.getLogs({
        address: boldTokenAddress,
        events: boldEvents,
        fromBlock: from,
        toBlock: from + span - 1n
      }))
    )

    from += span;
  }

  const counts = getTroveCounts(troveLogs);
  const minted = getMinted(boldLogs);
  const repaid = getRepaid(troveLogs);
  const redeemed = getRedeemed(troveLogs);
  const liquidated = getLiquidated(troveLogs);
  const [wethTotals, wstethTotals, rethTotals] = getSPTotals(spLogs);

  const dayStartTime = Number((await client.getBlock({blockNumber: dayStartBlock})).timestamp) * MILLISECONDS;

  const state = loadAppState();

  const dailyStats = {
    netMinted: minted - repaid - liquidated - redeemed,
    netWethSP: wethTotals.deposits - wethTotals.withdrawals,
    netWstethSP: wstethTotals.deposits - wstethTotals.withdrawals,
    netRethSP: rethTotals.deposits - rethTotals.withdrawals,
    wstethExRate: WSTETH_EXCHANGE_RATE
  }
 
  await postToSlack(
    `*Daily stats* - ${dateAndTime(dayStartTime)}.\n

    *BOLD*
    Minted: ${formatNum(minted)}  
    Repaid: ${formatNum(repaid)}  
    Redeemed: ${formatNum(redeemed)}  
    Liquidated: ${formatNum(liquidated)} 
    Net total: ${formatNum(state.lastNetMinted)} => ${formatNum(dailyStats.netMinted)}
    \n

    *Troves*
    Troves opened: ${counts.opened} 
    Troves adjusted: ${counts.adjusted} 
    Troves closed: ${counts.closed} 
    Rate adjustments: ${counts.rateAdjusted} 
    Batch adjustments: ${counts.batchRateAdjusted}
    \n
    
    *SP*
    Deposits: ${formatNum(wethTotals.deposits + wstethTotals.deposits + rethTotals.deposits )}
    SP Withdrawals: ${formatNum(wethTotals.withdrawals + wstethTotals.withdrawals + rethTotals.withdrawals)}
    Net Totals: ${formatNum(dailyStats.netWethSP + dailyStats.netWstethSP + dailyStats.netRethSP)}`
   )

    //  ETH SP: Deposits ${formatNum(wethTotals.deposits)} (${wethTotals.depositsCount}) | Withdrawals: ${formatNum(wethTotals.withdrawals)} (${wethTotals.withdrawalsCount}) 
    // Net total: ${formatNum(state.lastNetWethSP)} => ${formatNum(dailyStats.netWethSP)} \n
    // WSTETH SP: Deposits ${formatNum(wstethTotals.deposits)} (${wstethTotals.depositsCount}) | Withdrawals: ${formatNum(wstethTotals.withdrawals)} (${wstethTotals.withdrawalsCount}) 
    // Net total: ${formatNum(state.lastNetWstethSP)} => ${formatNum(dailyStats.netWstethSP)} \n
    // RETH SP: Deposits ${formatNum(rethTotals.deposits)} (${rethTotals.depositsCount}) | Withdrawals: ${formatNum(rethTotals.withdrawals)} (${rethTotals.withdrawalsCount}) 
    // Net total: ${formatNum(state.lastNetRethSP)} => ${formatNum(dailyStats.netRethSP)} \n

   return dailyStats;
}

interface SPTotals{
  deposits: bigint
  withdrawals: bigint
}

function makeSPTotals(): SPTotals {
  return {
    deposits: 0n,
    withdrawals: 0n
  }
}

function getSPTotals(spLogs: Record<string, any[]>): SPTotals[]  {
  const colls = {
    "ETH": makeSPTotals(),
    "WSTETH": makeSPTotals(),
    "RETH": makeSPTotals()
  }

  for (const coll of Object.keys(spLogs)) {
    // Group operations by transaction hash
    const txMap = new Map<string, { deposits: bigint, withdrawals: bigint }>();
    
    for (const log of spLogs[coll]) {
      if (log.eventName === "DepositOperation") {
        const txHash = log.transactionHash;
        
        if (!txMap.has(txHash)) {
          txMap.set(txHash, { deposits: 0n, withdrawals: 0n });
        }
        
        const txData = txMap.get(txHash)!;
        
        if (log.args._operation === DepositOperations.provideToSP) {
          txData.deposits += log.args._topUpOrWithdrawal;
        } else if (log.args._operation === DepositOperations.withdrawFromSP) {
          txData.withdrawals += -BigInt(log.args._topUpOrWithdrawal);
        }
      }
    }
    
    // Calculate net per transaction and add to totals
    for (const { deposits, withdrawals } of txMap.values()) {
      const net = deposits - withdrawals;
      
      if (net > 0n) {
        colls[coll].deposits += net;
      } else if (net < 0n) {
        colls[coll].withdrawals += -net;
      }
    }
  }
  
  return [colls.ETH, colls.WSTETH, colls.RETH];
}

function getRepaid(troveLogs: any[]): bigint {
  let repaid: bigint = 0n;
  
  for (const log of troveLogs) {
    if (log.args._operation === TroveOperations.AdjustTrove ||
       log.args._operation === TroveOperations.CloseTrove
    ) {
      const delta = log.args._debtChangeFromOperation as bigint; // int256
      if (delta < 0n) repaid += -delta; // negative delta = user repayment
    }
  }

  return repaid;
}

function getMinted(boldLogs: any[]): bigint {
  let minted: bigint = 0n;

  for (const log of boldLogs) {
    if (log.eventName == "Transfer" && log.args.from == ZERO_ADDRESS){
      minted += log.args.value;
    }
  }

  return minted;
}

function getRedeemed(troveLogs: any[]): bigint {
  let redeemed: bigint = 0n;

  for (const log of troveLogs) {
    if (log.eventName === "Redemption") {
      redeemed += log.args._actualBoldAmount;
    }
  }

  return redeemed;
}

function getLiquidated(troveLogs: any[]): bigint {
  let liquidated: bigint = 0n;

  for (const log of troveLogs) {
    if (log.eventName === "Liquidation") {
      liquidated += log.args._debtOffsetBySP + log.args._debtRedistributed;
    }
  }

  return liquidated;
}

interface TroveCounts{
  opened: number
  adjusted: number
  closed: number
  rateAdjusted: number
  batchRateAdjusted: number
}

function getTroveCounts(troveLogs: any[]): TroveCounts {
  const counts: TroveCounts = {
    opened: 0,
    adjusted: 0,
    closed: 0,
    rateAdjusted: 0,
    batchRateAdjusted: 0
  };

  const closedTxHashes: string[] = []

  for (const log of troveLogs) {
    if (log.eventName === "TroveOperation") {
      if (log.args._operation === TroveOperations.OpenTrove) {counts.opened += 1}
      else if (log.args._operation === TroveOperations.AdjustTrove) {counts.adjusted += 1}
      else if (log.args._operation === TroveOperations.CloseTrove) {
        closedTxHashes.push(log.transactionHash);
        counts.closed += 1}
      else if (log.args._operation === TroveOperations.AdjustTroveInterestRate) {counts.rateAdjusted += 1}
    }
  
    if (log.eventName == "BatchUpdated" && log.args._operation == BatchOperations.lowerBatchManagerAnnualFee) {
      counts.batchRateAdjusted += 1
    }
  }

  console.log(closedTxHashes.length, "closed length")
  console.log(countDupes(closedTxHashes), "number of dupe txs with ClosedTrove ops in")
  return counts;
}

function countDupes(txHashes: (string | null | undefined)[]): number {
  const seen = new Set<string>();
  let dupes = 0;

  for (const hash of txHashes) {
    if (!hash) continue;
    const k = hash.toLowerCase(); // normalize
    if (seen.has(k)) dupes++;
    else seen.add(k);
  }

  return dupes;
}

// --- Helpers for finding first blocks with given events, for testing ---

interface FirstTroveOpBlocks {
  openTroveBlock?: bigint;
  adjustTroveBlock?: bigint;
  closeTroveBlock?: bigint;
  adjustTroveRateBlock?: bigint;
  liquidateBlock?: bigint;
  redemptionBlock?: bigint;
}

function getFirstTroveOps(troveLogs: any[]): FirstTroveOpBlocks {
  let first: FirstTroveOpBlocks = {};

  for (const log of troveLogs) {
    if (log.eventName === "TroveOperation" && log.args._operation === TroveOperations.OpenTrove) {
      first.openTroveBlock = log.blockNumber
    }

    if (log.eventName === "TroveOperation" && log.args._operation === TroveOperations.AdjustTrove) {
      first.adjustTroveBlock = log.blockNumber
    }

    if (log.eventName === "TroveOperation" && log.args._operation === TroveOperations.CloseTrove) {
      first.closeTroveBlock = log.blockNumber
    }

    if (log.eventName === "TroveOperation" && log.args._operation === TroveOperations.AdjustTroveInterestRate) {
      first.adjustTroveRateBlock = log.blockNumber
    }

    if (log.eventName === "Liquidation") {
      first.liquidateBlock = log.blockNumber
    }

    if (log.eventName === "Redemption") {
      first.redemptionBlock = log.blockNumber
    }
  }

  return first;
}

async function findFirstTroveOps(troveEvents: any[]): Promise<void> {
  const first: FirstTroveOpBlocks = {
      openTroveBlock: undefined,
      adjustTroveBlock: undefined,
      closeTroveBlock: undefined,
      adjustTroveRateBlock: undefined,
      liquidateBlock: undefined,
      redemptionBlock: undefined
  }

  let foundCount: number = 0;
  let startBlock = DEPLOYMENT_BLOCK;
  const span = 1000n;

  // While we have not found a first instance of each Trove op, fetch the next chunk of logs
  while (foundCount < Object.keys(first).length) {
    console.log("events chunk")
    for (const key in first) {
      console.log(key, first[key]);
    }
    const wethTroveLogs = await client.getLogs({
      address: ETH.troveManager, 
      events: troveEvents, 
      fromBlock: startBlock, 
      toBlock: startBlock + span
    });

    const wstethTroveLogs = await client.getLogs({
      address: WSTETH.troveManager, 
      events: troveEvents, 
      fromBlock: startBlock, 
      toBlock: startBlock + span
    });

    const rethTroveLogs = await client.getLogs({
      address: RETH.troveManager, 
      events: troveEvents, 
      fromBlock: startBlock, 
      toBlock: startBlock + span
    });

    const allTroveLogs = wethTroveLogs.concat(wstethTroveLogs).concat(rethTroveLogs);

    const found = getFirstTroveOps(allTroveLogs) 
    for (const key in found) {
      console.log(key, first[key]);
    }

    // If we've found the first instance of a given Trove op, record its block
    for (const key of Object.keys(found)) {
      if (first[key] === undefined && found[key] !== undefined) {
        first[key] = found[key]
        foundCount += 1;
      }
    }

    startBlock += span;  
  }

  // Log the block for each first Trove op 
  for (const key in first) {
    console.log(key, first[key]);
  }
}

// --- Polling functionality ---

interface AppState {
  lastPolledBlock: bigint;
  lastDailyBlock: bigint;
  lastNetMinted: bigint;
  lastNetWethSP: bigint;
  lastNetWstethSP: bigint;
  lastNetRethSP: bigint;
  wstethExRate: bigint;
}

function loadAppState(): AppState {
  try {
    const raw = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    return {
      lastPolledBlock: BigInt(raw.lastPolledBlock || DEPLOYMENT_BLOCK),
      lastDailyBlock: BigInt(raw.lastDailyBlock || DEPLOYMENT_BLOCK),
      lastNetMinted: BigInt(raw.lastNetMinted || 0),
      lastNetWethSP: BigInt(raw.lastNetWethSP || 0),
      lastNetWstethSP: BigInt(raw.lastNetWstethSP || 0),
      lastNetRethSP: BigInt(raw.lastNetRethSP || 0),
      wstethExRate: BigInt(raw.wstethExRate || 0)
    };
  } catch (error) {
    // First run - return defaults
    return {
      lastPolledBlock: DEPLOYMENT_BLOCK,
      lastDailyBlock: DEPLOYMENT_BLOCK,
      lastNetMinted: 0n,
      lastNetWethSP: 0n,
      lastNetWstethSP: 0n,
      lastNetRethSP: 0n,
      wstethExRate: WSTETH_EXCHANGE_RATE
    };
  }
}

function savePolledBlock(pollBlock: bigint): void {
  const state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  state.lastPolledBlock = pollBlock.toString();
  fs.writeFileSync(STATE_FILE, JSON.stringify(state));
}

function saveDailyStats(dailyBlock: bigint, dailyStats: DailyStats): void {
  const state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  state.lastDailyBlock = dailyBlock.toString();
  state.lastNetMinted = dailyStats.netMinted.toString();
  state.lastNetWethSP = dailyStats.netWethSP.toString();
  state.lastNetWstethSP = dailyStats.netWstethSP.toString();
  state.lastNetRethSP = dailyStats.netRethSP.toString();
  state.wstethExRate = dailyStats.wstethExRate.toString();

  fs.writeFileSync(STATE_FILE, JSON.stringify(state));
}

async function pollForNewEvents() {
  const currentBlock = await client.getBlockNumber();
  const state = loadAppState();
  
  // Only get the past day's update, even if we're >1 day out of date
  if (currentBlock - state.lastDailyBlock > BLOCKS_IN_ONE_DAY) {
    await dailyUpdate();
  }

  // Individual events. Limit to 1 day's worth of blocks per poll
  const fromBlock = state.lastPolledBlock + 1n;
  const maxToBlock = fromBlock + BLOCKS_IN_ONE_DAY - 1n;
  const toBlock = currentBlock < maxToBlock ? currentBlock : maxToBlock;
  
  // Update price caches and load them onto colls.
  await updateCache(ETH, getCacheFirstBlock(ETH, fromBlock), toBlock);
  await updateCache(WSTETH, getCacheFirstBlock(WSTETH, fromBlock), toBlock);
  await updateCache(RETH, getCacheFirstBlock(RETH, fromBlock), toBlock);

  let wethTroveLogs: any[] = [];
  let wstethTroveLogs:  any[] = [];
  let rethTroveLogs:  any[] = [];
  
  // Fetch in 1000-block chunks
  const troveEvents = parseAbi(Object.values(troveEventSigs));
  for (let from = fromBlock; from <= toBlock; ) {
    // console.log("fetch events step")
    const span = (from + GETLOGS_MAX_INTERVAL - 1n <= toBlock) ? GETLOGS_MAX_INTERVAL : (toBlock - from + 1n);
    
    const wethLogsChunk = await client.getLogs({
      address: ETH.troveManager, 
      events: troveEvents, 
      fromBlock: from, 
      toBlock: from + span -1n
    })
    await addTimestamps(wethLogsChunk);
    wethTroveLogs.push(...wethLogsChunk);

    const wstethLogsChunk = await client.getLogs({
      address: WSTETH.troveManager, 
      events: troveEvents, 
      fromBlock: from, 
      toBlock: from + span -1n
    })
    await addTimestamps(wstethLogsChunk);
    wstethTroveLogs.push(...wstethLogsChunk);

    const rethLogsChunk = await client.getLogs({
      address: RETH.troveManager, 
      events: troveEvents, 
      fromBlock: from, 
      toBlock: from + span -1n
    })
    await addTimestamps(rethLogsChunk);
    rethTroveLogs.push(...rethLogsChunk);
    
    from += span;
  }

  // Post all ops
  await postAllOpenTrove(wethTroveLogs, wstethTroveLogs, rethTroveLogs);
  await postAllAdjustTrove(wethTroveLogs, wstethTroveLogs, rethTroveLogs);
  await postAllClosedTrove(wethTroveLogs, wstethTroveLogs, rethTroveLogs);
  await postAllAdjustRate(wethTroveLogs, wstethTroveLogs, rethTroveLogs);
  await postAllBatchAdjustRate(wethTroveLogs, wstethTroveLogs, rethTroveLogs);
  await postAllBatchAdjustFee(wethTroveLogs, wstethTroveLogs, rethTroveLogs);
  await postAllRedemption(wethTroveLogs, wstethTroveLogs, rethTroveLogs);
  await postAllLiquidation(wethTroveLogs, wstethTroveLogs, rethTroveLogs);

  savePolledBlock(toBlock);

  // Trim block cache - keep last 2 days worth
  const cacheRetentionBlocks = BLOCKS_IN_ONE_DAY * 2n;
  trimBlockCache(toBlock - cacheRetentionBlocks);
}

function getCacheFirstBlock(coll: Coll, block: bigint ) {
  return block - coll.cacheInterval;
}

// Global cache at top of file
const blockTimestampCache = new Map<bigint, number>();

async function addTimestamps(logs: any[]): Promise<void> {
  // Find blocks not in cache
  const blocksToFetch = new Set<bigint>();
  for (const log of logs) {
    if (!blockTimestampCache.has(log.blockNumber)) {
      blocksToFetch.add(log.blockNumber);
    }
  }

  // Fetch missing blocks in parallel
  if (blocksToFetch.size > 0) {
    const blocks = await Promise.all(
      Array.from(blocksToFetch).map(bn => client.getBlock({ blockNumber: bn }))
    );
    
    blocks.forEach(block => {
      blockTimestampCache.set(block.number, Number(block.timestamp) * MILLISECONDS);
    });
  }

  // Apply timestamps (mostly from cache)
  for (const log of logs) {
    log.timestamp = blockTimestampCache.get(log.blockNumber)!;
  }
}

function trimBlockCache(minBlock: bigint): void {
  for (const [blockNum] of blockTimestampCache) {
    if (blockNum < minBlock) {
      blockTimestampCache.delete(blockNum);
    }
  }
}

interface DailyStats {
  netMinted: bigint,
  netWethSP: bigint,
  netWstethSP: bigint,
  netRethSP: bigint,
  wstethExRate
}

async function dailyUpdate() {
  const currentBlock = await client.getBlockNumber();
  const dayStartBlock = currentBlock - BLOCKS_IN_ONE_DAY;
  
  const spEvents = parseAbi(Object.values(spEventSigs));
  const troveEvents = parseAbi(Object.values(troveEventSigs));
  const boldEvents = parseAbi(Object.values(BOLDEventSigs));
  
  const dailyStats = await postDailyUpdate(
    [ETH, WSTETH, RETH],
    BOLD_TOKEN_ADDRESS,
    dayStartBlock,
    currentBlock,
    troveEvents,
    boldEvents,
    spEvents
  );

  saveDailyStats(currentBlock, dailyStats);
}

// --- Script ---

async function main() {
  try {
    await pollForNewEvents();

    setInterval(async () => {
      // Catch interval error
        try {
          await pollForNewEvents();
        } catch (error) {
          console.error('Poll error:', error);
          await postToSlack(`Bot error: ${error.message}`);
        }
      }, 2 * 60 * MILLISECONDS);
    // Catch startup error
    } catch (error) {
      console.error('Error in main:', error);
      await postToSlack(`Bot startup error: ${error.message}`);
    }
  }

main().catch(async (error) => {
  console.error('Unhandled error:', error);
  await postToSlack(`Bot crashed: ${error.message}`);
});
