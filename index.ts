import axios from 'axios';
import dotenv from 'dotenv';
import { createPublicClient, http, parseAbi } from 'viem';
import { mainnet } from 'viem/chains';
import fs from 'fs';

const STATE_FILE = 'state.json';
const POST_TO_SLACK = true // toggle to only log to console for testing

// TODO: LTV, and get chainlink price for ops that need it
dotenv.config();

const BOLD_TOKEN_ADDRESS = "0x6440f144b7e50d6a8439336510312d2f54beb01d"
const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

const DEPLOYMENT_BLOCK = 22516078n
// --- First blocks with trove ops ---
const FIRST_OPEN_TROVE_BLOCK = 22516627n
const FIRST_ADJUST_TROVE_BLOCK = 22516819n
const FIRST_CLOSE_TROVE_BLOCK = 22517875n
const FIRST_ADJUST_TROVE_RATE_BLOCK = 22524725n
const FIRST_LIQUIDATION_BLOCK = 22525888n
const FIRST_REDEMPTION_BLOCK = 22516536n

const BLOCKS_IN_ONE_DAY = 3600n * 24n / 12n // 24 hrs, 12 seconds per block

const DECIMAL_PRECISION: bigint = 1_000_000_000_000_000_000n;
// const MIN_REDEMPTION: bigint = 50_000n * DECIMAL_PRECISION; // 50k at 18 decimal precision
const MIN_REDEMPTION: bigint = 0n;

// const MIN_LIQUIDATION: bigint = 50_000n * DECIMAL_PRECISION; 
const MIN_LIQUIDATION: bigint = 0n

interface Coll {
  label: string,
  troveManager: string,
  sp: string
}

const WETH: Coll = {
  label: "WETH",
  troveManager: "0x7bcb64b2c9206a5b699ed43363f6f98d4776cf5a",
  sp: "0x5721cbbd64fc7ae3ef44a0a3f9a790a9264cf9bf"
}

const WSTETH: Coll = {
    label: "WSTETH",
  troveManager: "0xa2895d6a3bf110561dfe4b71ca539d84e1928b22",
  sp: "0x9502b7c397e9aa22fe9db7ef7daf21cd2aebe56b"
}

const RETH: Coll = {
  label: "RETH",
  troveManager: "0xb2b2abeb5c357a234363ff5d180912d319e3e19e",
  sp: "0xd442e41019b7f5c4dd78f50dc03726c446148695"
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
  StabilityPoolCollBalanceUpdated: 'event StabilityPoolCollBalanceUpdated(uint256 _newBalance)',
  StabilityPoolBoldBalanceUpdated: 'event StabilityPoolBoldBalanceUpdated(uint256 _newBalance)',
  P_Updated: 'event P_Updated(uint256 _P)',
  S_Updated: 'event S_Updated(uint256 _S, uint256 _scale)',
  B_Updated: 'event B_Updated(uint256 _B, uint256 _scale)',
  ScaleUpdated: 'event ScaleUpdated(uint256 _currentScale)',
  DepositUpdated: 'event DepositUpdated(address indexed _depositor, uint256 _newDeposit, uint256 _stashedColl, uint256 _snapshotP, uint256 _snapshotS, uint256 _snapshotB, uint256 _snapshotScale)',
  DepositOperation: 'event DepositOperation(address indexed _depositor, uint8 _operation, uint256 _depositLossSinceLastOperation, int256 _topUpOrWithdrawal, uint256 _yieldGainSinceLastOperation, uint256 _yieldGainClaimed, uint256 _ethGainSinceLastOperation, uint256 _ethGainClaimed)'
};

// --- Chainlink data structures ---

type PricePoint = {
  block: bigint;
  price: bigint;
  roundId: bigint; // optional, for debugging
}

type PriceCache = {
  WETH: PricePoint[];
  WSTETH: PricePoint[];
  RETH: PricePoint[];
}

// --- Formatting functions ---

function formatNum(val: bigint): string {

const scaledVal = Number(val) / 1e18;

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
const client = createPublicClient({
  chain: mainnet,
  transport: http(), // Uses free Cloudflare endpoint, for 1000 block range
});

// TODO: Use Liquity AG's paid alchemy plan
// const client = createPublicClient({
//   chain: mainnet,
//   transport: http(`https://eth-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`),
// });

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

async function postOpenTrove(logs: any[], collLabel: string) {
  console.log(` --- openTroves ---`)
  const opened = logs.filter(log => 
    log.eventName === "TroveOperation" && 
    log.args['_operation'] === TroveOperations.OpenTrove
  )

  for (const trove of opened) {
    const debt = trove.args._debtChangeFromOperation + trove.args._debtIncreaseFromUpfrontFee;

    await postToSlack(`
      ${addCircles(debt)} *Trove opened* (${collLabel}) - Loan: ${formatNum(debt)}, Coll: ${formatNum(trove.args._collChangeFromOperation)}, Rate: ${percent(trove.args._annualInterestRate)} [${dateAndTime(trove.timestamp)}, ${txHashLink(trove)}]` 
    )
  }
}
// Trove closed: $41k, 32 wstETH, 2.70%, 235% [05:44, TX, Trove]

async function postClosedTrove(logs: any[], collLabel: string) {
  console.log(` --- closedTroves ---`)
  const closed = logs.filter(log => 
    log.eventName === "TroveOperation" && 
    log.args['_operation'] === TroveOperations.CloseTrove
  )

  for (const trove of closed) {
    await postToSlack(
      `${addCircles(trove.args._debtChangeFromOperation)} *Trove closed* (${collLabel}) - Debt: ${formatNum(trove.args._debtChangeFromOperation * -1n)}, Coll: ${formatNum(trove.args._collChangeFromOperation * -1n)} [${dateAndTime(trove.timestamp)}, ${txHashLink(trove)}]`
    )
  }
}

async function postAdjustTrove(logs: any[], collLabel: string): Promise<void> {
  const txHashToEvents = new Map<bigint, { adjusted: any, updated: any }>();
  
  // Catch each pair of (AdjustTrove, TroveUpdated) events with same Tx hash.
  // TODO: Add use case in post
  for (const log of logs) {
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
      const debtChange = adjusted.args._debtIncreaseFromUpfrontFee + adjusted.args._debtChangeFromOperation;
      const prevDebt = updated.args._debt + debtChange;
      
      const prevColl = updated.args._coll + adjusted.args._collChangeFromOperation;
      
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

      postToSlack(
        `${debtCircles} *Trove adjusted* (${collLabel}) - ${debtChangeStr}, ${collChangeStr}, Rate: ${percent(adjusted.args._annualInterestRate)} [${dateAndTime(adjusted.timestamp)}, ${txHashLink(adjusted)}]`
      )
    }
  }
}

async function postAdjustRate(logs: any[], collLabel: string): Promise<void> {
  console.log(` --- adjustRates ---`)
  const txHashToEvents = new Map<bigint, { adjusted: any, updated: any }>();
  
  // Pair TroveOperation (rate adjustment) with TroveUpdated events
  for (const log of logs) {
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
      const collAmount = formatNum(updated.args._coll);
      
      // Format interest rate (assuming it's in basis points or similar)
      const interestRate = percent(adjusted.args._annualInterestRate)
      
      await postToSlack(
        `*Rate Adjusted* (${collLabel}) - Loan: ${debt}, Coll: ${collAmount}, New rate: ${interestRate} [${dateAndTime(adjusted.timestamp)}, ${txHashLink(adjusted)}]`
      );
    }
  }
}

async function postBatchAdjustRate(logs: any[], collLabel: string): Promise<void> {
  console.log(` --- batchAdjustRate ---`)
  const batchAdjusted = logs.filter(log => 
  log.eventName === "BatchUpdated" &&
  log.args._operation == BatchOperations.setBatchManagerAnnualInterestRate)
  
  for (const log of batchAdjusted) {
    postToSlack( 
      `*${collLabel} Batch rate adjusted* (${collLabel}) - New rate: ${percent(log.args._annualInterestRate)}, Batch debt: ${formatNum(log.args._debt)} [${dateAndTime(log.timestamp)}, ${txHashLink(log)}, ${addressLink(log.args._interestBatchManager, "Manager")}]`
    )
  }    
};

async function postBatchAdjustFee(logs: any[], collLabel: string): Promise<void> {
  const batchAdjusted = logs.filter(log => 
  log.eventName === "BatchUpdated" &&
  log.args._operation == BatchOperations.lowerBatchManagerAnnualFee)
  
  for (const log of batchAdjusted) {
    postToSlack( 
      `${collLabel} *Batch fee lowered* (${collLabel}) - New fee: ${log.args._annualManagementFee}, Batch debt: ${log.args._debt} [${dateAndTime(log.timestamp)}, ${txHashLink(log)}, ${addressLink(log.args._interestBatchManager, "Manager")}]`
    )
  } 
};

async function postRedemption(logs: any[], collLabel: string): Promise<void> {
  const redemptions = logs.filter(log => 
    log.eventName === "Redemption" && 
    log.args._actualBoldAmount > MIN_REDEMPTION)

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

    postToSlack(` *Redemption* (${collLabel}) - Amount: ${formatNum(log.args._actualBoldAmount)}, Fee rate: ${(feePct)}, BOLD price: ${formatNum(effectiveBOLDPrice)} USD, ${collLabel} price: ${formatNum(log.args._redemptionPrice)} USD [${dateAndTime(log.timestamp)}, ${txHashLink(log)}]`
    )
  }
}

async function postLiquidation(logs: any[], collLabel: string): Promise<void> {
  const liqs = logs.filter(
    (log) =>
      log.eventName === "Liquidation"
    //  &&
    //   (log.args._debtOffsetBySP + log.args._debtRedistributed) > MIN_LIQUIDATION
  );

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
      `*Liquidation* ${dateAndTime(log.timestamp)} - Amount: ${formatNum(boldAmount)} ${collLabel}, price: ${formatNum(price)} USD ${txHashLink(log)}`
    );
  }
}

// --- Posting functions for all branches ---

async function postAllOpenTrove(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postOpenTrove(wethTroveLogs, 'ETH')
  await postOpenTrove(wstethTroveLogs, 'wstETH')
  await postOpenTrove(rethTroveLogs, 'rETH')
}

async function postAllClosedTrove(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postClosedTrove(wethTroveLogs, 'ETH')
  await postClosedTrove(wstethTroveLogs, 'wstETH')
  await postClosedTrove(rethTroveLogs, 'rETH')
}

async function postAllAdjustTrove(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postAdjustTrove(wethTroveLogs, 'ETH')
  await postAdjustTrove(wstethTroveLogs, 'wstETH')
  await postAdjustTrove(rethTroveLogs, 'rETH')
}

async function postAllAdjustRate(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postAdjustRate(wethTroveLogs, 'ETH')
  await postAdjustRate(wstethTroveLogs, 'wstETH')
  await postAdjustRate(rethTroveLogs, 'rETH')
}

async function postAllBatchAdjustRate(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postBatchAdjustRate(wethTroveLogs, 'ETH')
  await postBatchAdjustRate(wstethTroveLogs, 'wstETH')
  await postBatchAdjustRate(rethTroveLogs, 'rETH')
}

async function postAllBatchAdjustFee(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postBatchAdjustFee(wethTroveLogs, 'ETH')
  await postBatchAdjustFee(wstethTroveLogs, 'wstETH')
  await postBatchAdjustFee(rethTroveLogs, 'rETH')
}

async function postAllRedemption(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postRedemption(wethTroveLogs, 'ETH')
  await postRedemption(wstethTroveLogs, 'wstETH')
  await postRedemption(rethTroveLogs, 'rETH')
}

async function postAllLiquidation(wethTroveLogs: any[], wstethTroveLogs: any[], rethTroveLogs: any[]) {
  await postLiquidation(wethTroveLogs, 'ETH')
  await postLiquidation(wstethTroveLogs, 'wstETH')
  await postLiquidation(rethTroveLogs, 'rETH')
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
  interval: bigint = 1000n
): Promise<NetTotals>  {
  let total = 0n;

  let troveLogs: any[] = []
  let boldLogs: any[] = []
  const spLogs = {
    "WETH": [],
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
          toBlock: from + span
        }))
      )

      spLogs[coll.label].push(...
        (await client.getLogs({
        address: coll.sp,
          events: spEvents,
          fromBlock: from,
          toBlock: from + span
        }))
      )
    }

      boldLogs.push(...
        (await client.getLogs({
        address: boldTokenAddress,
        events: boldEvents,
        fromBlock: from,
        toBlock: from + span
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

  const dayStartTime = Number((await client.getBlock({blockNumber: dayStartBlock})).timestamp) * 1000 // miliseconds

  const state = loadBigintState();

  const netTotals = {
    netMinted: minted - repaid - liquidated - redeemed,
    netWethSP: wethTotals.deposits - wethTotals.withdrawals,
    netWstethSP: wstethTotals.deposits - wstethTotals.withdrawals,
    netRethSP: rethTotals.deposits - rethTotals.withdrawals
  }
 
  postToSlack(
    `*Daily stats* - ${dateAndTime(dayStartTime)}.\n

    *BOLD*
    Minted: ${formatNum(minted)}  
    Repaid: ${formatNum(repaid)}  
    Redeemed: ${formatNum(redeemed)}  
    Liquidated: ${formatNum(liquidated)} 
    Net total: ${formatNum(state.lastNetMinted)} => ${formatNum(netTotals.netMinted)}
    \n

    *Troves*
    Troves opened: ${counts.opened} 
    Troves adjusted: ${counts.adjusted} 
    Troves closed: ${counts.closed} 
    Rate adjustments: ${counts.rateAdjusted} 
    Batch adjustments: ${counts.rateAdjusted}
    \n
    
    *SP*
    Deposits: ${formatNum(wethTotals.deposits + wstethTotals.deposits + rethTotals.deposits )}
    SP Withdrawals: ${formatNum(wethTotals.withdrawals + wstethTotals.withdrawals + rethTotals.withdrawals)}
    Net Totals: ${formatNum(netTotals.netWethSP + netTotals.netWstethSP + netTotals.netRethSP)}`
   )

    //  ETH SP: Deposits ${formatNum(wethTotals.deposits)} (${wethTotals.depositsCount}) | Withdrawals: ${formatNum(wethTotals.withdrawals)} (${wethTotals.withdrawalsCount}) 
    // Net total: ${formatNum(state.lastNetWethSP)} => ${formatNum(netTotals.netWethSP)} \n
    // WSTETH SP: Deposits ${formatNum(wstethTotals.deposits)} (${wstethTotals.depositsCount}) | Withdrawals: ${formatNum(wstethTotals.withdrawals)} (${wstethTotals.withdrawalsCount}) 
    // Net total: ${formatNum(state.lastNetWstethSP)} => ${formatNum(netTotals.netWstethSP)} \n
    // RETH SP: Deposits ${formatNum(rethTotals.deposits)} (${rethTotals.depositsCount}) | Withdrawals: ${formatNum(rethTotals.withdrawals)} (${rethTotals.withdrawalsCount}) 
    // Net total: ${formatNum(state.lastNetRethSP)} => ${formatNum(netTotals.netRethSP)} \n

   return netTotals;
}

interface SPTotals{
  deposits: bigint
  depositsCount: number
  withdrawals: bigint
  withdrawalsCount: number
}

function makeSPTotals(): SPTotals {
  return {
    deposits: 0n,
    depositsCount: 0,
    withdrawals: 0n,
    withdrawalsCount: 0
  }
}

function getSPTotals(spLogs: Record<string, any[]>): SPTotals[]  {
  const colls = {
    "WETH": makeSPTotals(),
    "WSTETH": makeSPTotals(),
    "RETH": makeSPTotals()
  }

  for (const coll of Object.keys(spLogs)) {
    for (const log of spLogs[coll]) {
      if (log.eventName === "DepositOperation") {
        if (log.args._operation === DepositOperations.provideToSP) {
          colls[coll].deposits += log.args._topUpOrWithdrawal
          colls[coll].depositsCount += 1
        } else if(log.args._operation === DepositOperations.withdrawFromSP) {
          colls[coll].withdrawals -= log.args._topUpOrWithdrawal // withdrawals are negative ints
          colls[coll].withdrawalsCount += 1
        }
      }
    }
  }

  return [colls.WETH, colls.WSTETH, colls.RETH];
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

  for (const log of troveLogs) {
    if (log.eventName === "TroveOperation") {
      if (log.args._operation === TroveOperations.OpenTrove) {counts.opened += 1}
      else if (log.args._operation === TroveOperations.AdjustTrove) {counts.adjusted += 1}
      else if (log.args._operation === TroveOperations.CloseTrove) {counts.closed += 1}
      else if (log.args._operation === TroveOperations.AdjustTroveInterestRate) {counts.rateAdjusted += 1}
    }
  
    if (log.eventName == "BatchUpdated" && log.args._operation == BatchOperations.lowerBatchManagerAnnualFee) {
      counts.batchRateAdjusted += 1
    }
  }

  return counts;
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
      address: WETH.troveManager, 
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

function loadState() {
  const data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  return data;
}

function loadBigintState() {
  const data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  return Object.fromEntries(Object.entries(data).map(([k, v]) => [k, BigInt(v)]));
}

function savePolledBlock(pollBlock: bigint): void {
  const state = loadState();
  state.lastPolledBlock = pollBlock.toString();
  fs.writeFileSync(STATE_FILE, JSON.stringify(state));
}

function saveDailyStats(dailyBlock: bigint, netTotals: NetTotals): void {
  const state = loadState();
  state.lastDailyBlock = dailyBlock.toString();
  state.lastNetMinted = netTotals.netMinted.toString();
  state.lastNetWethSP = netTotals.netWethSP.toString();
  state.lastNetWstethSP = netTotals.netWstethSP.toString();
  state.lastNetRethSP = netTotals.netRethSP.toString();

  fs.writeFileSync(STATE_FILE, JSON.stringify(state));
}


async function pollForNewEvents() {
  const state = loadBigintState();
  const currentBlock = await client.getBlockNumber();
  
  // Only get the past day's update, even if we're >1 day out of date
  if (currentBlock - state.lastDailyBlock > BLOCKS_IN_ONE_DAY) {
    await dailyUpdate();
  }

  // Individual events. Limit to 1 day's worth of blocks per poll
  const fromBlock = state.lastPolledBlock + 1n;
  const maxToBlock = fromBlock + BLOCKS_IN_ONE_DAY - 1n;
  const toBlock = currentBlock < maxToBlock ? currentBlock : maxToBlock;
  const interval = 1000n;

  let wethTroveLogs: any[] = [];
  let wstethTroveLogs:  any[] = [];
  let rethTroveLogs:  any[] = [];
  
  // Fetch in 1000-block chunks
  const troveEvents = parseAbi(Object.values(troveEventSigs));
  for (let from = fromBlock; from <= toBlock; ) {
    console.log("fetch events step")
    const span = (from + interval - 1n <= toBlock) ? interval : (toBlock - from + 1n);
    
    const wethLogsChunk = await client.getLogs({
      address: WETH.troveManager, 
      events: troveEvents, 
      fromBlock: from, 
      toBlock: from + span
    })
    await addTimestamps(wethLogsChunk);
    wethTroveLogs.push(...wethLogsChunk);

    const wstethLogsChunk = await client.getLogs({
      address: WSTETH.troveManager, 
      events: troveEvents, 
      fromBlock: from, 
      toBlock: from + span
    })
    await addTimestamps(wstethLogsChunk);
    wstethTroveLogs.push(...wstethLogsChunk);

    const rethLogsChunk = await client.getLogs({
      address: RETH.troveManager, 
      events: troveEvents, 
      fromBlock: from, 
      toBlock: from + span
    })
    await addTimestamps(rethLogsChunk);
    rethTroveLogs.push(...rethLogsChunk);
    
    from += span;
  }
  
  await postAllOpenTrove(wethTroveLogs, wstethTroveLogs, rethTroveLogs);
  await postAllAdjustTrove(wethTroveLogs, wstethTroveLogs, rethTroveLogs);
  await postAllClosedTrove(wethTroveLogs, wstethTroveLogs, rethTroveLogs);
  await postAllAdjustRate(wethTroveLogs, wstethTroveLogs, rethTroveLogs);
  await postAllBatchAdjustRate(wethTroveLogs, wstethTroveLogs, rethTroveLogs);
  await postAllBatchAdjustFee(wethTroveLogs, wstethTroveLogs, rethTroveLogs);
  await postAllRedemption(wethTroveLogs, wstethTroveLogs, rethTroveLogs);
  await postAllLiquidation(wethTroveLogs, wstethTroveLogs, rethTroveLogs);

  savePolledBlock(toBlock);
}

async function addTimestamps(logs: any[]): Promise<void> {
  for (const log of logs) {
    const block = await client.getBlock({ blockNumber: log.blockNumber });
    log.timestamp = Number(block.timestamp) * 1000; // Convert to milliseconds
  }
}

interface NetTotals {
  netMinted: bigint,
  netWethSP: bigint,
  netWstethSP: bigint,
  netRethSP: bigint
}

async function dailyUpdate() {
  const currentBlock = await client.getBlockNumber();
  const dayStartBlock = currentBlock - BLOCKS_IN_ONE_DAY;
  
  const spEvents = parseAbi(Object.values(spEventSigs));
  const troveEvents = parseAbi(Object.values(troveEventSigs));
  const boldEvents = parseAbi(Object.values(BOLDEventSigs));
  
  const netTotals = await postDailyUpdate(
    [WETH, WSTETH, RETH],
    BOLD_TOKEN_ADDRESS,
    dayStartBlock,
    currentBlock,
    troveEvents,
    boldEvents,
    spEvents
  );

  saveDailyStats(currentBlock, netTotals);
}

// --- Script ---

async function main() {
  try {
    const latestBlock: bigint = await client.getBlockNumber();
    const ONE_DAY_AGO: bigint = latestBlock - BLOCKS_IN_ONE_DAY

    const fromBlock = FIRST_OPEN_TROVE_BLOCK;
    const toBlock =  fromBlock + 999n 
    
    // const troveEvents = parseAbi(Object.values(troveEventSigs))
    // const wethTroveLogs = await client.getLogs({
    //   address: WETH.troveManager, 
    //   events: troveEvents, 
    //   fromBlock: fromBlock, 
    //   toBlock: toBlock
    // });
    // await addTimestamps(wethTroveLogs);
    //   // await postOpenTrove(wethTroveLogs, 'WETH')
    // setInterval(pollForNewEvents, 60 * 10 * 1000);
  pollForNewEvents()
  } catch (error) {
    console.error('Error in main:', error);
    await postToSlack(`Bot startup error: ${error.message}`);
  }
}

main().catch(async (error) => {
  console.error('Unhandled error:', error);
  await postToSlack(`Bot crashed: ${error.message}`);
});