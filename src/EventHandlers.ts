/*
 * Please refer to https://docs.envio.dev for a thorough guide on all Envio indexer features
 */
import {
  Diesel,
  Diesel_type2 as PoolId,
  Diesel_type8 as Identity,
  handlerContext as Context,
  Transaction,
  Pool,
  Asset,
} from "generated";
import { v4 as uuid } from "uuid";
import BN from "bn.js";
import axios from "axios";

type IdentityIsContract = [string, boolean];

const ONE_E_18 = new BN(10).pow(new BN(18));
const BASIS_POINTS = BigInt(10000);
const AMM_FEES: AmmFees = {
  lpFeeVolatile: BigInt(30),
  lpFeeStable: BigInt(5),
  protocolFeeStable: BigInt(0),
  protocolFeeVolatile: BigInt(0),
};

function roundingUpDivision(nominator: bigint, denominator: bigint): bigint {
  let roundingDownDivisionResult = nominator / denominator;
  if (nominator % denominator === BigInt(0)) {
    return roundingDownDivisionResult;
  } else {
    return roundingDownDivisionResult + BigInt(1);
  }
}

interface ExtraEvent {
  pool_id: string;
  transaction_type: string;
  lp_id: string | undefined;
  lp_amount: string | undefined;
  asset_0_in: string;
  asset_0_out: string;
  asset_1_in: string;
  asset_1_out: string;
}

function extract(transaction: Transaction): ExtraEvent {
  return {
    lp_amount: transaction.lp_amount?.toString(),
    lp_id: transaction.lp_id,
    pool_id: transaction.pool_id,
    transaction_type: transaction.transaction_type,
    asset_0_in: transaction.asset_0_in.toString(),
    asset_0_out: transaction.asset_0_out.toString(),
    asset_1_in: transaction.asset_1_in.toString(),
    asset_1_out: transaction.asset_1_out.toString(),
  };
}

function poolIdToStr(poolId: PoolId): string {
  return `${poolId[0].bits}_${poolId[1].bits}_${poolId[2]}`;
}

function identityToStr(identity: Identity): IdentityIsContract {
  switch (identity.case) {
    case "Address":
      return [identity.payload.bits, false];
    case "ContractId":
      return [identity.payload.bits, true];
  }
}

function powDecimals(decimals: number): BN {
  return new BN(10).pow(new BN(decimals));
}

interface AmmFees {
  lpFeeVolatile: bigint;
  lpFeeStable: bigint;
  protocolFeeVolatile: bigint;
  protocolFeeStable: bigint;
}

function k(
  isStable: boolean,
  x: BN,
  y: BN,
  powDecimalsX: BN,
  powDecimalsY: BN
): BN {
  if (isStable) {
    const _x: BN = x.mul(ONE_E_18).div(powDecimalsX);
    const _y: BN = y.mul(ONE_E_18).div(powDecimalsY);
    const _a: BN = _x.mul(_y).div(ONE_E_18);
    const _b: BN = _x.mul(_x).div(ONE_E_18).add(_y.mul(_y).div(ONE_E_18));
    return _a.mul(_b).div(ONE_E_18); // x3y+y3x >= k
  } else {
    return x.mul(y); // xy >= k
  }
}

function kPool(pool: Pool): BN {
  return k(
    pool.is_stable,
    new BN(pool.reserve_0.toString()),
    new BN(pool.reserve_1.toString()),
    powDecimals(pool.decimals_0),
    powDecimals(pool.decimals_1)
  );
}

function calculateFee(
  poolId: PoolId,
  amount: bigint,
  ammFees: AmmFees
): bigint {
  const feeBP = poolId[2]
    ? ammFees.lpFeeStable + ammFees.protocolFeeStable
    : ammFees.lpFeeVolatile + ammFees.protocolFeeVolatile;
  const nominator = amount * feeBP;
  return roundingUpDivision(nominator, BASIS_POINTS);
}

async function upsertTransaction(context: Context, transaction: Transaction) {
  const oldTransaction = await context.Transaction.get(transaction.id);
  if (oldTransaction === undefined) {
    context.Transaction.set(transaction);
  } else {
    const extra: ExtraEvent[] = JSON.parse(oldTransaction.extra ?? "[]");
    extra.push(extract(transaction));
    const enrichedTransaction = {
      ...oldTransaction,
      initiator: transaction.initiator,
      extra: JSON.stringify(extra),
    };
    context.Transaction.set(enrichedTransaction);
  }
}

const shouldReturnEarlyDueToDuplicate = async (
  duplicateId: string,
  context: Context
) => {
  const deduplicator = await context.DeDuplicator.get(duplicateId);
  if (deduplicator === undefined) {
    context.DeDuplicator.set({ id: duplicateId, additionalDuplications: 0 });
    return false;
  } else {
    // Return Early
    context.DeDuplicator.set({
      ...deduplicator,
      additionalDuplications: deduplicator.additionalDuplications + 1,
    });
    return true;
  }
};

const USDC_ID =
  "0x286c479da40dc953bddc3bb4c453b608bba2e0ac483b077bd475174115395e6b";
let ETH_ID =
  "0xf8f8b6283d7fa5b672b530cbb84fcccb4ff8dc40f8176ef4544ddb1f1952ad07";
let FUEL_ID =
  "0x1d5d97005e41cae2187a895fd8eab0506111e0e2f3331cd3912c15c24e3c1d82";
let USDC_PRICE_USD = 1;
let ETH_PRICE_USD = 3364.34;
let FUEL_PRICE_USD = 0.067197;

function toDecimal(bnValue: BN, decimals: number) {
  const divisor = new BN(10).pow(new BN(decimals));
  const integerPart = bnValue.div(divisor).toString(); // Integer part
  const fractionalPart = bnValue
    .mod(divisor)
    .toString()
    .padStart(decimals, "0"); // Fractional part
  return `${integerPart}.${fractionalPart}`.replace(/\.?0+$/, ""); // Trim trailing zeroes
}

const toDecimalNum = (amount: number, decimals: number): number => {
  return amount / Math.pow(10, decimals);
};

setTimeout(async function () {
  try {
    const url =
      "https://coins.llama.fi/prices/current/fuel:0x286c479da40dc953bddc3bb4c453b608bba2e0ac483b077bd475174115395e6b,fuel:0xf8f8b6283d7fa5b672b530cbb84fcccb4ff8dc40f8176ef4544ddb1f1952ad07,fuel:0x1d5d97005e41cae2187a895fd8eab0506111e0e2f3331cd3912c15c24e3c1d82";

    const res = await axios.get(url);

    USDC_PRICE_USD =
      res.data.coins[
        "fuel:0x286c479da40dc953bddc3bb4c453b608bba2e0ac483b077bd475174115395e6b"
      ].price;

    ETH_PRICE_USD =
      res.data.coins[
        "fuel:0xf8f8b6283d7fa5b672b530cbb84fcccb4ff8dc40f8176ef4544ddb1f1952ad07"
      ].price;

    FUEL_PRICE_USD =
      res.data.coins[
        "fuel:0x1d5d97005e41cae2187a895fd8eab0506111e0e2f3331cd3912c15c24e3c1d82"
      ].price;
  } catch (error) {
    console.error("Error fetching data", error);
  }
}, 30000);

// function setAsset(context: Context, id0: string, id1: string, exchange_rate: number) {
//     if (id1 === USDC_ID) {
//         context.Asset.set({
//             id: id0,
//             price_usd: exchange_rate * USDC_PRICE_USD,
//             exchange_rate
//         })
//     } else if (id1 === ETH_ID) {
//         context.Asset.set({
//             id: id0,
//             price_usd: exchange_rate * ETH_PRICE_USD,
//             exchange_rate
//         })
//     }
// }

const calcVolume = (numerator: number, denominator: number, PRICE: number) => {
  const amt = numerator / denominator;
  const totalVol = amt * 2 * PRICE;
  return totalVol;
};

function getVolume(event: any, pool: Pool): number {
  if (event.params.asset_0_in > 0n || event.params.asset_1_out > 0n) {
    if (pool.asset_0 === ETH_ID) {
      return calcVolume(
        Number(event.params.asset_0_in),
        10 ** pool.decimals_0,
        ETH_PRICE_USD
      );
    } else if (pool.asset_1 === ETH_ID) {
      return calcVolume(
        Number(event.params.asset_1_out),
        10 ** pool.decimals_1,
        ETH_PRICE_USD
      );
    } else if (pool.asset_0 === USDC_ID) {
      return calcVolume(
        Number(event.params.asset_0_in),
        10 ** pool.decimals_0,
        USDC_PRICE_USD
      );
    } else if (pool.asset_1 === USDC_ID) {
      return calcVolume(
        Number(event.params.asset_1_out),
        10 ** pool.decimals_1,
        USDC_PRICE_USD
      );
    } else if (pool.asset_0 === FUEL_ID) {
      return calcVolume(
        Number(event.params.asset_0_in),
        10 ** pool.decimals_0,
        FUEL_PRICE_USD
      );
    } else if (pool.asset_1 === FUEL_ID) {
      return calcVolume(
        Number(event.params.asset_1_out),
        10 ** pool.decimals_1,
        FUEL_PRICE_USD
      );
    }
  } else if (event.params.asset_1_in > 0n || event.params.asset_0_out > 0n) {
    if (pool.asset_0 === ETH_ID) {
      return calcVolume(
        Number(event.params.asset_0_out),
        10 ** pool.decimals_0,
        ETH_PRICE_USD
      );
    } else if (pool.asset_1 === ETH_ID) {
      return calcVolume(
        Number(event.params.asset_1_in),
        10 ** pool.decimals_1,
        ETH_PRICE_USD
      );
    } else if (pool.asset_0 === USDC_ID) {
      return calcVolume(
        Number(event.params.asset_0_out),
        10 ** pool.decimals_0,
        USDC_PRICE_USD
      );
    } else if (pool.asset_1 === USDC_ID) {
      return calcVolume(
        Number(event.params.asset_1_in),
        10 ** pool.decimals_1,
        USDC_PRICE_USD
      );
    } else if (pool.asset_0 === FUEL_ID) {
      return calcVolume(
        Number(event.params.asset_0_out),
        10 ** pool.decimals_0,
        FUEL_PRICE_USD
      );
    } else if (pool.asset_1 === FUEL_ID) {
      return calcVolume(
        Number(event.params.asset_1_in),
        10 ** pool.decimals_1,
        FUEL_PRICE_USD
      );
    }
  }
  return 0;
}

async function calculatePoolTVL(
  context: Context,
  pool: Pool,
  reserve0: bigint,
  reserve1: bigint
): Promise<{ tvl: bigint; tvlUSD: number }> {
  const tvl = reserve0 + reserve1;

  // For ETH pairs
  if (pool.asset_0 === ETH_ID || pool.asset_1 === ETH_ID) {
    if (pool.asset_0 === ETH_ID) {
      // ETH is token0
      // const ethAmt = Number(toDecimal(new BN(pool.reserve_0.toString()), pool.decimals_0))
      const ethAmt =
        (Number(pool.reserve_0) / 10 ** pool.decimals_0) * ETH_PRICE_USD;
      console.log("Pool: ", pool.id);

      console.log("ETH amt USD", ethAmt);

      const tvlUSD = ethAmt * 2;
      console.log("TVL USD", tvlUSD);

      return { tvl, tvlUSD: tvlUSD };
    } else {
      // ETH is token1
      //const ethAmt = Number(toDecimal(new BN(pool.reserve_1.toString()), pool.decimals_1))
      const ethAmt =
        (Number(pool.reserve_1) / 10 ** pool.decimals_1) * ETH_PRICE_USD;
      console.log("Pool: ", pool.id);
      console.log("ETH amt USD", ethAmt);
      console.log(pool.reserve_1.toString());
      const tvlUSD = ethAmt * 2;
      console.log("TVL USD", tvlUSD);

      return { tvl, tvlUSD: tvlUSD };
    }
  }

  // For USDC pairs
  if (pool.asset_0 === USDC_ID || pool.asset_1 === USDC_ID) {
    if (pool.asset_0 === USDC_ID) {
      // USDC is token0
      const usdcAmt = Number(
        toDecimal(new BN(pool.reserve_0.toString()), pool.decimals_0)
      );
      const tvlUSD = usdcAmt * 2 * USDC_PRICE_USD;

      return { tvl, tvlUSD: tvlUSD };
    } else {
      // USDC is token1
      const usdcAmt = Number(
        toDecimal(new BN(pool.reserve_1.toString()), pool.decimals_1)
      );
      const tvlUSD = usdcAmt * 2 * USDC_PRICE_USD;

      return { tvl, tvlUSD: tvlUSD };
    }
  }

  // For Fuel Pairs
  if (pool.asset_0 === FUEL_ID || pool.asset_1 === FUEL_ID) {
    // Continue here
    if (pool.asset_0 === FUEL_ID) {
      // FUEL is token0
      const fuelAmt = Number(
        toDecimal(new BN(pool.reserve_0.toString()), pool.decimals_0)
      );
      const tvlUSD = fuelAmt * 2 * FUEL_PRICE_USD;

      return { tvl, tvlUSD: tvlUSD };
    } else {
      // FUEL is token1
      const fuelAmt = Number(
        toDecimal(new BN(pool.reserve_1.toString()), pool.decimals_1)
      );
      const tvlUSD = fuelAmt * 2 * FUEL_PRICE_USD;

      return { tvl, tvlUSD: tvlUSD };
    }
  }

  return { tvl, tvlUSD: 0 };
}

Diesel.CreatePoolEvent.handler(async ({ event, context }) => {
  // Save a raw event
  const id = `${event.logIndex}_${event.transaction.id}_${event.block.height}`;
  const rawEvent = {
    id: id,
    pool_id: poolIdToStr(event.params.pool_id),
    decimals_0: event.params.decimals_0,
    decimals_1: event.params.decimals_1,
    logIndex: event.logIndex,
    transactionId: event.transaction.id,
    blockId: event.block.id,
    blockHeight: event.block.height,
  };
  context.RawCreatePoolEvent.set(rawEvent);

  context.log.info(
    `Handling CreatePoolEvent for pool ID: ${poolIdToStr(event.params.pool_id)}`
  );

  if (await shouldReturnEarlyDueToDuplicate(id, context)) {
    context.log.error(
      `Return Early due to duplicate in CreatePoolEvent - tx: ${event.transaction.id}`
    );
    return;
  }

  const pool = {
    id: poolIdToStr(event.params.pool_id),
    asset_0: event.params.pool_id[0].bits,
    asset_1: event.params.pool_id[1].bits,
    is_stable: event.params.pool_id[2],
    reserve_0: 0n,
    reserve_1: 0n,
    create_time: event.block.time,
    decimals_0: event.params.decimals_0,
    decimals_1: event.params.decimals_1,
    tvl: 0n,
    tvlUSD: 0,
    swapVolume: 0,
    lpId: "n-a",
  };
  context.Pool.set(pool);

  const Asset0 = {
    id: event.params.pool_id[0].bits,
    exchange_rate_usdc: 0,
    exchange_rate_eth: 0,
    exchange_rate_fuel: 0,
  };

  const Asset1 = {
    id: event.params.pool_id[1].bits,
    exchange_rate_usdc: 0,
    exchange_rate_eth: 0,
    exchange_rate_fuel: 0,
  };

  console.log(event.params.pool_id[0].bits, event.params.pool_id[1].bits);

  context.Asset.set(Asset0);

  context.Asset.set(Asset1);
});

Diesel.MintEvent.handler(async ({ event, context }) => {
  // Save a raw event
  const id = `${event.logIndex}_${event.transaction.id}_${event.block.height}`;
  const rawEvent = {
    id: id,
    pool_id: poolIdToStr(event.params.pool_id),
    recipient: identityToStr(event.params.recipient)[0],
    liquidity: event.params.liquidity.id.bits,
    asset_0_in: event.params.asset_0_in,
    asset_1_in: event.params.asset_1_in,
    logIndex: event.logIndex,
    transactionId: event.transaction.id,
    blockId: event.block.id,
    blockHeight: event.block.height,
  };
  context.RawMintEvent.set(rawEvent);

  context.log.info(
    `Handling MintEvent for transaction ID: ${event.transaction.id}`
  );

  if (await shouldReturnEarlyDueToDuplicate(id, context)) {
    context.log.error(
      `Return Early due to duplicate in MintEvent - tx: ${event.transaction.id}`
    );
    return;
  }

  const poolId = poolIdToStr(event.params.pool_id);
  const pool = await context.Pool.get(poolId);
  if (pool === undefined) {
    context.log.error(`Pool ${poolId} not found but received MintEvent`);
    return;
  }

  const new_reserve_0 = (pool?.reserve_0 ?? 0n) + event.params.asset_0_in;
  const new_reserve_1 = (pool?.reserve_1 ?? 0n) + event.params.asset_1_in;

  const { tvl, tvlUSD } = await calculatePoolTVL(
    context,
    pool,
    new_reserve_0,
    new_reserve_1
  );

  context.Pool.set({
    id: poolId,
    asset_0: event.params.pool_id[0].bits,
    asset_1: event.params.pool_id[1].bits,
    is_stable: event.params.pool_id[2],
    reserve_0: (pool?.reserve_0 ?? 0n) + event.params.asset_0_in,
    reserve_1: (pool?.reserve_1 ?? 0n) + event.params.asset_1_in,
    create_time: pool?.create_time ?? event.block.time,
    decimals_0: pool?.decimals_0,
    decimals_1: pool?.decimals_1,
    tvl: tvl,
    tvlUSD: tvlUSD ? tvlUSD : 0,
    lpId: event.params.liquidity.id.bits ?? "n-a",
    swapVolume: pool?.swapVolume,
  });

  const [address, isContract] = identityToStr(event.params.recipient);
  const transaction: Transaction = {
    id: event.transaction.id,
    transaction_type: "ADD_LIQUIDITY",
    pool_id: poolId,
    initiator: address,
    is_contract_initiator: isContract,
    asset_0_in: event.params.asset_0_in,
    asset_0_out: 0n,
    asset_1_in: event.params.asset_1_in,
    asset_1_out: 0n,
    block_time: event.block.time,
    lp_id: event.params.liquidity.id.bits,
    lp_amount: event.params.liquidity.amount,
    extra: undefined,
  };
  await upsertTransaction(context, transaction);
});

Diesel.BurnEvent.handler(async ({ event, context }) => {
  // Save a raw event
  const id = `${event.logIndex}_${event.transaction.id}_${event.block.height}`;
  const rawEvent = {
    id: id,
    pool_id: poolIdToStr(event.params.pool_id),
    recipient: identityToStr(event.params.recipient)[0],
    liquidity: event.params.liquidity.id.bits,
    asset_0_out: event.params.asset_0_out,
    asset_1_out: event.params.asset_1_out,
    logIndex: event.logIndex,
    transactionId: event.transaction.id,
    blockId: event.block.id,
    blockHeight: event.block.height,
  };
  context.RawBurnEvent.set(rawEvent);

  context.log.info(
    `Handling BurnEvent for transaction ID: ${event.transaction.id}`
  );

  if (await shouldReturnEarlyDueToDuplicate(id, context)) {
    context.log.error(
      `Return Early due to duplicate in BurnEvent - tx: ${event.transaction.id}`
    );
    return;
  }

  const poolId = poolIdToStr(event.params.pool_id);
  const pool = await context.Pool.get(poolId);
  if (pool === undefined) {
    context.log.error(`Pool ${poolId} not found but received BurnEvent`);
    return;
  }

  const new_reserve_0 = (pool?.reserve_0 ?? 0n) - event.params.asset_0_out;
  const new_reserve_1 = (pool?.reserve_1 ?? 0n) - event.params.asset_1_out;

  const { tvl, tvlUSD } = await calculatePoolTVL(
    context,
    pool,
    new_reserve_0,
    new_reserve_1
  );

  console.log(poolId, tvlUSD);

  context.Pool.set({
    id: poolId,
    asset_0: event.params.pool_id[0].bits,
    asset_1: event.params.pool_id[1].bits,
    is_stable: event.params.pool_id[2],
    reserve_0: (pool?.reserve_0 ?? 0n) - event.params.asset_0_out,
    reserve_1: (pool?.reserve_1 ?? 0n) - event.params.asset_1_out,
    create_time: pool?.create_time ?? event.block.time,
    decimals_0: pool?.decimals_0,
    decimals_1: pool?.decimals_1,
    tvl: tvl,
    tvlUSD: tvlUSD ? tvlUSD : 0,
    lpId: event.params.liquidity.id.bits ?? "n-a",
    swapVolume: pool?.swapVolume,
  });
  const [address, isContract] = identityToStr(event.params.recipient);
  const transaction: Transaction = {
    id: event.transaction.id,
    pool_id: poolId,
    transaction_type: "REMOVE_LIQUIDITY",
    initiator: address,
    is_contract_initiator: isContract,
    asset_0_in: 0n,
    asset_0_out: event.params.asset_0_out,
    asset_1_in: 0n,
    asset_1_out: event.params.asset_1_out,
    block_time: event.block.time,
    lp_id: event.params.liquidity.id.bits,
    lp_amount: event.params.liquidity.amount,
    extra: undefined,
  };
  await upsertTransaction(context, transaction);
});

const setAssetExchangeRate = (
  pool: Pool,
  asset: Asset | undefined,
  context: Context,
  exchange_rate_usdc: number | null,
  exchange_rate_eth: number | null,
  exchange_rate_fuel: number | null
) => {
  context.Asset.set({
    id: pool.asset_1,
    exchange_rate_usdc: exchange_rate_usdc || asset?.exchange_rate_usdc || 0,
    exchange_rate_eth: exchange_rate_eth || asset?.exchange_rate_eth || 0,
    exchange_rate_fuel: exchange_rate_fuel || asset?.exchange_rate_fuel || 0,
  });
};

const setExchangeRate = async (
  Asset0: number,
  Asset1: number,
  pool: Pool,
  context: Context
) => {
  if (pool.asset_0 === USDC_ID) {
    const asset = await context.Asset.get(pool.asset_1);
    const exchange_rate_usdc = Asset0 / Asset1;

    setAssetExchangeRate(pool, asset, context, exchange_rate_usdc, null, null);
  } else if (pool.asset_1 === USDC_ID) {
    const asset = await context.Asset.get(pool.asset_0);
    const exchange_rate_usdc = Asset1 / Asset0;

    setAssetExchangeRate(pool, asset, context, exchange_rate_usdc, null, null);
  }

  if (pool.asset_0 === ETH_ID) {
    const asset = await context.Asset.get(pool.asset_1);
    const exchange_rate_eth = Asset0 / Asset1;

    setAssetExchangeRate(pool, asset, context, null, exchange_rate_eth, null);
  } else if (pool.asset_1 === ETH_ID) {
    const asset = await context.Asset.get(pool.asset_0);
    const exchange_rate_eth = Asset1 / Asset0;

    setAssetExchangeRate(pool, asset, context, null, exchange_rate_eth, null);
  }

  if (pool.asset_0 === FUEL_ID) {
    const asset = await context.Asset.get(pool.asset_1);
    const exchange_rate_fuel = Asset0 / Asset1;

    setAssetExchangeRate(pool, asset, context, null, null, exchange_rate_fuel);
  } else if (pool.asset_1 === FUEL_ID) {
    const asset = await context.Asset.get(pool.asset_0);
    const exchange_rate_fuel = Asset1 / Asset0;

    setAssetExchangeRate(pool, asset, context, null, null, exchange_rate_fuel);
  }
};

Diesel.SwapEvent.handler(async ({ event, context }) => {
  const id = `${event.logIndex}_${event.transaction.id}_${event.block.height}`;
  const rawEvent = {
    id: id,
    pool_id: poolIdToStr(event.params.pool_id),
    recipient: identityToStr(event.params.recipient)[0],
    asset_0_in: event.params.asset_0_in,
    asset_1_in: event.params.asset_1_in,
    asset_0_out: event.params.asset_0_out,
    asset_1_out: event.params.asset_1_out,
    logIndex: event.logIndex,
    transactionId: event.transaction.id,
    blockId: event.block.id,
    blockHeight: event.block.height,
  };
  context.RawSwapEvent.set(rawEvent);

  context.log.info(
    `Handling SwapEvent for transaction ID: ${event.transaction.id}`
  );

  if (await shouldReturnEarlyDueToDuplicate(id, context)) {
    context.log.error(
      `Return Early due to duplicate in SwapEvent - tx: ${event.transaction.id}`
    );
    return;
  }

  const poolId = poolIdToStr(event.params.pool_id);

  const dailyTimestamp =
    new Date(event.block.time * 1000).setHours(0, 0, 0, 0) / 1000;
  const dailySnapshotId = `${poolId}_${dailyTimestamp}`;

  const hourlyTimestamp =
    new Date(event.block.time * 1000).setMinutes(0, 0, 0) / 1000;
  const hourlySnapshotId = `${poolId}_${hourlyTimestamp}`;

  const [pool, dailySnapshot, hourlySnapshot] = await Promise.all([
    context.Pool.get(poolId),
    context.SwapDaily.get(dailySnapshotId),
    context.SwapHourly.get(hourlySnapshotId),
  ]);

  if (pool === undefined) {
    context.log.error(`Pool ${poolId} not found but received SwapEvent`);
    return;
  }

  const new_reserve_0 =
    (pool?.reserve_0 ?? 0n) +
    event.params.asset_0_in -
    event.params.asset_0_out;
  const new_reserve_1 =
    (pool?.reserve_1 ?? 0n) +
    event.params.asset_1_in -
    event.params.asset_1_out;

  if (event.params.asset_0_in > 0n) {
    const Asset0 = Number(event.params.asset_0_in);
    const Asset1 = Number(event.params.asset_1_out);

    setExchangeRate(Asset0, Asset1, pool, context);
  } else if (event.params.asset_1_in > 0n) {
    const Asset0 = Number(event.params.asset_1_in);
    const Asset1 = Number(event.params.asset_0_out);

    setExchangeRate(Asset0, Asset1, pool, context);
  }

  const volume = getVolume(event, pool);

  // const is_buy = event.params.asset_1_in > 0n;
  // const is_sell = event.params.asset_1_out > 0n

  // let exchange_rate = BigInt(0)
  //   if (is_buy) {
  //     console.log(event.params.asset_1_in, event.params.asset_0_out, "in n out");

  //     exchange_rate = (BigInt(event.params.asset_0_out)  * BigInt(10n ** 18n)) / BigInt(event.params.asset_1_in)

  //   } else if (is_sell) {
  //     console.log(event.params.asset_1_out, event.params.asset_0_in, "out n in");
  //     exchange_rate = (BigInt(event.params.asset_1_out)  * BigInt(10n ** 18n)) / BigInt(event.params.asset _0_in)

  // } catch (error) {
  //   console.log("error calculating exchange rate", error);
  // }

  const { tvl, tvlUSD } = await calculatePoolTVL(
    context,
    pool,
    new_reserve_0,
    new_reserve_1
  );

  const updatedPool = {
    id: poolId,
    asset_0: event.params.pool_id[0].bits,
    asset_1: event.params.pool_id[1].bits,
    is_stable: event.params.pool_id[2],
    reserve_0:
      (pool?.reserve_0 ?? 0n) +
      event.params.asset_0_in -
      event.params.asset_0_out,
    reserve_1:
      (pool?.reserve_1 ?? 0n) +
      event.params.asset_1_in -
      event.params.asset_1_out,
    create_time: pool?.create_time ?? event.block.time,
    decimals_0: pool?.decimals_0,
    decimals_1: pool?.decimals_1,
    tvl: tvl,
    tvlUSD: tvlUSD ? tvlUSD : 0,
    lpId: pool?.lpId ?? "n-a",
    swapVolume: pool?.swapVolume + volume,
  };

  context.Pool.set(updatedPool);

  const [address, isContract] = identityToStr(event.params.recipient);
  const transaction: Transaction = {
    id: event.transaction.id,
    pool_id: poolId,
    transaction_type: "SWAP",
    initiator: address,
    is_contract_initiator: isContract,
    asset_0_in: event.params.asset_0_in,
    asset_0_out: event.params.asset_0_out,
    asset_1_in: event.params.asset_1_in,
    asset_1_out: event.params.asset_1_out,
    block_time: event.block.time,
    lp_id: undefined,
    lp_amount: undefined,
    extra: undefined,
  };
  await upsertTransaction(context, transaction);

  const feeBP = poolId[2] ? 0.05 : 0.3;

  context.SwapDaily.set({
    id: dailySnapshotId,
    pool_id: poolId,
    snapshot_time: dailyTimestamp,
    count: (dailySnapshot?.count ?? 0) + 1,
    asset_0_in: (dailySnapshot?.asset_0_in ?? 0n) + event.params.asset_0_in,
    asset_0_out: (dailySnapshot?.asset_0_out ?? 0n) + event.params.asset_0_out,
    asset_1_in: (dailySnapshot?.asset_1_in ?? 0n) + event.params.asset_1_in,
    asset_1_out: (dailySnapshot?.asset_1_out ?? 0n) + event.params.asset_1_out,
    feesUSD: (dailySnapshot?.feesUSD ?? 0) + Number(volume) * feeBP,
    volume: (dailySnapshot?.volume ?? 0) + volume,
  });

  context.SwapHourly.set({
    id: hourlySnapshotId,
    pool_id: poolId,
    snapshot_time: hourlyTimestamp,
    count: (hourlySnapshot?.count ?? 0) + 1,
    asset_0_in: (hourlySnapshot?.asset_0_in ?? 0n) + event.params.asset_0_in,
    asset_0_out: (hourlySnapshot?.asset_0_out ?? 0n) + event.params.asset_0_out,
    asset_1_in: (hourlySnapshot?.asset_1_in ?? 0n) + event.params.asset_1_in,
    asset_1_out: (hourlySnapshot?.asset_1_out ?? 0n) + event.params.asset_1_out,
    feesUSD: (hourlySnapshot?.feesUSD ?? 0) + Number(volume) * feeBP,
    volume: (hourlySnapshot?.volume ?? 0) + volume,
  });

  const k0 = kPool(pool);
  const updatedPoolSubtractFees = {
    ...updatedPool,
    reserve_0:
      updatedPool.reserve_0 -
      calculateFee(event.params.pool_id, event.params.asset_0_in, AMM_FEES),
    reserve_1:
      updatedPool.reserve_1 -
      calculateFee(event.params.pool_id, event.params.asset_1_in, AMM_FEES),
  };
  const k1 = kPool(updatedPoolSubtractFees);

  if (k1 < k0) {
    context.CurveViolation.set({
      asset_0_in: event.params.asset_0_in,
      asset_0_out: event.params.asset_0_out,
      asset_1_in: event.params.asset_1_in,
      asset_1_out: event.params.asset_1_out,
      id: uuid(),
      block_time: event.block.time,
      pool_id: poolId,
      reserve_0: pool.reserve_0,
      reserve_1: pool.reserve_1,
      new_reserve_0: updatedPool.reserve_0,
      new_reserve_1: updatedPool.reserve_1,
      tx_id: event.transaction.id,
      k_0: k0.toString(),
      k_1: k1.toString(),
    });
  }
});
