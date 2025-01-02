/*
 * Please refer to https://docs.envio.dev for a thorough guide on all Envio indexer features
 */
import {
    Mira,
    Mira_type2 as PoolId,
    Mira_type8 as Identity,
    handlerContext as Context,
    Transaction,
    Pool,
} from "generated";
import {v4 as uuid} from 'uuid';
import BN from 'bn.js';
import axios from "axios";

type IdentityIsContract = [string, boolean];

const ONE_E_18 = new BN(10).pow(new BN(18));
const BASIS_POINTS = BigInt(10000);
const AMM_FEES: AmmFees = {
    lpFeeVolatile: BigInt(30),
    lpFeeStable: BigInt(5),
    protocolFeeStable: BigInt(0),
    protocolFeeVolatile: BigInt(0),
}

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
        asset_1_out: transaction.asset_1_out.toString()
    }
}

function poolIdToStr(poolId: PoolId): string {
    return `${poolId[0].bits}_${poolId[1].bits}_${poolId[2]}`
}

function identityToStr(identity: Identity): IdentityIsContract {
    switch (identity.case) {
        case 'Address':
            return [identity.payload.bits, false];
        case 'ContractId':
            return [identity.payload.bits, true];
    }
}

function powDecimals(decimals: number): BN {
    return new BN(10).pow(new BN(decimals));
}

interface AmmFees {
    lpFeeVolatile: bigint,
    lpFeeStable: bigint,
    protocolFeeVolatile: bigint,
    protocolFeeStable: bigint,
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
        powDecimals(pool.decimals_1),
    );
}

function calculateFee(
    poolId: PoolId,
    amount: bigint,
    ammFees: AmmFees
): bigint {
    const feeBP = poolId[2] ?
        ammFees.lpFeeStable + ammFees.protocolFeeStable :
        ammFees.lpFeeVolatile + ammFees.protocolFeeVolatile;
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
            extra: JSON.stringify(extra)
        };
        context.Transaction.set(enrichedTransaction);
    }
}

const shouldReturnEarlyDueToDuplicate = async (duplicateId: string, context: Context) => {
    const deduplicator = await context.DeDuplicator.get(duplicateId);
    if (deduplicator === undefined) {
        context.DeDuplicator.set({id: duplicateId, additionalDuplications: 0});
        return false;
    } else {
        // Return Early
        context.DeDuplicator.set({...deduplicator, additionalDuplications: deduplicator.additionalDuplications + 1});
        return true;
    }
}

const USDC_ID = "0x286c479da40dc953bddc3bb4c453b608bba2e0ac483b077bd475174115395e6b"
let ETH_ID = "0xf8f8b6283d7fa5b672b530cbb84fcccb4ff8dc40f8176ef4544ddb1f1952ad07"
let USDC_PRICE_USD = 1
let ETH_PRICE_USD = 3364.34

const toDecimal = (amount: number, decimals: number): number => {
    return Number(amount) / Math.pow(10, decimals);
};

setTimeout(async function() {
    try {
        const url = 'https://coins.llama.fi/prices/current/fuel:0x286c479da40dc953bddc3bb4c453b608bba2e0ac483b077bd475174115395e6b,fuel:0xf8f8b6283d7fa5b672b530cbb84fcccb4ff8dc40f8176ef4544ddb1f1952ad07'
    
        const res = await axios.get(url)
        console.log(res.data.coins);
        
        USDC_PRICE_USD = res.data.coins['fuel:0x286c479da40dc953bddc3bb4c453b608bba2e0ac483b077bd475174115395e6b'].price
    
        ETH_PRICE_USD = res.data.coins['fuel:0xf8f8b6283d7fa5b672b530cbb84fcccb4ff8dc40f8176ef4544ddb1f1952ad07'].price
    } catch (error) {
        console.error("Error fetching data", error)
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

async function calculatePoolTVL(
    context: Context,
    pool: Pool,
    reserve0: bigint,
    reserve1: bigint
): Promise<{ tvl: bigint; tvlUSD: number | null }> {
    const tvl = reserve0 + reserve1;
    
    // For USDC pairs
    if (pool.asset_0 === USDC_ID || pool.asset_1 === USDC_ID) {
        if (pool.asset_0 === USDC_ID) {
            // USDC is token0
            const usdc = await context.Asset.get(USDC_ID)
            const tokenValue = (Number(reserve1) / Number(reserve0)) * usdc!.price_usd
            console.log(toDecimal(Number(reserve1 * BigInt(10n ** 18n) / reserve0), pool.decimals_0 + pool.decimals_1));
            console.log(Number(reserve1) / Number(reserve0));
            
            
            // Save token1's price in USD
            context.Asset.set({
                id: pool.asset_1,
                price_usd: tokenValue, // Convert to price per token
            });

            const usdcAmt = toDecimal(Number(reserve0), pool.decimals_0) * 1;
            const tokenAmt = (usdcAmt) / Number(reserve1)
                

            return { tvl, tvlUSD: (usdcAmt + tokenAmt)};
        } else {
            // USDC is token1
            const usdc = await context.Asset.get(USDC_ID)
            const tokenValue = (Number(reserve0) / Number(reserve1)) * usdc!.price_usd
            console.log(toDecimal(Number(reserve0 * BigInt(10n ** 18n) / reserve1), pool.decimals_0 + pool.decimals_1))
            console.log(Number(reserve0) / Number(reserve1));
            

            // Save token0's price in USD
            context.Asset.set({
                id: pool.asset_0,
                price_usd: tokenValue
            });

            const usdcAmt = toDecimal(Number(reserve1), pool.decimals_1) * 1
            const tokenAmt = (usdcAmt) / Number(reserve0);

            return { tvl, tvlUSD: (usdcAmt + tokenAmt) };
        }
    }

        // For ETH pairs
        if (pool.asset_0 === ETH_ID || pool.asset_1 === ETH_ID) {
            if (pool.asset_0 === ETH_ID) {
                // ETH is token0
                const eth = await context.Asset.get(ETH_ID)
            const tokenValue = (Number(reserve1) / Number(reserve0)) * eth!.price_usd
            console.log(toDecimal(Number(reserve1 * BigInt(10n ** 18n) / reserve0), pool.decimals_0 + pool.decimals_1));
            console.log(Number(reserve1) / Number(reserve0));
                
                // Save token1's price in USD
                context.Asset.set({
                    id: pool.asset_1,
                    price_usd: tokenValue,
                });

                const ethAmt = toDecimal(Number(reserve0), pool.decimals_0) * ETH_PRICE_USD;
                const tokenAmt = (ethAmt) / Number(reserve1);

                return { tvl, tvlUSD: (ethAmt + tokenAmt)};
            } else {
                // ETH is token1
                const eth = await context.Asset.get(ETH_ID)
            const tokenValue = (Number(reserve0) / Number(reserve1)) * eth!.price_usd
            console.log(toDecimal(Number(reserve0 * BigInt(10n ** 18n) / reserve1), pool.decimals_0 + pool.decimals_1))
            console.log(Number(reserve0) / Number(reserve1));
                
                // Save token0's price in USD
                context.Asset.set({
                    id: pool.asset_0,
                    price_usd: tokenValue,
                });

                const ethAmt = toDecimal(Number(reserve1), pool.decimals_1) * ETH_PRICE_USD;
                const tokenAmt = (ethAmt) / Number(reserve0);

                return { tvl, tvlUSD: (ethAmt + tokenAmt) };
            }
        }

    return { tvl, tvlUSD: null };
}

Mira.CreatePoolEvent.handler(async ({event, context}) => {
    // Save a raw event
    const id = `${event.logIndex}_${event.transaction.id}_${event.block.height}`
    const rawEvent = {
        id: id,
        pool_id: poolIdToStr(event.params.pool_id),
        decimals_0: event.params.decimals_0,
        decimals_1: event.params.decimals_1,
        logIndex: event.logIndex,
        transactionId: event.transaction.id,
        blockId: event.block.id,
        blockHeight: event.block.height
    };
    context.RawCreatePoolEvent.set(rawEvent);

    context.log.info(`Handling CreatePoolEvent for pool ID: ${poolIdToStr(event.params.pool_id)}`);

    if (await shouldReturnEarlyDueToDuplicate(id, context)) {
        context.log.error(`Return Early due to duplicate in CreatePoolEvent - tx: ${event.transaction.id}`);
        return;
    }

    let newAsset0 = {
        id: event.params.pool_id[0].bits,
        price_usd: 0,
        exchange_rate: 0
    }

    let newAsset1 = {
        id: event.params.pool_id[1].bits,
        price_usd: 0,
        exchange_rate: 0
    }

    if (event.params.pool_id[0].bits === USDC_ID) {
        newAsset0 = {
            id: event.params.pool_id[0].bits,
            price_usd: USDC_PRICE_USD,
            exchange_rate: 0
        }
    } else if (event.params.pool_id[1].bits === USDC_ID) {
        newAsset1 = {
            id: event.params.pool_id[1].bits,
            price_usd: USDC_PRICE_USD,
            exchange_rate: 0
        }
    }

    if (event.params.pool_id[0].bits === ETH_ID) {
        newAsset0 = {
            id: event.params.pool_id[0].bits,
            price_usd: ETH_PRICE_USD,
            exchange_rate: 0
        }
    } else if (event.params.pool_id[1].bits === ETH_ID) {
        newAsset1 = {
            id: event.params.pool_id[1].bits,
            price_usd: ETH_PRICE_USD,
            exchange_rate: 0
        }
    }

    context.Asset.set(newAsset0);
    context.Asset.set(newAsset1);

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
        tvlUSD: 0
    };
    context.Pool.set(pool);
});


Mira.MintEvent.handler(async ({event, context}) => {
    event.params.liquidity
    // Save a raw event
    const id = `${event.logIndex}_${event.transaction.id}_${event.block.height}`
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
        blockHeight: event.block.height
    };
    context.RawMintEvent.set(rawEvent);

    context.log.info(`Handling MintEvent for transaction ID: ${event.transaction.id}`);

    if (await shouldReturnEarlyDueToDuplicate(id, context)) {
        context.log.error(`Return Early due to duplicate in MintEvent - tx: ${event.transaction.id}`);
        return;
    }

    const poolId = poolIdToStr(event.params.pool_id);
    const pool = await context.Pool.get(poolId);
    if (pool === undefined) {
        context.log.error(`Pool ${poolId} not found but received MintEvent`);
        return;
    }

    const new_reserve_0 = (pool?.reserve_0 ?? 0n) + event.params.asset_0_in
    const new_reserve_1 = (pool?.reserve_1 ?? 0n) + event.params.asset_1_in

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
        tvlUSD: tvlUSD ? tvlUSD : 0
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
        lp_id: event.params. liquidity.id.bits,
        lp_amount: event.params.liquidity.amount,
        extra: undefined
    };
    await upsertTransaction(context, transaction);
});

Mira.BurnEvent.handler(async ({event, context}) => {
    // Save a raw event
    const id = `${event.logIndex}_${event.transaction.id}_${event.block.height}`
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
        blockHeight: event.block.height
    };
    context.RawBurnEvent.set(rawEvent);

    context.log.info(`Handling BurnEvent for transaction ID: ${event.transaction.id}`);

    if (await shouldReturnEarlyDueToDuplicate(id, context)) {
        context.log.error(`Return Early due to duplicate in BurnEvent - tx: ${event.transaction.id}`);
        return;
    }

    const poolId = poolIdToStr(event.params.pool_id);
    const pool = await context.Pool.get(poolId);
    if (pool === undefined) {
        context.log.error(`Pool ${poolId} not found but received BurnEvent`);
        return;
    }

    const new_reserve_0 = (pool?.reserve_0 ?? 0n) - event.params.asset_0_out
    const new_reserve_1 = (pool?.reserve_1 ?? 0n) - event.params.asset_1_out

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
        reserve_0: (pool?.reserve_0 ?? 0n) - event.params.asset_0_out,
        reserve_1: (pool?.reserve_1 ?? 0n) - event.params.asset_1_out,
        create_time: pool?.create_time ?? event.block.time,
        decimals_0: pool?.decimals_0,
        decimals_1: pool?.decimals_1,
        tvl: tvl,
        tvlUSD: tvlUSD ? tvlUSD : 0
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
        extra: undefined
    };
    await upsertTransaction(context, transaction);
});

Mira.SwapEvent.handler(async ({event, context}) => {
    const id = `${event.logIndex}_${event.transaction.id}_${event.block.height}`
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
        blockHeight: event.block.height
    };
    context.RawSwapEvent.set(rawEvent);

    context.log.info(`Handling SwapEvent for transaction ID: ${event.transaction.id}`);

    if (await shouldReturnEarlyDueToDuplicate(id, context)) {
        context.log.error(`Return Early due to duplicate in SwapEvent - tx: ${event.transaction.id}`);
        return;
    }

    const poolId = poolIdToStr(event.params.pool_id);

    const dailyTimestamp = new Date(event.block.time * 1000).setHours(0, 0, 0, 0) / 1000;
    const dailySnapshotId = `${poolId}_${dailyTimestamp}`

    const hourlyTimestamp = new Date(event.block.time * 1000).setMinutes(0, 0, 0) / 1000;
    const hourlySnapshotId = `${poolId}_${hourlyTimestamp}`

    const [pool, dailySnapshot, hourlySnapshot] = await Promise.all([
        context.Pool.get(poolId),
        context.SwapDaily.get(dailySnapshotId),
        context.SwapHourly.get(hourlySnapshotId),
    ]);

    if (pool === undefined) {
        context.log.error(`Pool ${poolId} not found but received SwapEvent`);
        return;
    }

  

    const new_reserve_0 = (pool?.reserve_0 ?? 0n) + event.params.asset_0_in - event.params.asset_0_out
    const new_reserve_1 = (pool?.reserve_1 ?? 0n) + event.params.asset_1_in - event.params.asset_1_out

    // const is_buy = event.params.asset_1_in > 0n;
    // const is_sell = event.params.asset_1_out > 0n
  
    // let exchange_rate = BigInt(0)
    // let price1
  
    // try {
    //     if (pool.asset_0 === USDC_ID || pool.asset_1 === USDC_ID) {
    //         context.Asset.set({
    //             id: USDC_ID,
    //             price_usd: USDC_PRICE_USD, // 1
    //             exchange_rate: 1
    //         });
    //     }
        
    //     if (pool.asset_0 === ETH_ID || pool.asset_1 === ETH_ID) {
    //         context.Asset.set({
    //             id: ETH_ID,
    //             price_usd: ETH_PRICE_USD, // 3364
    //             exchange_rate: ETH_PRICE_USD
    //         });
    //     }        

    //   if (is_buy) {
    //     console.log(event.params.asset_1_in, event.params.asset_0_out, "in n out");
        
    //     exchange_rate = (BigInt(event.params.asset_0_out)  * BigInt(10n ** 18n)) / BigInt(event.params.asset_1_in)

        
    //     const ratio = toDecimal(Number(exchange_rate), 18)

    //     console.log(ratio, ratio * Number(event.params.asset_0_out), event.params.asset_1_in);
        
    //     console.log(`For 1 asset0 I will get ${ratio * 1} asset1`);

    //     if (pool.asset_1 === USDC_ID) {
    //         console.log("Asset 1 is usdc");

    //         const asset = await context.Asset.get(event.params.pool_id[1].bits)
    //         const price_usd = (1 / ratio) * asset!.price_usd

    //         console.log(`For 1 asset0 I will get ${ratio * 1} USDC (asset1)`);
    //         console.log(`price of 1 asset0 is ${ratio * 1 * USDC_PRICE_USD}`);

    //         context.Asset.set({
    //             id: event.params.pool_id[0].bits,
    //             price_usd,
    //             exchange_rate: ratio
    //         })
    //     } else if (pool.asset_0 === USDC_ID) {
    //         console.log("Asset 0 is usdc");
            
    //         const asset = await context.Asset.get(event.params.pool_id[0].bits)!
    //         const price_usd = (1 / ratio) * asset!.price_usd

    //         context.Asset.set({
    //             id: event.params.pool_id[1].bits,
    //             price_usd,
    //             exchange_rate: ratio
    //         })
    //     } else if (pool.asset_1 === ETH_ID) {
    //         const asset = await context.Asset.get(event.params.pool_id[1].bits)!
    //         const price_usd = (1 / ratio) * asset!.price_usd

    //         console.log(`For 1 asset0 I will get ${ratio * 1} ETH (asset1)`);
    //         console.log(`price of 1 asset0 is ${ratio * 1 * ETH_PRICE_USD}`);

    //         context.Asset.set({
    //             id: event.params.pool_id[0].bits,
    //             price_usd,
    //             exchange_rate: ratio
    //         })
    //     } else if (pool.asset_0 === ETH_ID) {
    //         console.log("Asset 0 is ETH");
    //         const asset = await context.Asset.get(event.params.pool_id[0].bits)!
    //         const price_usd = (1 / ratio) * asset!.price_usd

    //         context.Asset.set({
    //             id: event.params.pool_id[1].bits,
    //             price_usd,
    //             exchange_rate: ratio
    //         })
    //     }
         
        
  
    //     //  price1 = (new_reserve_0 * event.params.asset_1_in) / (new_reserve_1 + event.params.asset_1_in)
    //   } else if (is_sell) {
    //     console.log(event.params.asset_1_out, event.params.asset_0_in, "out n in");
    //     exchange_rate = (BigInt(event.params.asset_1_out)  * BigInt(10n ** 18n)) / BigInt(event.params.asset _0_in) 

    //     const ratio = toDecimal(Number(exchange_rate), 18)

    //     console.log(ratio, ratio * Number(event.params.asset_0_in), event.params.asset_1_out);

    //     if (pool.asset_1 === USDC_ID) {
    //         console.log("Asset 1 is usdc");

    //         const asset = await context.Asset.get(event.params.pool_id[1].bits)
    //         const price_usd = (1 / ratio) * asset!.price_usd

    //         console.log(`For 1 asset0 I will get ${ratio * 1} USDC (asset1)`);
    //         console.log(`price of 1 asset0 is ${ratio * 1 * USDC_PRICE_USD}`);

    //         context.Asset.set({
    //             id: event.params.pool_id[0].bits,
    //             price_usd,
    //             exchange_rate: ratio
    //         })
    //     } else if (pool.asset_0 === USDC_ID) {
    //         console.log("Asset 0 is usdc");
            
    //         const asset = await context.Asset.get(event.params.pool_id[0].bits)!
    //         const price_usd = (1 / ratio) * asset!.price_usd

    //         context.Asset.set({
    //             id: event.params.pool_id[1].bits,
    //             price_usd,
    //             exchange_rate: ratio
    //         })
    //     } else if (pool.asset_1 === ETH_ID) {
    //         const asset = await context.Asset.get(event.params.pool_id[1].bits)!
    //         const price_usd = (1 / ratio) * asset!.price_usd

    //         console.log(`For 1 asset0 I will get ${ratio * 1} ETH (asset1)`);
    //         console.log(`price of 1 asset0 is ${ratio * 1 * ETH_PRICE_USD}`);

    //         context.Asset.set({
    //             id: event.params.pool_id[0].bits,
    //             price_usd,
    //             exchange_rate: ratio
    //         })
    //     } else if (pool.asset_0 === ETH_ID) {
    //         console.log("Asset 0 is ETH");
    //         const asset = await context.Asset.get(event.params.pool_id[0].bits)!
    //         const price_usd = (1 / ratio) * asset!.price_usd

    //         context.Asset.set({
    //             id: event.params.pool_id[1].bits,
    //             price_usd,
    //             exchange_rate: ratio
    //         })
    //     }
        
        

    //     //  price1 = (new_reserve_0 * event.params.asset_1_out) / (new_reserve_1 - event.params.asset_1_out)
    //   }
  
    //   // setAsset(context, event.params.pool_id[0].bits, event.params.pool_id[1].bits, toDecimal(Number(exchange_rate), pool.decimals_1))
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
        reserve_0: (pool?.reserve_0 ?? 0n) + event.params.asset_0_in - event.params.asset_0_out,
        reserve_1: (pool?.reserve_1 ?? 0n) + event.params.asset_1_in - event.params.asset_1_out,
        create_time: pool?.create_time ?? event.block.time,
        decimals_0: pool?.decimals_0,
        decimals_1: pool?.decimals_1,
        tvl: tvl,
        tvlUSD: tvlUSD ? tvlUSD : 0
    }

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
        extra: undefined
    };
    await upsertTransaction(context, transaction);

    const feeBP = poolId[2] ?
    AMM_FEES.lpFeeStable + AMM_FEES.protocolFeeStable :
    AMM_FEES.lpFeeVolatile + AMM_FEES.protocolFeeVolatile;
    const feesUSD0 = toDecimal(Number(event.params.asset_0_in), pool.decimals_0) * Number(feeBP);
    const feesUSD1 = toDecimal(Number(event.params.asset_1_in), pool.decimals_1) * Number(feeBP);

    context.SwapDaily.set({
        id: dailySnapshotId,
        pool_id: poolId,
        snapshot_time: dailyTimestamp,
        count: (dailySnapshot?.count ?? 0) + 1,
        asset_0_in: (dailySnapshot?.asset_0_in ?? 0n) + event.params.asset_0_in,
        asset_0_out: (dailySnapshot?.asset_0_out ?? 0n) + event.params.asset_0_out,
        asset_1_in: (dailySnapshot?.asset_1_in ?? 0n) + event.params.asset_1_in,
        asset_1_out: (dailySnapshot?.asset_1_out ?? 0n) + event.params.asset_1_out,
        feesUSD: feesUSD0 + feesUSD1
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
        feesUSD: feesUSD0 + feesUSD1
    });

    const k0 = kPool(pool);
    const updatedPoolSubtractFees = {
        ...updatedPool,
        reserve_0: updatedPool.reserve_0 - calculateFee(event.params.pool_id, event.params.asset_0_in, AMM_FEES),
        reserve_1: updatedPool.reserve_1 - calculateFee(event.params.pool_id, event.params.asset_1_in, AMM_FEES),
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
            k_1: k1.toString()
        });
    }
});
