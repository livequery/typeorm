import { Not, LessThan, LessThanOrEqual, MoreThan, MoreThanOrEqual, Between, In, ILike } from "typeorm";

export const ExpressionMapper = {
    eq: { sql: v => v, mongodb: v => ({ $eq: v }) },
    ne: { sql: v => Not(v), mongodb: v => ({ $ne: v }) },
    lt: { sql: v => LessThan(v), mongodb: v => ({ $lt: v }) },
    lte: { sql: v => LessThanOrEqual(v), mongodb: v => ({ $lte: v }) },
    gt: { sql: v => MoreThan(v), mongodb: v => ({ $gt: v }) },
    gte: { sql: v => MoreThanOrEqual(v), mongodb: v => ({ $gte: v }) },
    between: { sql: ([a, b]) => Between(a, b), mongodb: ([a, b]) => ({ $gte: a, $lt: b }) },
    in: { sql: a => In(a), mongodb: a => ({ $in: a }) },
    like: { sql: v => ILike(`%${v}%`), mongodb: v => ({ $regex: `${v}` }) }
}